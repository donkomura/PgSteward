use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::time::Duration;

use crate::rt::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServerLimits {
    pub max_connections: u32,
    pub superuser_reserved_connections: u32,
    pub reserved_connections: u32,
}

impl ServerLimits {
    #[must_use]
    pub fn reserved(&self) -> u32 {
        self.superuser_reserved_connections
            .saturating_add(self.reserved_connections)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BudgetInputs {
    pub limits: ServerLimits,
    pub foreign_peak: u32,
    pub margin: u32,
}

impl BudgetInputs {
    fn deductions(&self) -> u64 {
        u64::from(self.limits.reserved()) + u64::from(self.foreign_peak) + u64::from(self.margin)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TotalBudget {
    inputs: BudgetInputs,
    total: u32,
    shortfall: u32,
}

impl TotalBudget {
    #[must_use]
    pub fn derive(inputs: BudgetInputs) -> Self {
        let max = u64::from(inputs.limits.max_connections);
        let deductions = inputs.deductions();
        let total = u32::try_from(max.saturating_sub(deductions)).unwrap_or(u32::MAX);
        let shortfall = u32::try_from(deductions.saturating_sub(max)).unwrap_or(u32::MAX);
        Self {
            inputs,
            total,
            shortfall,
        }
    }

    #[must_use]
    pub fn total(&self) -> u32 {
        self.total
    }

    #[must_use]
    pub fn inputs(&self) -> BudgetInputs {
        self.inputs
    }

    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.total == 0
    }

    #[must_use]
    pub fn shortfall(&self) -> u32 {
        self.shortfall
    }
}

impl fmt::Display for TotalBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let limits = self.inputs.limits;
        write!(
            f,
            "{} = max_connections {} - superuser_reserved_connections {} - reserved_connections {} - foreign peak {} - margin {}",
            self.total,
            limits.max_connections,
            limits.superuser_reserved_connections,
            limits.reserved_connections,
            self.inputs.foreign_peak,
            self.inputs.margin,
        )?;
        if self.shortfall > 0 {
            write!(f, " (short by {})", self.shortfall)?;
        }
        Ok(())
    }
}

/// The largest number of foreign connections seen over the last window. The
/// budget is derived from this peak rather than the latest sample, so that a
/// dip between two polls does not hand out slots that an administrator's psql
/// is about to take back.
#[derive(Debug, Clone)]
pub struct ForeignPeak {
    window: Duration,
    samples: VecDeque<(Instant, u32)>,
}

impl ForeignPeak {
    #[must_use]
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            samples: VecDeque::new(),
        }
    }

    // The samples are kept in decreasing order: one that is not larger than a
    // later one can never be the peak again, so it is dropped on arrival and
    // the queue stays short however often the poll runs.
    pub fn observe(&mut self, at: Instant, count: u32) {
        while let Some(&(oldest, _)) = self.samples.front() {
            if at.saturating_duration_since(oldest) <= self.window {
                break;
            }
            self.samples.pop_front();
        }
        while let Some(&(_, seen)) = self.samples.back() {
            if seen > count {
                break;
            }
            self.samples.pop_back();
        }
        self.samples.push_back((at, count));
    }

    #[must_use]
    pub fn peak(&self) -> u32 {
        self.samples.front().map_or(0, |&(_, count)| count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BudgetChange {
    Unchanged,
    Grew { from: u32, to: u32 },
    Shrank { from: u32, to: u32 },
}

/// One instance's derived total budget, kept up to date from what the instance
/// reports. Growing takes effect at once; shrinking is a desired state the
/// proxies still have to converge to, which is why the direction is part of the
/// answer.
#[derive(Debug, Clone)]
pub struct InstanceBudget {
    limits: ServerLimits,
    margin: u32,
    peak: ForeignPeak,
    budget: TotalBudget,
}

impl InstanceBudget {
    #[must_use]
    pub fn new(limits: ServerLimits, margin: u32, window: Duration) -> Self {
        let peak = ForeignPeak::new(window);
        let budget = TotalBudget::derive(BudgetInputs {
            limits,
            foreign_peak: peak.peak(),
            margin,
        });
        Self {
            limits,
            margin,
            peak,
            budget,
        }
    }

    pub fn observe(&mut self, at: Instant, foreign_connections: u32) -> BudgetChange {
        self.peak.observe(at, foreign_connections);
        self.derive()
    }

    pub fn update_limits(&mut self, limits: ServerLimits) -> BudgetChange {
        self.limits = limits;
        self.derive()
    }

    #[must_use]
    pub fn current(&self) -> TotalBudget {
        self.budget
    }

    fn derive(&mut self) -> BudgetChange {
        let from = self.budget.total();
        self.budget = TotalBudget::derive(BudgetInputs {
            limits: self.limits,
            foreign_peak: self.peak.peak(),
            margin: self.margin,
        });
        let to = self.budget.total();
        match to.cmp(&from) {
            Ordering::Greater => BudgetChange::Grew { from, to },
            Ordering::Less => BudgetChange::Shrank { from, to },
            Ordering::Equal => BudgetChange::Unchanged,
        }
    }
}
