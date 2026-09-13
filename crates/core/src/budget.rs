use std::fmt;

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
