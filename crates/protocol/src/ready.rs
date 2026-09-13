use crate::message::{
    BackendTag, FrontendTag, MessageError, TransactionStatus, parse_ready_for_query,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyEvent {
    Ready(TransactionStatus),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TrackerError {
    #[error("ReadyForQuery arrived with no outstanding request")]
    UnexpectedReadyForQuery,
    #[error(transparent)]
    Message(#[from] MessageError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyTracker {
    outstanding: u32,
    extended_open: bool,
    status: TransactionStatus,
    terminated: bool,
}

impl Default for ReadyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyTracker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            outstanding: 0,
            extended_open: false,
            status: TransactionStatus::Idle,
            terminated: false,
        }
    }

    pub fn on_frontend(&mut self, tag: FrontendTag) {
        match tag {
            FrontendTag::Query | FrontendTag::FunctionCall => self.outstanding += 1,
            FrontendTag::Sync => {
                self.outstanding += 1;
                self.extended_open = false;
            }
            FrontendTag::Parse
            | FrontendTag::Bind
            | FrontendTag::Describe
            | FrontendTag::Execute
            | FrontendTag::Close
            | FrontendTag::Flush => self.extended_open = true,
            FrontendTag::Terminate => self.terminated = true,
            FrontendTag::CopyData
            | FrontendTag::CopyDone
            | FrontendTag::CopyFail
            | FrontendTag::Password => {}
        }
    }

    pub fn on_backend(
        &mut self,
        tag: BackendTag,
        body: &[u8],
    ) -> Result<Option<ReadyEvent>, TrackerError> {
        if tag != BackendTag::ReadyForQuery {
            return Ok(None);
        }
        if self.outstanding == 0 {
            return Err(TrackerError::UnexpectedReadyForQuery);
        }
        let status = parse_ready_for_query(body)?;
        self.outstanding -= 1;
        self.status = status;
        Ok(Some(ReadyEvent::Ready(status)))
    }

    #[must_use]
    pub fn status(&self) -> TransactionStatus {
        self.status
    }

    #[must_use]
    pub fn outstanding(&self) -> u32 {
        self.outstanding
    }

    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    #[must_use]
    pub fn may_release(&self) -> bool {
        !self.terminated
            && self.outstanding == 0
            && !self.extended_open
            && self.status == TransactionStatus::Idle
    }
}
