use std::collections::BTreeSet;

use crate::frontend::{
    CloseTarget, FrontendError, decode_bind, decode_close, decode_parse, decode_query,
};
use crate::listen::has_listen;
use crate::message::FrontendTag;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict<'a> {
    Forward,
    Skip,
    Reject(&'a str),
    RejectListen,
}

/// The state one server connection carries while it is assigned, and what a
/// client may ask of it.
///
/// A prepared statement lives on the connection it was parsed on, and the reset
/// that returns the connection to the pool deallocates it, so a name a client
/// prepared before a boundary is not there after one. Binding it would run
/// whatever the server happens to hold under that name, so the bind is refused
/// here instead of reaching the server. A registration made with LISTEN is the
/// same kind of state and the same reset drops it, so the statement that would
/// make one is refused before the server ever runs it.
///
/// After a refusal the window is in the same state a real backend would leave
/// it in: everything is discarded until the Sync that closes it, and the Sync
/// itself goes through so that the server ends its implicit transaction and
/// answers with the `ReadyForQuery` the client is waiting for.
#[derive(Debug, Clone, Default)]
pub struct PreparedStatements {
    parsed: BTreeSet<String>,
    refused: bool,
}

impl PreparedStatements {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Discards everything up to the Sync that closes the window, the way a
    /// refused bind does.
    ///
    /// A refused LISTEN leaves this to the caller, because the answers already
    /// in flight may have to reach the client before the refusal does, and
    /// until then the message stays unread.
    pub fn discard_until_sync(&mut self) {
        self.refused = true;
    }

    pub fn on_frontend<'a>(
        &mut self,
        tag: FrontendTag,
        body: &'a [u8],
    ) -> Result<Verdict<'a>, FrontendError> {
        if self.refused {
            if tag != FrontendTag::Sync {
                return Ok(Verdict::Skip);
            }
            self.refused = false;
            return Ok(Verdict::Forward);
        }
        match tag {
            FrontendTag::Query => {
                if has_listen(decode_query(body)?) {
                    return Ok(Verdict::RejectListen);
                }
            }
            FrontendTag::Parse => {
                let parse = decode_parse(body)?;
                if has_listen(parse.query) {
                    return Ok(Verdict::RejectListen);
                }
                self.parsed.insert(parse.statement.to_owned());
            }
            FrontendTag::Bind => {
                let statement = decode_bind(body)?;
                if !self.parsed.contains(statement) {
                    self.refused = true;
                    return Ok(Verdict::Reject(statement));
                }
            }
            FrontendTag::Close => {
                if let CloseTarget::Statement(name) = decode_close(body)? {
                    self.parsed.remove(name);
                }
            }
            _ => {}
        }
        Ok(Verdict::Forward)
    }
}
