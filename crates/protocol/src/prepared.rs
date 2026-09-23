use std::collections::BTreeSet;

use crate::frontend::{CloseTarget, FrontendError, decode_bind, decode_close, decode_parse};
use crate::message::FrontendTag;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict<'a> {
    Forward,
    Skip,
    Reject(&'a str),
}

/// The prepared statements one server connection carries while it is assigned.
///
/// A statement lives on the connection it was parsed on, and the reset that
/// returns the connection to the pool deallocates it, so a name a client
/// prepared before a boundary is not there after one. Binding it would run
/// whatever the server happens to hold under that name, so the bind is refused
/// here instead of reaching the server.
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
            FrontendTag::Parse => {
                self.parsed.insert(decode_parse(body)?.to_owned());
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
