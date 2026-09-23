use crate::backend::{ErrorResponse, sqlstate};

const HINT: &str = "The admin console has SHOW POOLS, SHOW BUDGET, SHOW CLIENTS, SHOW SERVERS, \
                    SHOW INSTANCES, SHOW CONFIG, RELOAD, PAUSE, RESUME, \
                    SET INSTANCE <name> margin = <count>, and \
                    SET TENANT <name> min = <count>, max = <count>, weight = <count>.";

const SET_INSTANCE: &str = "SET INSTANCE";
const SET_TENANT: &str = "SET TENANT";

const INSTANCE_SHAPE: &str = "a name followed by `margin = <count>`";
const TENANT_SHAPE: &str = "a name followed by one or more of `min = <count>`, `max = <count>` \
                            and `weight = <count>`, separated by commas and each written once";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminCommand {
    /// The text held no statement. The console answers it with an
    /// `EmptyQueryResponse`, the way a server answers an empty query.
    Empty,
    ShowPools,
    ShowBudget,
    ShowClients,
    ShowServers,
    ShowInstances,
    ShowConfig,
    Reload,
    Pause,
    Resume,
    SetInstance {
        instance: String,
        margin: u32,
    },
    SetTenant {
        tenant: String,
        settings: TenantSettings,
    },
}

/// The parts of a tenant's share this command changes. What it leaves out
/// keeps the value the allocation table already holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TenantSettings {
    pub min: Option<u32>,
    pub max: Option<u32>,
    pub weight: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdminError {
    #[error("the admin console has no such command")]
    NoSuchCommand,
    #[error("`{command}` expects {expected}")]
    Malformed {
        command: &'static str,
        expected: &'static str,
    },
    #[error("the admin console takes one command at a time")]
    TooManyStatements,
    #[error("a quoted name is not closed")]
    UnclosedName,
}

impl AdminError {
    /// What the console sends back. Every refusal is a syntax error because
    /// the console's language is a closed list: a command outside it is not a
    /// feature that is missing, it is a text the console cannot read.
    #[must_use]
    pub fn response(&self) -> ErrorResponse {
        ErrorResponse::error(sqlstate::SYNTAX_ERROR, self.to_string()).with_hint(HINT)
    }
}

/// Reads one admin command out of the text of a simple query.
pub fn parse(sql: &str) -> Result<AdminCommand, AdminError> {
    let statements = tokenize(sql)?;
    match statements.as_slice() {
        [] => Ok(AdminCommand::Empty),
        [tokens] => command(tokens),
        _ => Err(AdminError::TooManyStatements),
    }
}

fn command(tokens: &[Token<'_>]) -> Result<AdminCommand, AdminError> {
    match tokens {
        [single] if keyword(single, "RELOAD") => Ok(AdminCommand::Reload),
        [single] if keyword(single, "PAUSE") => Ok(AdminCommand::Pause),
        [single] if keyword(single, "RESUME") => Ok(AdminCommand::Resume),
        [show, what] if keyword(show, "SHOW") => shown(what),
        [set, what, rest @ ..] if keyword(set, "SET") && keyword(what, "INSTANCE") => {
            set_instance(rest)
        }
        [set, what, rest @ ..] if keyword(set, "SET") && keyword(what, "TENANT") => {
            set_tenant(rest)
        }
        _ => Err(AdminError::NoSuchCommand),
    }
}

fn shown(what: &Token<'_>) -> Result<AdminCommand, AdminError> {
    if keyword(what, "POOLS") {
        Ok(AdminCommand::ShowPools)
    } else if keyword(what, "BUDGET") {
        Ok(AdminCommand::ShowBudget)
    } else if keyword(what, "CLIENTS") {
        Ok(AdminCommand::ShowClients)
    } else if keyword(what, "SERVERS") {
        Ok(AdminCommand::ShowServers)
    } else if keyword(what, "INSTANCES") {
        Ok(AdminCommand::ShowInstances)
    } else if keyword(what, "CONFIG") {
        Ok(AdminCommand::ShowConfig)
    } else {
        Err(AdminError::NoSuchCommand)
    }
}

fn set_instance(tokens: &[Token<'_>]) -> Result<AdminCommand, AdminError> {
    let malformed = AdminError::Malformed {
        command: SET_INSTANCE,
        expected: INSTANCE_SHAPE,
    };
    let [named, margin, Token::Equals, value] = tokens else {
        return Err(malformed);
    };
    if !keyword(margin, "MARGIN") {
        return Err(malformed);
    }
    match (name(named), count(value)) {
        (Some(instance), Some(margin)) => Ok(AdminCommand::SetInstance { instance, margin }),
        _ => Err(malformed),
    }
}

fn set_tenant(tokens: &[Token<'_>]) -> Result<AdminCommand, AdminError> {
    let malformed = AdminError::Malformed {
        command: SET_TENANT,
        expected: TENANT_SHAPE,
    };
    let [named, assignments @ ..] = tokens else {
        return Err(malformed);
    };
    let Some(tenant) = name(named) else {
        return Err(malformed);
    };
    if assignments.is_empty() {
        return Err(malformed);
    }
    let mut settings = TenantSettings::default();
    for assignment in assignments.split(|token| matches!(token, Token::Comma)) {
        let [key, Token::Equals, value] = assignment else {
            return Err(malformed);
        };
        let Some(value) = count(value) else {
            return Err(malformed);
        };
        let slot = if keyword(key, "MIN") {
            &mut settings.min
        } else if keyword(key, "MAX") {
            &mut settings.max
        } else if keyword(key, "WEIGHT") {
            &mut settings.weight
        } else {
            return Err(malformed);
        };
        if slot.replace(value).is_some() {
            return Err(malformed);
        }
    }
    Ok(AdminCommand::SetTenant { tenant, settings })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token<'a> {
    Word(&'a str),
    Quoted(String),
    Equals,
    Comma,
}

fn keyword(token: &Token<'_>, word: &str) -> bool {
    matches!(token, Token::Word(actual) if actual.eq_ignore_ascii_case(word))
}

/// A name is taken exactly as it is written. Unlike a SQL identifier it is not
/// folded to lower case: it is looked up in the cluster configuration, whose
/// keys are arbitrary strings, and most instance names cannot be written as a
/// SQL identifier anyway. Quoting only lets a name hold what a word cannot.
fn name(token: &Token<'_>) -> Option<String> {
    match token {
        Token::Word(word) => Some((*word).to_owned()),
        Token::Quoted(name) => Some(name.clone()),
        Token::Equals | Token::Comma => None,
    }
}

fn count(token: &Token<'_>) -> Option<u32> {
    match token {
        Token::Word(word) => word.parse().ok(),
        _ => None,
    }
}

/// Splits the text into statements, and each statement into tokens. Empty
/// statements are dropped, so stray semicolons around one command are
/// harmless while a second command is not.
fn tokenize(sql: &str) -> Result<Vec<Vec<Token<'_>>>, AdminError> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut tokens = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            byte if byte.is_ascii_whitespace() => at += 1,
            b';' => {
                if !tokens.is_empty() {
                    statements.push(std::mem::take(&mut tokens));
                }
                at += 1;
            }
            b'=' => {
                tokens.push(Token::Equals);
                at += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                at += 1;
            }
            b'"' => {
                let (quoted, end) = quoted(sql, at)?;
                tokens.push(Token::Quoted(quoted));
                at = end;
            }
            _ => {
                let end = end_of_word(bytes, at);
                tokens.push(Token::Word(&sql[at..end]));
                at = end;
            }
        }
    }
    if !tokens.is_empty() {
        statements.push(tokens);
    }
    Ok(statements)
}

fn end_of_word(bytes: &[u8], at: usize) -> usize {
    let mut end = at;
    while end < bytes.len() && !is_delimiter(bytes[end]) {
        end += 1;
    }
    end
}

fn is_delimiter(byte: u8) -> bool {
    byte.is_ascii_whitespace() || matches!(byte, b';' | b'=' | b',' | b'"')
}

fn quoted(sql: &str, at: usize) -> Result<(String, usize), AdminError> {
    let bytes = sql.as_bytes();
    let mut name = String::new();
    let mut cursor = at + 1;
    loop {
        let offset = bytes[cursor..]
            .iter()
            .position(|byte| *byte == b'"')
            .ok_or(AdminError::UnclosedName)?;
        let close = cursor + offset;
        name.push_str(&sql[cursor..close]);
        if bytes.get(close + 1) == Some(&b'"') {
            name.push('"');
            cursor = close + 2;
        } else {
            return Ok((name, close + 1));
        }
    }
}
