#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MessageError {
    #[error("unknown backend message tag {0:?}")]
    UnknownBackendTag(u8),
    #[error("unknown frontend message tag {0:?}")]
    UnknownFrontendTag(u8),
    #[error("unknown transaction status {0:?} in ReadyForQuery")]
    UnknownTransactionStatus(u8),
    #[error("malformed {tag:?} body: {reason}")]
    MalformedBody {
        tag: BackendTag,
        reason: &'static str,
    },
}

macro_rules! tags {
    ($name:ident, $error:ident, { $($variant:ident = $byte:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: [$name; tags!(@count $($variant)+)] = [$($name::$variant),+];
        }

        impl TryFrom<u8> for $name {
            type Error = MessageError;

            fn try_from(byte: u8) -> Result<Self, MessageError> {
                match byte {
                    $($byte => Ok($name::$variant),)+
                    other => Err(MessageError::$error(other)),
                }
            }
        }

        impl From<$name> for u8 {
            fn from(tag: $name) -> u8 {
                match tag {
                    $($name::$variant => $byte,)+
                }
            }
        }
    };
    (@count) => { 0usize };
    (@count $head:ident $($tail:ident)*) => { 1usize + tags!(@count $($tail)*) };
}

tags!(BackendTag, UnknownBackendTag, {
    Authentication = b'R',
    BackendKeyData = b'K',
    BindComplete = b'2',
    CloseComplete = b'3',
    CommandComplete = b'C',
    CopyData = b'd',
    CopyDone = b'c',
    CopyInResponse = b'G',
    CopyOutResponse = b'H',
    CopyBothResponse = b'W',
    DataRow = b'D',
    EmptyQueryResponse = b'I',
    ErrorResponse = b'E',
    FunctionCallResponse = b'V',
    NegotiateProtocolVersion = b'v',
    NoData = b'n',
    NoticeResponse = b'N',
    NotificationResponse = b'A',
    ParameterDescription = b't',
    ParameterStatus = b'S',
    ParseComplete = b'1',
    PortalSuspended = b's',
    ReadyForQuery = b'Z',
    RowDescription = b'T',
});

tags!(FrontendTag, UnknownFrontendTag, {
    Bind = b'B',
    Close = b'C',
    CopyData = b'd',
    CopyDone = b'c',
    CopyFail = b'f',
    Describe = b'D',
    Execute = b'E',
    Flush = b'H',
    FunctionCall = b'F',
    Parse = b'P',
    Password = b'p',
    Query = b'Q',
    Sync = b'S',
    Terminate = b'X',
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    Failed,
}

impl TryFrom<u8> for TransactionStatus {
    type Error = MessageError;

    fn try_from(byte: u8) -> Result<Self, MessageError> {
        match byte {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::InTransaction),
            b'E' => Ok(Self::Failed),
            other => Err(MessageError::UnknownTransactionStatus(other)),
        }
    }
}

impl From<TransactionStatus> for u8 {
    fn from(status: TransactionStatus) -> Self {
        match status {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::Failed => b'E',
        }
    }
}

pub fn parse_ready_for_query(body: &[u8]) -> Result<TransactionStatus, MessageError> {
    match body {
        [status] => TransactionStatus::try_from(*status),
        _ => Err(MessageError::MalformedBody {
            tag: BackendTag::ReadyForQuery,
            reason: "expected exactly one status byte",
        }),
    }
}
