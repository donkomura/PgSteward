use std::io;

use bytes::BytesMut;
use pgsteward_protocol::backend::{
    encode_backend_key_data, encode_parameter_status, encode_ready_for_query,
};
use pgsteward_protocol::message::TransactionStatus;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional};

use crate::server::ServerConnection;
use crate::session::ClientSession;

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("i/o error while relaying a session: {0}")]
    Io(#[from] io::Error),
}

pub async fn session_mode<C, S>(
    client: ClientSession<C>,
    server: ServerConnection<S>,
) -> Result<(), RelayError>
where
    C: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut greeting = BytesMut::new();
    for (name, value) in server.parameters() {
        encode_parameter_status(name, value, &mut greeting);
    }
    encode_backend_key_data(server.backend_key(), &mut greeting);
    encode_ready_for_query(TransactionStatus::Idle, &mut greeting);

    let (mut client_stream, from_client) = client.into_parts();
    let (mut server_stream, from_server) = server.into_parts();

    greeting.extend_from_slice(&from_server);
    client_stream.write_all(&greeting).await?;
    client_stream.flush().await?;
    if !from_client.is_empty() {
        server_stream.write_all(&from_client).await?;
        server_stream.flush().await?;
    }

    copy_bidirectional(&mut client_stream, &mut server_stream).await?;
    Ok(())
}
