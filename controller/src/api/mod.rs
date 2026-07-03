use tonic::transport::Server;

use crate::{
    RelayState,
    api::{
        blob::BlobService,
        log::LogService,
        proto::{
            blob_store_server::BlobStoreServer, logging_server::LoggingServer,
            relay_service_server::RelayServiceServer,
        },
        relay::RelayServiceImpl,
    },
};

mod blob;
mod log;
mod relay;

pub mod proto {
    tonic::include_proto!("zrp.relay.v1");
}

pub async fn serve(port: u16, state: RelayState) -> Result<(), Box<dyn std::error::Error>> {
    let addr = format!("0.0.0.0:{}", port).parse()?;

    Server::builder()
        .add_service(BlobStoreServer::new(
            BlobService::new(state.config.clone()).await,
        ))
        .add_service(LoggingServer::new(
            LogService::new(state.config.clone()).await,
        ))
        .add_service(RelayServiceServer::new(RelayServiceImpl { state }))
        .serve(addr)
        .await?;

    Ok(())
}
