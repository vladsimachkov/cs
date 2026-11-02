use coldstore_proto::tape::read_bundle_request::Location as TapeReadLocation;
use coldstore_proto::tape::read_bundle_response::Payload as TapeReadPayload;
use coldstore_proto::tape::tape_service_client::TapeServiceClient as TapeGrpcClient;
use coldstore_proto::tape::write_bundle_request::Payload as TapeWriteRequestPayload;
use coldstore_proto::tape::{
    AcquireDriveRequest, LoadTapeRequest, ReadBundleRequest as TapeReadBundleRequest,
    ReleaseDriveRequest, WriteBundleMeta as TapeWriteBundleMeta,
    WriteBundleRequest as TapeWriteBundleRequest,
};
use tokio::sync::OwnedSemaphorePermit;
use tonic::{Request, Status};

#[tonic::async_trait]
pub trait TapeArchiveWriter: Send + Sync {
    async fn write_bundle(
        &self,
        bundle_id: &str,
        object_count: u32,
        data: Vec<u8>,
    ) -> std::result::Result<TapeArchiveWrite, Status>;
}

#[derive(Clone)]
pub struct TapeArchiveClient {
    client: TapeGrpcClient<tonic::transport::Channel>,
    drive_id: String,
    tape_id: String,
    tape_set: Vec<String>,
    block_size: u32,
}

impl TapeArchiveClient {
    pub fn new(
        client: TapeGrpcClient<tonic::transport::Channel>,
        drive_id: impl Into<String>,
        tape_id: impl Into<String>,
        tape_set: Vec<String>,
        block_size: u32,
    ) -> Self {
        Self {
            client,
            drive_id: drive_id.into(),
            tape_id: tape_id.into(),
            tape_set,
            block_size,
        }
    }
}

#[tonic::async_trait]
pub trait TapeRecallReader: Send + Sync {
    async fn read_bundle(
        &self,
        tape_id: &str,
        filemark: u32,
        length: u64,
    ) -> std::result::Result<Vec<u8>, Status>;
}

#[derive(Clone)]
pub struct TapeRecallClient {
    client: TapeGrpcClient<tonic::transport::Channel>,
    drive_id: String,
}

impl TapeRecallClient {
    pub fn new(
        client: TapeGrpcClient<tonic::transport::Channel>,
        drive_id: impl Into<String>,
    ) -> Self {
        Self {
            client,
            drive_id: drive_id.into(),
        }
    }
}

async fn acquire_and_load_tape(
    client: &mut TapeGrpcClient<tonic::transport::Channel>,
    preferred_drive_id: &str,
    tape_id: &str,
) -> std::result::Result<String, Status> {
    let acquired = client
        .acquire_drive(Request::new(AcquireDriveRequest {
            preferred_drive_id: Some(preferred_drive_id.to_string()),
            required_tape_id: None,
            priority: 0,
            timeout_secs: 0,
        }))
        .await?
        .into_inner();
    let drive_id = if acquired.drive_id.is_empty() {
        preferred_drive_id.to_string()
    } else {
        acquired.drive_id
    };

    if acquired.current_tape.as_deref() != Some(tape_id) {
        client
            .load_tape(Request::new(LoadTapeRequest {
                tape_id: tape_id.to_string(),
                drive_id: drive_id.clone(),
                slot_id: None,
            }))
            .await?;
    }

    Ok(drive_id)
}

async fn release_drive_best_effort(
    client: &mut TapeGrpcClient<tonic::transport::Channel>,
    drive_id: &str,
) {
    let _ = client
        .release_drive(Request::new(ReleaseDriveRequest {
            drive_id: drive_id.to_string(),
        }))
        .await;
}

#[derive(Debug)]
pub(crate) struct TapeLease {
    pub(crate) _permit: OwnedSemaphorePermit,
    pub(crate) _tape_id: String,
}

#[tonic::async_trait]
impl TapeArchiveWriter for TapeArchiveClient {
    async fn write_bundle(
        &self,
        bundle_id: &str,
        object_count: u32,
        data: Vec<u8>,
    ) -> std::result::Result<TapeArchiveWrite, Status> {
        let mut client = self.client.clone();
        let drive_id = acquire_and_load_tape(&mut client, &self.drive_id, &self.tape_id).await?;
        let response = async {
            client
                .write_bundle(Request::new(tokio_stream::iter(vec![
                    TapeWriteBundleRequest {
                        payload: Some(TapeWriteRequestPayload::Meta(TapeWriteBundleMeta {
                            drive_id: drive_id.clone(),
                            bundle_id: bundle_id.into(),
                            total_size: data.len() as u64,
                            object_count,
                            block_size: self.block_size,
                        })),
                    },
                    TapeWriteBundleRequest {
                        payload: Some(TapeWriteRequestPayload::Data(data)),
                    },
                ])))
                .await
                .map(|response| response.into_inner())
        }
        .await;
        release_drive_best_effort(&mut client, &drive_id).await;
        let response = response?;

        if !response.success {
            return Err(Status::internal(format!(
                "tape write_bundle failed for {bundle_id}: {}",
                response
                    .error
                    .unwrap_or_else(|| "unknown tape error".into())
            )));
        }
        if response.drive_id != drive_id {
            return Err(Status::internal(format!(
                "tape write_bundle responded for drive {}, expected {}",
                response.drive_id, drive_id
            )));
        }
        if response.bundle_id != bundle_id {
            return Err(Status::internal(format!(
                "tape write_bundle responded for bundle {}, expected {bundle_id}",
                response.bundle_id
            )));
        }

        Ok(TapeArchiveWrite {
            tape_id: self.tape_id.clone(),
            tape_set: self.tape_set.clone(),
            filemark_start: response.filemark_start,
            filemark_end: response.filemark_end,
            bytes_written: response.bytes_written,
        })
    }
}

#[tonic::async_trait]
impl TapeRecallReader for TapeRecallClient {
    async fn read_bundle(
        &self,
        tape_id: &str,
        filemark: u32,
        length: u64,
    ) -> std::result::Result<Vec<u8>, Status> {
        let mut client = self.client.clone();
        let drive_id = acquire_and_load_tape(&mut client, &self.drive_id, tape_id).await?;
        let result = async {
            let mut stream = client
                .read_bundle(Request::new(TapeReadBundleRequest {
                    drive_id: drive_id.clone(),
                    location: Some(TapeReadLocation::Filemark(filemark)),
                    length,
                }))
                .await?
                .into_inner();

            let mut expected_size = None;
            let mut data = Vec::new();
            while let Some(message) = stream.message().await? {
                match message.payload {
                    Some(TapeReadPayload::Meta(meta)) => {
                        if expected_size.replace(meta.total_size).is_some() {
                            return Err(Status::internal("tape read returned duplicate metadata"));
                        }
                    }
                    Some(TapeReadPayload::Data(chunk)) => data.extend_from_slice(&chunk),
                    None => return Err(Status::internal("tape read returned empty chunk")),
                }
            }

            if let Some(expected_size) = expected_size {
                if expected_size != data.len() as u64 {
                    return Err(Status::data_loss(format!(
                        "tape read returned {} bytes, expected {}",
                        data.len(),
                        expected_size
                    )));
                }
            }
            Ok(data)
        }
        .await;
        release_drive_best_effort(&mut client, &drive_id).await;
        result
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TapeArchiveWrite {
    pub tape_id: String,
    pub tape_set: Vec<String>,
    pub filemark_start: u32,
    pub filemark_end: u32,
    pub bytes_written: u64,
}
