//! Functions and types for interacting with AWS S3
//!
//! Asynchronously stream files from AWS S3, downloading different file ranges (i.e. chunks) in parallel to maximize throughput.
//!
//! TODO gate compilation behind some sort of feature flag like features = "s3"

use anyhow::{bail, Result};
use async_compat::CompatExt;
use bytes::{Bytes, BytesMut};
use futures::stream::{Stream, StreamExt};
use futures::task::Poll;
use futures::Future;
use futures::{ready, FutureExt};
use rusoto_core::request::{HttpClient, HttpConfig};
use rusoto_core::{credential::DefaultCredentialsProvider, Region};
use rusoto_s3::{GetObjectRequest, HeadObjectOutput, HeadObjectRequest, S3Client, S3};
use std::convert::TryInto;
use std::pin::Pin;
use std::task::Context;

#[pin_project::pin_project(project = ObjectChunksProj)]
/// [Stream] a file from Amazon S3 in `chunk_size` chunks by providing a byte `range` to the HTTP [GetObjectRequest].
///
/// Doing this is useful because AWS S3 is a block object store where different requests can be issued and serviced independently
/// and in parallel. When advancing the [ObjectChunks], [Future] structures are returned for retrieving the downloaded object chunk content.
/// With this design, callers can issue concurrent download requests for different file regions and take full advantage of instance and S3
/// network bandwidth and parallel file-serving capabilities.
pub struct ObjectChunks {
    next_chunk_start: usize,
    chunk_size: usize,
    bucket: String,
    key: String,
    client: std::sync::Arc<S3Client>, // TODO: share a client?
    file_size: Option<usize>,         // set on first stream call??
    head_object_request: Option<
        Pin<
            std::boxed::Box<
                dyn Future<
                        Output = std::result::Result<
                            HeadObjectOutput,
                            rusoto_core::RusotoError<rusoto_s3::HeadObjectError>,
                        >,
                    > + std::marker::Send,
            >,
        >,
    >,
}

const URI_PREFIX: &str = "s3://";

impl ObjectChunks {
    pub fn new(uri: &str, chunk_size: usize) -> Result<Pin<Box<ObjectChunks>>> {
        let uri = uri.trim_start_matches(URI_PREFIX);
        if let Some(bucket_delimiter_index) = uri.find('/') {
            let (bucket, key) = uri.split_at(bucket_delimiter_index);
            if key.len() < 1 {
                bail!(
                    "Invalid S3 URI: '{}'. Missing key following the '/' bucket delimiter",
                    uri
                );
            }

            // attempt to use a 8mb HTTP request buffer for better performance?
            let cred_provider = DefaultCredentialsProvider::new().unwrap();
            let mut http_config_with_bigger_buffer = HttpConfig::new();
            http_config_with_bigger_buffer.read_buf_size(1024 * 1024 * 8);
            let http_provider =
                HttpClient::new_with_config(http_config_with_bigger_buffer).unwrap();
            let client = std::sync::Arc::new(S3Client::new_with(
                http_provider,
                cred_provider,
                Region::UsEast1,
            ));

            let bucket = String::from(bucket);
            let key = String::from(&key[1..]);
            let stream = Box::pin(ObjectChunks {
                next_chunk_start: 0,
                chunk_size,
                client,
                bucket,
                key,
                file_size: None,
                head_object_request: None,
            });

            Ok(stream)
        } else {
            bail!("Invalid S3 URI: '{}'. Missing '/' bucket delimiter", uri);
        }
    }
}

// TODO: reimplement with TryStream in mind to propagate errors?
/* TODO: can I use some sort of impl Future instead of boxing it with a dyn future? */
impl Stream for ObjectChunks {
    type Item = Pin<
        Box<dyn Future<Output = std::io::Result<Bytes>> + std::marker::Unpin + std::marker::Send>,
    >;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let ObjectChunksProj {
            next_chunk_start,
            chunk_size,
            bucket,
            key,
            client,
            file_size,
            head_object_request,
        } = self.as_mut().project();

        if file_size.is_none() {
            if head_object_request.is_none() {
                let other_client = client.clone();
                let other_bucket = bucket.clone();
                let other_key = key.clone();
                *head_object_request = Some(
                    async move {
                        other_client
                            .head_object(HeadObjectRequest {
                                bucket: other_bucket,
                                key: other_key,
                                ..Default::default()
                            })
                            .compat()
                            .await
                    }
                    .boxed(),
                );
            }
            if let Some(request) = head_object_request {
                // return Poll::Pending until the saved HeadObjectRequest is ready
                let object_metadata = ready!(request.as_mut().poll(cx));
                *file_size = Some(
                    object_metadata
                        .unwrap()
                        .content_length
                        .unwrap()
                        .try_into()
                        .unwrap(),
                ); // TODO: graceful error vs unwrap
            }
        }

        if let Some(file_size) = *file_size {
            if *next_chunk_start > file_size {
                // done streaming the file
                return Poll::Ready(None);
            }
        }

        // request the next chunk
        let chunk_request = GetObjectRequest {
            bucket: bucket.clone(),
            key: key.clone(),
            range: Some(format!(
                "bytes={}-{}",
                next_chunk_start,
                *next_chunk_start + (*chunk_size - 1)
            )),
            ..Default::default()
        };

        let chunk_size = *chunk_size;
        let client = client.clone();
        let object = async move { client.get_object(chunk_request).compat().await };
        let next_chunk = object
            .map(|chunk_content| chunk_content.unwrap().body.take().expect("No body"))
            .then(move |mut chunk_content_byte_stream| async move {
                let mut body = BytesMut::with_capacity(chunk_size);
                while let Some(data) = chunk_content_byte_stream.next().await {
                    body.extend_from_slice(&data.unwrap());
                }
                std::io::Result::Ok(body.freeze())
            })
            .boxed();

        *next_chunk_start = *next_chunk_start + chunk_size;
        Poll::Ready(Some(Box::pin(next_chunk)))
    }
}

/* TODO: add S3 file download tests which confirm downloads happen in parallel when wrapped with TakeThenBuffered? */
