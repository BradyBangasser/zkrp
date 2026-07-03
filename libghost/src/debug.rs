use crate::relay::proto::logging_client::LoggingClient;
use crate::relay::proto::{LogUploadResponse, UploadChunk};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

const CHUNK_SIZE: usize = 16 * 1024;
static DEBUG_BUFFER: OnceLock<DebugLogBuffer> = OnceLock::new();

#[derive(Clone)]
pub struct DebugLogBuffer {
    inner: Arc<Mutex<Inner>>,
}
struct Inner {
    data: VecDeque<u8>,
    capacity: usize,
}
impl DebugLogBuffer {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                data: VecDeque::with_capacity(capacity_bytes.min(64 * 1024)),
                capacity: capacity_bytes.max(1),
            })),
        }
    }
    fn push(&self, line: &str) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.data.extend(line.as_bytes());
        let overflow = inner.data.len().saturating_sub(inner.capacity);
        if overflow > 0 {
            inner.data.drain(..overflow);
        }
    }
    pub fn snapshot(&self) -> Vec<u8> {
        let Ok(inner) = self.inner.lock() else {
            return Vec::new();
        };
        let mut bytes: Vec<u8> = inner.data.iter().copied().collect();
        if inner.data.len() >= inner.capacity
            && let Some(nl) = bytes.iter().position(|&b| b == b'\n')
        {
            bytes.drain(..=nl);
        }

        bytes
    }
    pub fn clear(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.data.clear();
        }
    }
}
pub fn init_buffer(capacity_bytes: usize) -> DebugLogBuffer {
    DEBUG_BUFFER
        .get_or_init(|| DebugLogBuffer::new(capacity_bytes))
        .clone()
}
pub fn global_buffer() -> Option<DebugLogBuffer> {
    DEBUG_BUFFER.get().cloned()
}
pub struct BufferLayer {
    buffer: DebugLogBuffer,
}
impl BufferLayer {
    pub fn new(buffer: DebugLogBuffer) -> Self {
        Self { buffer }
    }
}
impl<S> Layer<S> for BufferLayer
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        use std::fmt::Write as _;
        let meta = event.metadata();
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut line = String::with_capacity(96);
        let _ = write!(line, "{millis} {:>5} {}: ", meta.level(), meta.target());
        let mut visitor = LineVisitor { out: &mut line };
        event.record(&mut visitor);
        line.push('\n');
        self.buffer.push(&line);
    }
}

struct LineVisitor<'a> {
    out: &'a mut String,
}
impl Visit for LineVisitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        if field.name() == "message" {
            let _ = write!(self.out, "{value:?} ");
        } else {
            let _ = write!(self.out, "{}={value:?} ", field.name());
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        use std::fmt::Write as _;
        if field.name() == "message" {
            let _ = write!(self.out, "{value} ");
        } else {
            let _ = write!(self.out, "{}={value} ", field.name());
        }
    }
}
pub fn buffer_layer(capacity_bytes: usize) -> (BufferLayer, DebugLogBuffer) {
    let buffer = init_buffer(capacity_bytes);
    (BufferLayer::new(buffer.clone()), buffer)
}
pub fn init_tracing(level: tracing::Level, buffer_capacity_bytes: usize) {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::prelude::*;
    let (buf_layer, _buffer) = buffer_layer(buffer_capacity_bytes);
    let level_filter = LevelFilter::from_level(level);
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(level_filter))
        .with(buf_layer.with_filter(level_filter))
        .init();
}
pub async fn upload(
    grpc_addr: &str,
    data: Vec<u8>,
) -> Result<LogUploadResponse, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = LoggingClient::connect(grpc_addr.to_string()).await?;
    let chunks: Vec<UploadChunk> = data
        .chunks(CHUNK_SIZE)
        .map(|c| UploadChunk { data: c.to_vec() })
        .collect();
    let stream = futures::stream::iter(chunks);
    let response = client.upload_debug_log(stream).await?;
    Ok(response.into_inner())
}
