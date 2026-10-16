//! Prometheus metrics for zkrp services.
//!
//! Every label value comes from a fixed set (service and method names from an
//! allowlist, gRPC status names, outcome enums). Nothing here may ever be
//! labeled with a peer id, address, mailbox id, or email.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

pub use metrics::{counter, gauge, histogram};

/// Latency buckets in seconds, tuned for unary RPCs and S3-backed uploads.
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Serve `/metrics` on `addr` (from `METRICS_ADDR`, else `default`). Must be
/// called inside a Tokio runtime. A failure is logged, not fatal: losing
/// metrics must never take a relay down.
pub fn install(default: &str) {
    let addr = std::env::var("METRICS_ADDR").unwrap_or_else(|_| default.to_string());
    let addr: SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            tracing::error!("METRICS_ADDR {addr:?}: {e}; metrics disabled");
            return;
        }
    };
    let result = PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), LATENCY_BUCKETS)
        .and_then(|b| b.install());
    match result {
        Ok(()) => tracing::info!("metrics on http://{addr}/metrics"),
        Err(e) => tracing::error!("metrics exporter failed: {e}"),
    }
}

fn grpc_code_name(code: &str) -> &'static str {
    match code {
        "0" => "OK",
        "1" => "CANCELLED",
        "2" => "UNKNOWN",
        "3" => "INVALID_ARGUMENT",
        "4" => "DEADLINE_EXCEEDED",
        "5" => "NOT_FOUND",
        "6" => "ALREADY_EXISTS",
        "7" => "PERMISSION_DENIED",
        "8" => "RESOURCE_EXHAUSTED",
        "9" => "FAILED_PRECONDITION",
        "10" => "ABORTED",
        "11" => "OUT_OF_RANGE",
        "12" => "UNIMPLEMENTED",
        "13" => "INTERNAL",
        "14" => "UNAVAILABLE",
        "15" => "DATA_LOSS",
        "16" => "UNAUTHENTICATED",
        _ => "UNKNOWN",
    }
}

/// Codes that count against the availability SLO: the server's fault, not the
/// caller's.
pub fn is_server_error(code: &str) -> bool {
    matches!(code, "UNKNOWN" | "INTERNAL" | "UNAVAILABLE" | "DATA_LOSS" | "DEADLINE_EXCEEDED")
}

/// Records `zkrp_grpc_requests_total{service,method,code}` and
/// `zkrp_grpc_request_duration_seconds{service,method}` for every call.
///
/// The status is read from the response headers, which is where tonic puts it
/// for every error returned from a handler or interceptor. A stream that fails
/// after its first message reports its status in trailers and is counted OK.
#[derive(Clone)]
pub struct GrpcMetricsLayer {
    services: Arc<[&'static str]>,
}

impl GrpcMetricsLayer {
    /// `services`: fully-qualified gRPC service names to label individually.
    /// Calls to anything else are labeled `other`, so random paths can't blow
    /// up label cardinality.
    pub fn new(services: &[&'static str]) -> Self {
        Self {
            services: services.into(),
        }
    }
}

impl<S> tower::Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcMetrics {
            inner,
            services: self.services.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcMetrics<S> {
    inner: S,
    services: Arc<[&'static str]>,
}

/// (service, method) for a gRPC path `/pkg.Service/Method`, both from the
/// allowlist or `other`.
fn route(services: &[&'static str], path: &str) -> (&'static str, String) {
    let mut parts = path.trim_start_matches('/').splitn(2, '/');
    let (svc, method) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    match services.iter().find(|s| **s == svc) {
        Some(s) if !method.is_empty() && method.len() <= 64 && method.bytes().all(|b| b.is_ascii_alphanumeric()) => {
            (*s, method.to_string())
        }
        _ => ("other", "other".to_string()),
    }
}

impl<S, ReqB, ResB> tower::Service<http::Request<ReqB>> for GrpcMetrics<S>
where
    S: tower::Service<http::Request<ReqB>, Response = http::Response<ResB>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqB>) -> Self::Future {
        let (service, method) = route(&self.services, req.uri().path());
        let start = Instant::now();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let res = fut.await;
            let code = match &res {
                Ok(r) => grpc_code_name(
                    r.headers()
                        .get("grpc-status")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("0"),
                ),
                Err(_) => "UNAVAILABLE",
            };
            counter!("zkrp_grpc_requests_total",
                "service" => service, "method" => method.clone(), "code" => code)
            .increment(1);
            histogram!("zkrp_grpc_request_duration_seconds",
                "service" => service, "method" => method)
            .record(start.elapsed().as_secs_f64());
            res
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_only_known_services() {
        let s = ["zrp.relay.v1.BlobStore"];
        assert_eq!(route(&s, "/zrp.relay.v1.BlobStore/UploadBlob"), ("zrp.relay.v1.BlobStore", "UploadBlob".into()));
        assert_eq!(route(&s, "/evil.Service/Anything"), ("other", "other".into()));
        assert_eq!(route(&s, "/zrp.relay.v1.BlobStore/a%2Fb"), ("other", "other".into()));
    }

    #[test]
    fn server_errors() {
        assert!(is_server_error(grpc_code_name("14")));
        assert!(!is_server_error(grpc_code_name("16")));
        assert!(!is_server_error(grpc_code_name("0")));
    }
}
