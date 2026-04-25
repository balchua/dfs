//! OpenTelemetry tracing integration.
//!
//! Configures a [`tracing_subscriber`] registry with layers for
//! human-readable or JSON logging and optional OTLP span export.
//!
//! #Log ↔ Trace correlation
//!
//! Every `#[tracing::instrument]` span and `info!`/`debug!` event
//! is logged to stderr. When `--otlp-endpoint` is set, spans are
//! exported to the collector (Jaeger, Tempo, etc.) and log events
//! emitted inside a span are correlated via the OTel trace id.
//!
//! In the human-readable output the tracing span fields (`trace_id`,
//! `span_id`) are printed by the fmt layer when the OTel layer is
//! active.

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Configuration for the tracing subsystem.
#[derive(Debug, Clone)]
pub struct TraceConfig {
    pub json: bool,
    pub otlp_endpoint: Option<String>,
    pub service_name: String,
}

///Initialise the tracing subscriber.
pub fn init_tracing(config: TraceConfig) -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    if let Some(ref endpoint) = config.otlp_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.clone())
            .with_timeout(std::time::Duration::from_secs(3))
            .build()?;

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .build();

        let tracer = tracer_provider.tracer("dfs-node");

        // Must keptive for tracing-opentelemetry to work.
        let otel_layer = OpenTelemetryLayer::new(tracer);

        if config.json {
            let stderr = tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_target(false);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(otel_layer)
                .with(stderr)
                .init();
        } else {
            let stderr = tracing_subscriber::fmt::layer()
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_target(false);
            tracing_subscriber::registry()
                .with(env_filter)
                .with(otel_layer)
                .with(stderr)
                .init();
        }
    } else if config.json {
        tracing_subscriber::fmt()
            .json()
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_env_filter(env_filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .with_env_filter(env_filter)
            .init();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "Test code uses unwrap for ergonomic assertions"
)]
mod tests {
    use super::*;

    #[test]
    fn init_without_otlp_does_not_error() {
        let cfg = TraceConfig {
            json: false,
            otlp_endpoint: None,
            service_name: "dfs-node-test".into(),
        };
        assert!(init_tracing(cfg).is_ok());
    }
}