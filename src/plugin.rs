//! Stdio JSON-RPC loop for the `animus-queue-postgres` plugin.
//!
//! Handles `initialize`, `$/ping`, `health/check`, `shutdown`, `exit`,
//! `--manifest` / `--help` CLI shortcuts, and the 12 `queue/*` methods — the
//! EXACT method set + request/response shapes of `animus-queue-default`
//! (`animus-queue-protocol` v0.5.10).
//!
//! The Postgres pool is opened lazily at `initialize` (not at process start)
//! so the `--manifest` probe `animus install --locked` runs at build time
//! never needs a reachable database.

use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes as plugin_error_codes, EnvRequirement, HealthCheckResult, HealthStatus,
    InitializeResult, KindCapability, PluginCapabilities, PluginInfo, PluginManifest, RpcError,
    RpcRequest, RpcResponse, PLUGIN_KIND_QUEUE, PROTOCOL_VERSION,
};
use animus_queue_protocol::{
    error_codes as queue_error_codes, QueueCapabilities, QueueCompletionRequest, QueueDropRequest,
    QueueEnqueueRequest, QueueEnqueueResponse, QueueHoldRequest, QueueLeaseRequest,
    QueueListRequest, QueueMarkAssignedRequest, QueueReleasePendingParams, QueueReleaseRequest,
    QueueReorderRequest, KIND, METHOD_QUEUE_COMPLETION, METHOD_QUEUE_DROP, METHOD_QUEUE_ENQUEUE,
    METHOD_QUEUE_HOLD, METHOD_QUEUE_LEASE, METHOD_QUEUE_LIST, METHOD_QUEUE_MARK_ASSIGNED,
    METHOD_QUEUE_NEXT_DEADLINE, METHOD_QUEUE_RELEASE, METHOD_QUEUE_RELEASE_PENDING,
    METHOD_QUEUE_REORDER, METHOD_QUEUE_STATS, PROTOCOL_VERSION as QUEUE_PROTOCOL_VERSION,
};
use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

use crate::config::QueueConfig;
use crate::store::{QueueLeaseError, QueueReleasePendingError, Store};

const PLUGIN_NAME: &str = "animus-queue-postgres";
const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");
const PLUGIN_DESCRIPTION: &str =
    "Durable Postgres-backed queue plugin for Animus (survives restarts; reclaims crashed leases).";

/// Stable entrypoint. Call from `#[tokio::main]` in `main.rs`.
pub async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    if handle_cli_args() {
        return Ok(());
    }

    if io::stdin().is_terminal() {
        eprintln!("{PLUGIN_NAME} is a STDIO plugin; pipe JSON-RPC on stdin or pass --manifest");
        std::process::exit(2);
    }

    let backend: Arc<RwLock<Option<Store>>> = Arc::new(RwLock::new(None));
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));

    let mut stdin = tokio::io::stdin();
    let mut buffer: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 4096];
    loop {
        let n = stdin.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);

        loop {
            let leading_ws = buffer
                .iter()
                .take_while(|b| b.is_ascii_whitespace())
                .count();
            if leading_ws > 0 {
                buffer.drain(..leading_ws);
            }
            if buffer.is_empty() {
                break;
            }
            let mut stream =
                serde_json::Deserializer::from_slice(&buffer).into_iter::<RpcRequest>();
            match stream.next() {
                Some(Ok(request)) => {
                    let consumed = stream.byte_offset();
                    drop(stream);
                    buffer.drain(..consumed);
                    let backend = backend.clone();
                    let stdout = stdout.clone();
                    tokio::spawn(async move {
                        handle_request(request, backend, stdout).await;
                    });
                }
                Some(Err(error)) if error.is_eof() => break,
                Some(Err(error)) => {
                    tracing::warn!(plugin = PLUGIN_NAME, %error, "invalid JSON-RPC frame");
                    if let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
                        buffer.drain(..=pos);
                        continue;
                    }
                    break;
                }
                None => break,
            }
        }
    }
    Ok(())
}

fn handle_cli_args() -> bool {
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--manifest" | "-m" => {
                print_manifest();
                return true;
            }
            "--help" | "-h" => {
                eprintln!("{PLUGIN_NAME} {PLUGIN_VERSION} — STDIO durable queue plugin for Animus");
                eprintln!("Usage:");
                eprintln!("  {PLUGIN_NAME} --manifest    Print plugin manifest as JSON and exit");
                eprintln!("  {PLUGIN_NAME}               Run JSON-RPC loop on stdin/stdout");
                eprintln!("Env: DATABASE_URL (or ANIMUS_POSTGRES_URL), ANIMUS_QUEUE_LEASE_TTL_SECS, ANIMUS_QUEUE_TABLE");
                return true;
            }
            _ => {}
        }
    }
    false
}

fn print_manifest() {
    let manifest = PluginManifest {
        name: PLUGIN_NAME.to_string(),
        version: PLUGIN_VERSION.to_string(),
        plugin_kind: PLUGIN_KIND_QUEUE.to_string(),
        description: PLUGIN_DESCRIPTION.to_string(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        capabilities: queue_methods().into_iter().map(|m| m.to_string()).collect(),
        // The plugin host spawns plugins with a CLEAN environment and forwards
        // only the variables declared here. Advertise the DB connection + queue
        // tuning vars so a host-spawned process (outside the Docker wrapper that
        // sources subject-pg.env) still receives them. Mirrors the sibling
        // Postgres plugins.
        env_required: env_requirements(),
        notification_buffer_size: None,
    };
    let mut stdout = io::stdout().lock();
    let _ = writeln!(
        stdout,
        "{}",
        serde_json::to_string(&manifest).expect("serialize manifest")
    );
    let _ = stdout.flush();
}

/// Environment the plugin host must forward to a host-spawned process. The
/// host clears the environment and forwards only these names.
fn env_requirements() -> Vec<EnvRequirement> {
    vec![
        EnvRequirement {
            name: "DATABASE_URL".to_string(),
            description: Some(
                "Postgres connection URL (e.g. postgres://user:pass@host:5432/dbname).".to_string(),
            ),
            sensitive: true,
            required: false,
        },
        EnvRequirement {
            name: "ANIMUS_POSTGRES_URL".to_string(),
            description: Some("Fallback Postgres URL used when DATABASE_URL is unset.".to_string()),
            sensitive: true,
            required: false,
        },
        EnvRequirement {
            name: "ANIMUS_QUEUE_LEASE_TTL_SECS".to_string(),
            description: Some(
                "Lease TTL in seconds; an expired lease is reclaimable (default 1800).".to_string(),
            ),
            sensitive: false,
            required: false,
        },
        EnvRequirement {
            name: "ANIMUS_QUEUE_TABLE".to_string(),
            description: Some("Durable queue table name (default queue_item).".to_string()),
            sensitive: false,
            required: false,
        },
        EnvRequirement {
            name: "TOKIO_WORKER_THREADS".to_string(),
            description: Some(
                "Caps the plugin's tokio worker threads (set by the daemon to avoid PID exhaustion)."
                    .to_string(),
            ),
            sensitive: false,
            required: false,
        },
    ]
}

fn queue_methods() -> Vec<&'static str> {
    vec![
        METHOD_QUEUE_ENQUEUE,
        METHOD_QUEUE_LIST,
        METHOD_QUEUE_LEASE,
        METHOD_QUEUE_STATS,
        METHOD_QUEUE_NEXT_DEADLINE,
        METHOD_QUEUE_HOLD,
        METHOD_QUEUE_RELEASE,
        METHOD_QUEUE_RELEASE_PENDING,
        METHOD_QUEUE_DROP,
        METHOD_QUEUE_REORDER,
        METHOD_QUEUE_MARK_ASSIGNED,
        METHOD_QUEUE_COMPLETION,
        "health/check",
    ]
}

async fn handle_request(
    request: RpcRequest,
    backend: Arc<RwLock<Option<Store>>>,
    stdout: Arc<Mutex<tokio::io::Stdout>>,
) {
    let id = request.id.clone();
    let response = match request.method.as_str() {
        "initialize" => Some(handle_initialize(id, &backend).await),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(health_check(id, &backend).await),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        "exit" => std::process::exit(0),
        other if other.starts_with("$/") => None,
        METHOD_QUEUE_ENQUEUE => Some(handle_enqueue(id, request.params, &backend).await),
        METHOD_QUEUE_LIST => Some(handle_list(id, request.params, &backend).await),
        METHOD_QUEUE_LEASE => Some(handle_lease(id, request.params, &backend).await),
        METHOD_QUEUE_STATS => Some(handle_stats(id, &backend).await),
        METHOD_QUEUE_NEXT_DEADLINE => Some(handle_next_deadline(id, &backend).await),
        METHOD_QUEUE_HOLD => Some(handle_hold(id, request.params, &backend).await),
        METHOD_QUEUE_RELEASE => Some(handle_release(id, request.params, &backend).await),
        METHOD_QUEUE_RELEASE_PENDING => {
            Some(handle_release_pending(id, request.params, &backend).await)
        }
        METHOD_QUEUE_DROP => Some(handle_drop(id, request.params, &backend).await),
        METHOD_QUEUE_REORDER => Some(handle_reorder(id, request.params, &backend).await),
        METHOD_QUEUE_MARK_ASSIGNED => {
            Some(handle_mark_assigned(id, request.params, &backend).await)
        }
        METHOD_QUEUE_COMPLETION => Some(handle_completion(id, request.params, &backend).await),
        other => Some(RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::METHOD_NOT_FOUND,
                message: format!("method '{other}' not implemented by {PLUGIN_NAME}"),
                data: None,
            },
        )),
    };

    if let Some(response) = response {
        write_frame(&stdout, &response).await;
    }
}

async fn write_frame<T: serde::Serialize>(stdout: &Arc<Mutex<tokio::io::Stdout>>, frame: &T) {
    if let Ok(mut payload) = serde_json::to_string(frame) {
        payload.push('\n');
        let mut guard = stdout.lock().await;
        let _ = guard.write_all(payload.as_bytes()).await;
        let _ = guard.flush().await;
    }
}

async fn health_check(id: Option<Value>, backend: &Arc<RwLock<Option<Store>>>) -> RpcResponse {
    let (status, last_error) = match backend.read().await.as_ref() {
        Some(store) => match store.ping().await {
            Ok(()) => (HealthStatus::Healthy, None),
            Err(error) => (
                HealthStatus::Unhealthy,
                Some(format!("Postgres unreachable: {error}")),
            ),
        },
        // Not yet initialized: the process is up but has no pool to probe.
        None => (HealthStatus::Healthy, None),
    };
    let result = HealthCheckResult {
        status,
        uptime_ms: None,
        memory_usage_bytes: None,
        last_error,
    };
    match serde_json::to_value(result) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => {
            internal_error_response(id, format!("failed to encode health result: {error}"))
        }
    }
}

async fn handle_initialize(id: Option<Value>, backend: &Arc<RwLock<Option<Store>>>) -> RpcResponse {
    // Open (or reuse) the Postgres pool. The queue is keyed by DATABASE_URL —
    // one durable queue per database — so `project_binding` is accepted but not
    // required (cf. animus-config-postgres, which is also DB-keyed).
    {
        let mut guard = backend.write().await;
        if guard.is_none() {
            let config = match QueueConfig::from_env() {
                Ok(config) => config,
                Err(error) => {
                    return RpcResponse::err(
                        id,
                        RpcError {
                            code: plugin_error_codes::INTERNAL_ERROR,
                            message: format!("{PLUGIN_NAME} initialize failed: {error}"),
                            data: None,
                        },
                    );
                }
            };
            match Store::open(&config).await {
                Ok(store) => *guard = Some(store),
                Err(error) => {
                    return RpcResponse::err(
                        id,
                        RpcError {
                            code: plugin_error_codes::INTERNAL_ERROR,
                            message: format!("{PLUGIN_NAME} could not open Postgres: {error:#}"),
                            data: None,
                        },
                    );
                }
            }
        }
    }

    let capabilities = QueueCapabilities {
        // Strict FIFO within Pending, matching the reference plugin. The
        // `priority` column is stored for observability but does not weight
        // dispatch order.
        priority_weighted: false,
        // No backend-side cap on lease batch size.
        max_lease_batch: u32::MAX,
    };
    let extra = serde_json::to_value(capabilities).unwrap_or(Value::Null);
    let mut kind_capabilities = std::collections::HashMap::new();
    kind_capabilities.insert(
        KIND.to_string(),
        KindCapability {
            crate_version: QUEUE_PROTOCOL_VERSION.to_string(),
            extra,
        },
    );

    let result = InitializeResult {
        protocol_version: PROTOCOL_VERSION.to_string(),
        plugin_info: PluginInfo {
            name: PLUGIN_NAME.to_string(),
            version: PLUGIN_VERSION.to_string(),
            plugin_kind: PLUGIN_KIND_QUEUE.to_string(),
            description: Some(PLUGIN_DESCRIPTION.to_string()),
        },
        capabilities: PluginCapabilities {
            methods: queue_methods()
                .into_iter()
                .map(ToString::to_string)
                .collect(),
            streaming: false,
            progress: false,
            cancellation: false,
            projections: Vec::new(),
            subject_kinds: Vec::new(),
            mcp_tools: Vec::new(),
        },
        kind_capabilities,
    };

    match serde_json::to_value(result) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => {
            internal_error_response(id, format!("failed to encode initialize result: {error}"))
        }
    }
}

// ============================================================
// queue/* handlers
// ============================================================

async fn handle_enqueue(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueEnqueueRequest = match parse_params(id.clone(), params, "queue/enqueue") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match store
        .enqueue(
            request.subject_dispatch,
            request.run_at,
            request.expire_after_secs,
        )
        .await
    {
        Ok(outcome) => to_value_response(
            id,
            &QueueEnqueueResponse {
                enqueued: outcome.enqueued,
                entry_id: outcome.entry_id,
                subject_id: outcome.subject_id,
                warning: outcome.warning,
            },
        ),
        Err(error) => internal_error_response(id, format!("queue/enqueue failed: {error:#}")),
    }
}

async fn handle_list(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueListRequest = match params {
        Some(value) => match serde_json::from_value(value) {
            Ok(req) => req,
            Err(error) => {
                return RpcResponse::err(
                    id,
                    invalid_params(format!("invalid queue/list params: {error}")),
                );
            }
        },
        None => QueueListRequest::default(),
    };
    match store
        .list(&request.status, request.limit, request.offset)
        .await
    {
        Ok(response) => to_value_response(id, &response),
        Err(error) => internal_error_response(id, format!("queue/list failed: {error:#}")),
    }
}

async fn handle_lease(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueLeaseRequest = match parse_params(id.clone(), params, "queue/lease") {
        Ok(req) => req,
        Err(response) => return response,
    };
    let exclude_subjects = request
        .exclude_subjects
        .map(|ids| ids.into_iter().map(|id| id.0).collect::<Vec<String>>());
    match store
        .lease(request.max, request.workflow_ids, exclude_subjects)
        .await
    {
        Ok(response) => to_value_response(id, &response),
        Err(QueueLeaseError::WorkflowIdCountMismatch { expected, actual }) => RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_LEASE_WORKFLOW_ID_COUNT_MISMATCH,
                message: format!("workflow_ids.len()={actual} did not match max={expected}"),
                data: Some(json!({ "expected": expected, "actual": actual })),
            },
        ),
        Err(QueueLeaseError::Backend(error)) => {
            internal_error_response(id, format!("queue/lease failed: {error:#}"))
        }
    }
}

async fn handle_stats(id: Option<Value>, backend: &Arc<RwLock<Option<Store>>>) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    match store.stats().await {
        Ok(stats) => to_value_response(id, &stats),
        Err(error) => internal_error_response(id, format!("queue/stats failed: {error:#}")),
    }
}

async fn handle_next_deadline(
    id: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    match store.next_deadline().await {
        Ok(resp) => to_value_response(id, &resp),
        Err(error) => internal_error_response(id, format!("queue/next_deadline failed: {error:#}")),
    }
}

async fn handle_hold(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueHoldRequest = match parse_params(id.clone(), params, "queue/hold") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match store.hold(&request.entry_id).await {
        Ok(response) => to_value_response(id, &response),
        Err(error) => not_pending_or_internal(id, &error, "queue/hold"),
    }
}

async fn handle_release(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueReleaseRequest = match parse_params(id.clone(), params, "queue/release") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match store.release(&request.entry_id).await {
        Ok(response) => to_value_response(id, &response),
        Err(error) => not_pending_or_internal(id, &error, "queue/release"),
    }
}

async fn handle_release_pending(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueReleasePendingParams =
        match parse_params(id.clone(), params, "queue/release_pending") {
            Ok(req) => req,
            Err(response) => return response,
        };
    match store
        .release_pending(&request.entry_id, &request.reason)
        .await
    {
        Ok(response) => to_value_response(id, &response),
        // Mirror queue-default: missing entry → -32602 invalid_params.
        Err(QueueReleasePendingError::NotFound { entry_id }) => RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::INVALID_PARAMS,
                message: format!("entry_id not found: {entry_id}"),
                data: None,
            },
        ),
        Err(QueueReleasePendingError::NotAssigned {
            entry_id,
            actual_state,
        }) => RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_ENTRY_NOT_ASSIGNED,
                message: format!(
                    "entry {entry_id} is in state '{actual_state}', expected 'assigned'"
                ),
                data: Some(json!({ "actual_state": actual_state })),
            },
        ),
        Err(QueueReleasePendingError::Backend(error)) => {
            internal_error_response(id, format!("queue/release_pending failed: {error:#}"))
        }
    }
}

async fn handle_drop(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueDropRequest = match parse_params(id.clone(), params, "queue/drop") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match store.drop_entry(&request.entry_id).await {
        Ok(response) => to_value_response(id, &response),
        Err(error) => internal_error_response(id, format!("queue/drop failed: {error:#}")),
    }
}

async fn handle_reorder(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueReorderRequest = match parse_params(id.clone(), params, "queue/reorder") {
        Ok(req) => req,
        Err(response) => return response,
    };
    match store.reorder(&request.entry_ids).await {
        Ok(response) => to_value_response(id, &response),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_REORDER_FAILED,
                message: format!("queue/reorder failed: {error:#}"),
                data: None,
            },
        ),
    }
}

async fn handle_mark_assigned(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueMarkAssignedRequest =
        match parse_params(id.clone(), params, "queue/mark_assigned") {
            Ok(req) => req,
            Err(response) => return response,
        };
    match store
        .mark_assigned(&request.entry_id, request.workflow_id)
        .await
    {
        Ok(response) => to_value_response(id, &response),
        Err(error) => not_pending_or_internal(id, &error, "queue/mark_assigned"),
    }
}

async fn handle_completion(
    id: Option<Value>,
    params: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> RpcResponse {
    let store = match require_backend(id.clone(), backend).await {
        Ok(s) => s,
        Err(response) => return response,
    };
    let request: QueueCompletionRequest = match parse_params(id.clone(), params, "queue/completion")
    {
        Ok(req) => req,
        Err(response) => return response,
    };
    match store
        .completion(
            &request.entry_id,
            &request.status,
            request.workflow_ref.as_deref(),
            request.workflow_id.as_deref(),
        )
        .await
    {
        Ok(response) => to_value_response(id, &response),
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::INVALID_PARAMS,
                message: format!("queue/completion failed: {error:#}"),
                data: None,
            },
        ),
    }
}

// ============================================================
// helpers
// ============================================================

async fn require_backend(
    id: Option<Value>,
    backend: &Arc<RwLock<Option<Store>>>,
) -> std::result::Result<Store, RpcResponse> {
    match backend.read().await.as_ref().cloned() {
        Some(store) => Ok(store),
        None => Err(RpcResponse::err(
            id,
            RpcError {
                code: plugin_error_codes::PLUGIN_NOT_INITIALIZED,
                message: format!("{PLUGIN_NAME} received a queue/* method before initialize"),
                data: None,
            },
        )),
    }
}

fn parse_params<T: serde::de::DeserializeOwned>(
    id: Option<Value>,
    params: Option<Value>,
    method: &str,
) -> std::result::Result<T, RpcResponse> {
    let value = params.ok_or_else(|| {
        RpcResponse::err(
            id.clone(),
            invalid_params(format!("missing params for {method}")),
        )
    })?;
    serde_json::from_value::<T>(value).map_err(|error| {
        RpcResponse::err(
            id,
            invalid_params(format!("invalid {method} params: {error}")),
        )
    })
}

fn to_value_response<T: serde::Serialize>(id: Option<Value>, value: &T) -> RpcResponse {
    match serde_json::to_value(value) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => internal_error_response(id, format!("failed to encode response: {error}")),
    }
}

fn not_pending_or_internal(id: Option<Value>, error: &anyhow::Error, method: &str) -> RpcResponse {
    let msg = error.to_string();
    if msg.contains("not in the expected pre-mutation status") {
        return RpcResponse::err(
            id,
            RpcError {
                code: queue_error_codes::QUEUE_ENTRY_NOT_PENDING,
                message: msg,
                data: None,
            },
        );
    }
    internal_error_response(id, format!("{method} failed: {error:#}"))
}

fn invalid_params(message: impl Into<String>) -> RpcError {
    RpcError {
        code: plugin_error_codes::INVALID_PARAMS,
        message: message.into(),
        data: None,
    }
}

fn internal_error_response(id: Option<Value>, message: impl Into<String>) -> RpcResponse {
    RpcResponse::err(
        id,
        RpcError {
            code: plugin_error_codes::INTERNAL_ERROR,
            message: message.into(),
            data: None,
        },
    )
}
