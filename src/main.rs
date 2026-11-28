//! Vicuna Slack Bridge Plugin (Socket Mode)
//!
//! Speaks Vicuna's newline-delimited JSON-RPC bridge protocol over stdin/stdout.
//! Connects to Slack over Socket Mode (`apps.connections.open` + WebSocket) so
//! no public webhook URL is needed. Multiplexes multiple Work sessions sharing
//! the same `xapp-` App-Level Token over a single Socket Mode connection while
//! also supporting distinct Slack apps per session.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMsg;

const MAX_MSG: usize = 3800;
const MAX_SECTION_TEXT: usize = 2950;
const MAX_FILE: usize = 20 * 1024 * 1024;
const MAX_IMAGE_INLINE: usize = 10 * 1024 * 1024;

// ---------- config store ----------

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BridgeConfig {
    /// Bot User OAuth Token (`xoxb-...`)
    #[serde(default)]
    pub bot_token: String,
    /// App-Level Token (`xapp-...`, scope `connections:write`)
    #[serde(default)]
    pub app_token: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bot_user_id: String,
    #[serde(default)]
    pub bot_id: String,
    #[serde(default)]
    pub bot_username: String,
    #[serde(default)]
    pub team_name: String,
    /// Paired Slack user ID and DM / private channel ID
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub user_name: String,
    #[serde(default)]
    pub pair_code: String,
}

impl BridgeConfig {
    pub fn is_configured(&self) -> bool {
        !self.bot_token.is_empty() && !self.app_token.is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Store {
    #[serde(default)]
    pub bridges: HashMap<String, BridgeConfig>,
}

fn config_path() -> PathBuf {
    if let Ok(p) = std::env::var("VICUNA_BRIDGE_CONFIG_PATH") {
        if !p.trim().is_empty() {
            return PathBuf::from(p);
        }
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".into());
    PathBuf::from(home)
        .join("vicuna")
        .join("config")
        .join("plugins")
        .join("slack.json")
}

pub fn load_store_from(path: &Path) -> Store {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

pub fn save_store_to(path: &Path, s: &Store) -> Result<(), String> {
    if s.bridges.is_empty() {
        let _ = std::fs::remove_file(path);
        bump_all();
        return Ok(());
    }
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d).map_err(|e| e.to_string())?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(s).unwrap_or_default())
        .map_err(|e| e.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())?;
    bump_all();
    Ok(())
}

fn load_store() -> Store {
    load_store_from(&config_path())
}

fn save_store(s: &Store) -> Result<(), String> {
    save_store_to(&config_path(), s)
}

fn new_pair_code() -> String {
    let mut b = [0u8; 4];
    let _ = getrandom::fill(&mut b);
    format!("{:06}", u32::from_le_bytes(b) % 1_000_000)
}

pub fn token_tail(t: &str) -> String {
    let n = t.chars().count();
    if n <= 4 {
        "••••".into()
    } else {
        format!("••••{}", t.chars().skip(n - 4).collect::<String>())
    }
}

pub fn validate_bot_token_format(token: &str) -> Result<(), String> {
    let t = token.trim();
    if t.starts_with("xapp-") {
        return Err(
            "That's an App-Level Token (xapp-…). First paste your Bot User OAuth Token (xoxb-…) from OAuth & Permissions."
                .into(),
        );
    }
    if !t.starts_with("xoxb-") || t.len() < 20 || t.contains(char::is_whitespace) {
        return Err(
            "That doesn't look like a Slack Bot Token (expected xoxb-…). Copy it from OAuth & Permissions → Bot User OAuth Token."
                .into(),
        );
    }
    Ok(())
}

pub fn validate_app_token_format(token: &str) -> Result<(), String> {
    let t = token.trim();
    if t.starts_with("xoxb-") {
        return Err(
            "That's a Bot Token (xoxb-…). Now paste your App-Level Token (xapp-…) from Basic Information → App-Level Tokens (with connections:write scope)."
                .into(),
        );
    }
    if !t.starts_with("xapp-") || t.len() < 20 || t.contains(char::is_whitespace) {
        return Err(
            "That doesn't look like a Slack App-Level Token (expected xapp-…). Create one under Basic Information → App-Level Tokens with the connections:write scope."
                .into(),
        );
    }
    Ok(())
}

fn strip_leading_mention(text: &str) -> &str {
    let mut t = text.trim();
    if let Some(rest) = t.strip_prefix("<@") {
        if let Some(end) = rest.find('>') {
            let after = rest[end + 1..].trim_start();
            t = after
                .strip_prefix(':')
                .map(str::trim_start)
                .unwrap_or(after);
        }
    }
    t
}

pub fn is_pair_attempt(text: &str, code: &str) -> bool {
    if code.is_empty() {
        return false;
    }
    let t = strip_leading_mention(text);
    let t = t.strip_prefix("!pair").map(str::trim).unwrap_or(t);
    t == code
}

// ---------- runtime state ----------

#[derive(Debug, Clone)]
struct PendingSetup {
    bot_token: String,
    bot_user_id: String,
    bot_id: String,
    bot_username: String,
    team_name: String,
}

#[derive(Default, Clone)]
struct BridgeRt {
    connected: bool,
    last_error: String,
    pending_setup: Option<PendingSetup>,
    model_menu: Option<(String, Vec<String>)>,
    /// turn_id -> Slack message `ts`
    progress_ts: HashMap<String, String>,
    turns: HashMap<String, TurnProgress>,
}

#[derive(Clone)]
struct TurnProgress {
    started: Instant,
    current_tool: Option<String>,
    tool_started: Option<Instant>,
    tool_lines: Vec<String>,
    snapshot: String,
}

#[derive(Default)]
struct Runtime {
    store_gen: u64,
    bridges: HashMap<String, BridgeRt>,
}

fn rt() -> &'static Mutex<Runtime> {
    static R: OnceLock<Mutex<Runtime>> = OnceLock::new();
    R.get_or_init(Default::default)
}

fn bump_all() {
    if let Ok(mut r) = rt().lock() {
        r.store_gen += 1;
    }
}

fn set_error(sid: &str, e: impl Into<String>) {
    if let Ok(mut r) = rt().lock() {
        r.bridges.entry(sid.to_string()).or_default().last_error = e.into();
    }
}

fn set_connected(sid: &str, connected: bool, clear_err: bool) {
    if let Ok(mut r) = rt().lock() {
        let b = r.bridges.entry(sid.to_string()).or_default();
        b.connected = connected;
        if clear_err {
            b.last_error.clear();
        }
    }
}

fn set_app_connected(app_token: &str, connected: bool, err: Option<&str>) {
    let s = load_store();
    let sids: Vec<String> = s
        .bridges
        .iter()
        .filter(|(_, c)| c.app_token == app_token)
        .map(|(k, _)| k.clone())
        .collect();
    if let Ok(mut r) = rt().lock() {
        for sid in sids {
            let b = r.bridges.entry(sid).or_default();
            b.connected = connected;
            if let Some(e) = err {
                b.last_error = e.to_string();
            } else if connected {
                b.last_error.clear();
            }
        }
    }
}

// ---------- Host JSON-RPC client ----------

struct HostClient {
    tx: mpsc::UnboundedSender<String>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>,
    next_id: AtomicU64,
}

impl HostClient {
    fn new(tx: mpsc::UnboundedSender<String>) -> Self {
        Self {
            tx,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        if let Ok(mut p) = self.pending.lock() {
            p.insert(id, tx);
        }
        let line = json!({ "id": id, "method": method, "params": params }).to_string();
        if self.tx.send(line).is_err() {
            if let Ok(mut p) = self.pending.lock() {
                p.remove(&id);
            }
            return Err("host stdout closed".into());
        }
        match tokio::time::timeout(Duration::from_secs(60), rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err("host disconnected".into()),
            Err(_) => {
                if let Ok(mut p) = self.pending.lock() {
                    p.remove(&id);
                }
                Err(format!("host timed out on {method}"))
            }
        }
    }

    fn resolve(&self, id: u64, res: Result<Value, String>) {
        if let Some(w) = self.pending.lock().ok().and_then(|mut p| p.remove(&id)) {
            let _ = w.send(res);
        }
    }
}

// ---------- Slack Web API helpers ----------

fn api_base() -> String {
    std::env::var("VICUNA_SLACK_API").unwrap_or_else(|_| "https://slack.com/api".into())
}

fn http() -> &'static reqwest::Client {
    static C: OnceLock<reqwest::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::Client::builder()
            .user_agent("VicunaSlackBridge/0.1.0")
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default()
    })
}

async fn slack_api_post_at(
    base: &str,
    token: &str,
    method: &str,
    body: Option<Value>,
) -> Result<Value, String> {
    let url = format!("{}/{}", base.trim_end_matches('/'), method);
    let mut req = http().post(&url).bearer_auth(token);
    if let Some(b) = body {
        req = req.json(&b);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("slack {method}: {}", e.without_url()))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("slack {method}: HTTP {status}"));
    }
    let v: Value =
        serde_json::from_str(&text).map_err(|e| format!("slack {method}: invalid JSON: {e}"))?;
    if v.get("ok").and_then(Value::as_bool) != Some(true) {
        let err = v
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown_error");
        return Err(format!("slack {method}: {err}"));
    }
    Ok(v)
}

async fn slack_api_post(token: &str, method: &str, body: Option<Value>) -> Result<Value, String> {
    slack_api_post_at(&api_base(), token, method, body).await
}

async fn slack_api_get_at(
    base: &str,
    token: &str,
    method: &str,
    query: &[(&str, &str)],
) -> Result<Value, String> {
    let url = format!("{}/{}", base.trim_end_matches('/'), method);
    let resp = http()
        .get(&url)
        .bearer_auth(token)
        .query(query)
        .send()
        .await
        .map_err(|e| format!("slack {method}: {}", e.without_url()))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(format!("slack {method}: HTTP {status}"));
    }
    let v: Value =
        serde_json::from_str(&text).map_err(|e| format!("slack {method}: invalid JSON: {e}"))?;
    if v.get("ok").and_then(Value::as_bool) != Some(true) {
        let err = v
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown_error");
        return Err(format!("slack {method}: {err}"));
    }
    Ok(v)
}

async fn slack_api_get(token: &str, method: &str, query: &[(&str, &str)]) -> Result<Value, String> {
    slack_api_get_at(&api_base(), token, method, query).await
}

async fn upload_image_at(
    base: &str,
    bot_token: &str,
    channel_id: &str,
    bytes: Vec<u8>,
    mime: &str,
    filename: &str,
    caption: Option<&str>,
) -> Result<(), String> {
    let len_str = bytes.len().to_string();
    let step1 = slack_api_get_at(
        base,
        bot_token,
        "files.getUploadURLExternal",
        &[("filename", filename), ("length", len_str.as_str())],
    )
    .await?;
    let upload_url = step1
        .get("upload_url")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "slack files.getUploadURLExternal: missing upload_url".to_string())?;
    let file_id = step1
        .get("file_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "slack files.getUploadURLExternal: missing file_id".to_string())?;

    let upload_resp = http()
        .post(upload_url)
        .header("Content-Type", mime)
        .body(bytes)
        .send()
        .await
        .map_err(|e| format!("slack upload_url POST: {}", e.without_url()))?;
    let upload_status = upload_resp.status();
    if !upload_status.is_success() {
        return Err(format!("slack upload_url POST: HTTP {upload_status}"));
    }

    let mut complete_body = json!({
        "files": [{ "id": file_id, "title": filename }],
        "channel_id": channel_id,
    });
    if let Some(c) = caption.map(str::trim).filter(|s| !s.is_empty()) {
        complete_body["initial_comment"] = Value::String(c.to_string());
    }
    slack_api_post_at(
        base,
        bot_token,
        "files.completeUploadExternal",
        Some(complete_body),
    )
    .await?;
    Ok(())
}

async fn upload_image(
    bot_token: &str,
    channel_id: &str,
    bytes: Vec<u8>,
    mime: &str,
    filename: &str,
    caption: Option<&str>,
) -> Result<(), String> {
    upload_image_at(
        &api_base(),
        bot_token,
        channel_id,
        bytes,
        mime,
        filename,
        caption,
    )
    .await
}

async fn post_message(
    bot_token: &str,
    channel: &str,
    text: &str,
    blocks: Option<Value>,
) -> Option<String> {
    let mut body = json!({
        "channel": channel,
        "text": text,
    });
    if let Some(b) = blocks {
        body["blocks"] = b;
    }
    slack_api_post(bot_token, "chat.postMessage", Some(body))
        .await
        .ok()?
        .get("ts")
        .and_then(Value::as_str)
        .map(str::to_string)
}

async fn update_message(
    bot_token: &str,
    channel: &str,
    ts: &str,
    text: &str,
    blocks: Option<Value>,
) {
    let mut body = json!({
        "channel": channel,
        "ts": ts,
        "text": text,
    });
    if let Some(b) = blocks {
        body["blocks"] = b;
    }
    if let Err(e) = slack_api_post(bot_token, "chat.update", Some(body)).await {
        eprintln!("vicuna-plugin-slack: {e}");
    }
}

/// One async lock per progress message. Progress edits (12 s ticker, tool and
/// progress notifications) and the final reply all edit the same Slack
/// message; without this an edit built from stale state could land after the
/// final `chat.update` and overwrite the reply.
type MsgLocks = Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>;

fn msg_locks() -> std::sync::MutexGuard<'static, HashMap<String, Arc<tokio::sync::Mutex<()>>>> {
    static M: OnceLock<MsgLocks> = OnceLock::new();
    M.get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn msg_lock(ts: &str) -> Arc<tokio::sync::Mutex<()>> {
    msg_locks().entry(ts.to_string()).or_default().clone()
}

/// Forget a message's lock once its final edit landed (the map only grows otherwise).
fn drop_msg_lock(ts: &str) {
    msg_locks().remove(ts);
}

/// Edit the progress message from the turn's CURRENT state, under the
/// message lock; a turn that already finished owns nothing to edit.
async fn update_progress(
    bot_token: &str,
    channel: &str,
    ts: &str,
    sid: &str,
    turn_id: &str,
) -> bool {
    let lock = msg_lock(ts);
    let _g = lock.lock().await;
    let content = rt().lock().ok().and_then(|r| {
        let state = r.bridges.get(sid)?.turns.get(turn_id)?;
        Some(progress_content(state, Instant::now()))
    });
    let Some(content) = content else { return false };
    update_message(bot_token, channel, ts, &content, None).await;
    true
}

async fn lookup_user_name(bot_token: &str, user_id: &str) -> String {
    if let Ok(v) = slack_api_get(bot_token, "users.info", &[("user", user_id)]).await {
        for ptr in [
            "/user/profile/display_name",
            "/user/profile/real_name",
            "/user/real_name",
            "/user/name",
        ] {
            if let Some(s) = v
                .pointer(ptr)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                return s.to_string();
            }
        }
    }
    user_id.to_string()
}

// ---------- Formatting, Chunking & Block Kit helpers ----------

/// Split at line breaks into Slack-sized pieces (≤ 3800 chars).
pub fn chunks(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in text.split_inclusive('\n') {
        if cur.len() + line.len() > MAX_MSG && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        if line.len() > MAX_MSG {
            let mut s = line;
            while s.len() > MAX_MSG {
                let mut cut = MAX_MSG;
                while !s.is_char_boundary(cut) {
                    cut -= 1;
                }
                out.push(s[..cut].to_string());
                s = &s[cut..];
            }
            cur.push_str(s);
        } else {
            cur.push_str(line);
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

fn elapsed_text(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

fn status_line(state: &TurnProgress, now: Instant) -> String {
    let mut s = format!(
        "⏳ Working… {}",
        elapsed_text(now.duration_since(state.started))
    );
    if let (Some(tool), Some(at)) = (&state.current_tool, state.tool_started) {
        s.push_str(&format!(
            " · {} running {}",
            tool,
            elapsed_text(now.duration_since(at))
        ));
    }
    s
}

fn compact_tool_line(raw: &str) -> String {
    let line = raw.lines().next().unwrap_or("").trim();
    let line = line.strip_prefix("▸ ").unwrap_or(line);
    let safe = line.replace('`', "ˋ").replace('\n', " ");
    let clipped: String = safe.chars().take(132).collect();
    let clipped = if safe.chars().count() > 132 {
        format!("{clipped}…")
    } else {
        clipped
    };
    format!("▸ `{clipped}`")
}

fn compact_snapshot(text: &str) -> String {
    text.lines()
        .map(|l| {
            let trimmed = l.trim();
            if trimmed.starts_with("▸ ") {
                compact_tool_line(trimmed)
            } else {
                let clipped: String = trimmed.chars().take(300).collect();
                if trimmed.chars().count() > 300 {
                    format!("{clipped}…")
                } else {
                    clipped
                }
            }
        })
        .filter(|l| !l.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn progress_content(state: &TurnProgress, now: Instant) -> String {
    let mut lines = state.tool_lines.clone();
    if !state.snapshot.is_empty() {
        let snap = compact_snapshot(&state.snapshot);
        if !snap.is_empty() {
            lines.push(snap);
        }
    }
    let status = status_line(state, now);
    let mut kept = vec![status.clone()];
    let mut size = status.chars().count();
    for line in lines.into_iter().rev() {
        let n = line.chars().count() + 1;
        if size + n > MAX_MSG {
            break;
        }
        kept.push(line);
        size += n;
    }
    kept.reverse();
    kept.join("\n")
}

/// Build Slack Block Kit blocks: a `section` with text followed by `actions`
/// blocks (up to 5 buttons per block, 25 buttons total).
pub fn build_button_blocks(text: &str, items: &[(String, String)]) -> Option<Value> {
    if items.is_empty() {
        return None;
    }
    let section_text: String = if text.chars().count() > MAX_SECTION_TEXT {
        let clipped: String = text.chars().take(MAX_SECTION_TEXT - 1).collect();
        format!("{clipped}…")
    } else {
        text.to_string()
    };
    let mut blocks = vec![json!({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": section_text
        }
    })];
    for chunk in items.iter().take(25).collect::<Vec<_>>().chunks(5) {
        let elements: Vec<Value> = chunk
            .iter()
            .map(|(label, action_id)| {
                let btn_label: String = label.chars().take(75).collect();
                json!({
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": if btn_label.is_empty() { "Select".to_string() } else { btn_label },
                        "emoji": true
                    },
                    "action_id": action_id,
                    "value": action_id,
                })
            })
            .collect();
        blocks.push(json!({
            "type": "actions",
            "elements": elements
        }));
    }
    Some(Value::Array(blocks))
}

pub fn build_ask_payload(
    question: &str,
    options: &[String],
    freeform: bool,
) -> (String, Option<Value>) {
    let hint = if freeform || options.is_empty() {
        if options.is_empty() {
            "\n\n(Reply with text.)"
        } else {
            "\n\n(Click a choice or reply with text.)"
        }
    } else {
        ""
    };
    let text = format!("❓ {question}{hint}");
    let items: Vec<(String, String)> = options
        .iter()
        .enumerate()
        .map(|(i, o)| (o.clone(), format!("a:{i}")))
        .collect();
    let blocks = build_button_blocks(&text, &items);
    (text, blocks)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedBlockAction {
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: Option<String>,
    pub orig_text: String,
    pub action_id: String,
}

pub fn parse_block_action(payload: &Value) -> Option<ParsedBlockAction> {
    if payload.get("type").and_then(Value::as_str) != Some("block_actions") {
        return None;
    }
    let action_id = payload
        .pointer("/actions/0/action_id")
        .and_then(Value::as_str)?
        .to_string();
    let channel_id = payload
        .pointer("/channel/id")
        .or_else(|| payload.pointer("/container/channel_id"))
        .and_then(Value::as_str)?
        .to_string();
    let user_id = payload
        .pointer("/user/id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let message_ts = payload
        .pointer("/message/ts")
        .or_else(|| payload.pointer("/container/message_ts"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let orig_text = payload
        .pointer("/message/text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    Some(ParsedBlockAction {
        user_id,
        channel_id,
        message_ts,
        orig_text,
        action_id,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    Status,
    Stop,
    SendNow,
    Unpair,
    ModelOverview,
    ModelEngine(String),
    ModelSet(String, String),
}

pub fn parse_command(text: &str) -> Option<Command> {
    let t = strip_leading_mention(text);
    if t == "!help" {
        return Some(Command::Help);
    }
    if t == "!status" {
        return Some(Command::Status);
    }
    if t == "!stop" {
        return Some(Command::Stop);
    }
    if t == "!now" || t == "!send_now" {
        return Some(Command::SendNow);
    }
    if t == "!unpair" {
        return Some(Command::Unpair);
    }
    if t == "!model" || t.starts_with("!model ") {
        let rest: Vec<&str> = t.split_whitespace().skip(1).collect();
        return match rest.as_slice() {
            [] => Some(Command::ModelOverview),
            [e] => Some(Command::ModelEngine((*e).to_string())),
            [e, m, ..] => Some(Command::ModelSet((*e).to_string(), (*m).to_string())),
        };
    }
    None
}

// ---------- Multi-session routing & message filtering ----------

/// Return true if a Slack `message` event should be ignored (bot messages or
/// non-file_share subtypes like `message_changed` / `message_deleted` / `bot_message`).
pub fn should_ignore_message_event(event: &Value, known_bot_user_ids: &HashSet<String>) -> bool {
    if event.get("type").and_then(Value::as_str) != Some("message") {
        return true;
    }
    if event.get("bot_id").is_some_and(|v| !v.is_null()) {
        return true;
    }
    if let Some(sub) = event.get("subtype").and_then(Value::as_str) {
        if sub != "file_share" {
            return true;
        }
    }
    if let Some(uid) = event.get("user").and_then(Value::as_str) {
        if known_bot_user_ids.contains(uid) {
            return true;
        }
    } else {
        return true;
    }
    false
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteTarget {
    /// Channel is already paired to this session.
    Paired(String),
    /// Channel is unpaired and message matches this session's pair code.
    PairMatch(String),
    /// Channel is unpaired, no pair code matched, but there is at least one
    /// unpaired session on this `app_token` (carries its `bot_token` for DM hint).
    UnpairedHint { bot_token: String },
    /// No enabled session on this `app_token` matches.
    None,
}

pub fn route_incoming_message(
    store: &Store,
    app_token: &str,
    channel_id: &str,
    text: &str,
) -> RouteTarget {
    let mut matching: Vec<(&String, &BridgeConfig)> = store
        .bridges
        .iter()
        .filter(|(_, c)| c.enabled && c.is_configured() && c.app_token == app_token)
        .collect();
    matching.sort_by(|a, b| a.0.cmp(b.0));

    // 1. Exact paired channel match takes priority.
    for (sid, cfg) in &matching {
        if cfg.channel_id.as_deref() == Some(channel_id) {
            return RouteTarget::Paired((*sid).clone());
        }
    }
    // 2. Pair code match against an unpaired session.
    for (sid, cfg) in &matching {
        if cfg.channel_id.is_none() && is_pair_attempt(text, &cfg.pair_code) {
            return RouteTarget::PairMatch((*sid).clone());
        }
    }
    // 3. Fallback hint if any unpaired session exists on this app.
    if let Some((_, cfg)) = matching.iter().find(|(_, c)| c.channel_id.is_none()) {
        return RouteTarget::UnpairedHint {
            bot_token: cfg.bot_token.clone(),
        };
    }
    RouteTarget::None
}

pub fn route_interactive(store: &Store, app_token: &str, channel_id: &str) -> Option<String> {
    let mut matching: Vec<(&String, &BridgeConfig)> = store
        .bridges
        .iter()
        .filter(|(_, c)| c.enabled && c.is_configured() && c.app_token == app_token)
        .collect();
    matching.sort_by(|a, b| a.0.cmp(b.0));
    matching
        .into_iter()
        .find(|(_, c)| c.channel_id.as_deref() == Some(channel_id))
        .map(|(sid, _)| sid.clone())
}

// ---------- Bridge API handlers (called by Vicuna host) ----------

fn pending_input_prompt(p: &PendingSetup) -> Value {
    let note = if p.team_name.is_empty() {
        format!("Bot token verified (@{}).", p.bot_username)
    } else {
        format!(
            "Bot token verified (@{} in {}).",
            p.bot_username, p.team_name
        )
    };
    json!({
        "note": note,
        "label": "Now paste your App-Level Token (Basic Information → App-Level Tokens, with connections:write scope).",
        "placeholder": "xapp-1-…",
        "secret": true,
        "submitLabel": "Connect"
    })
}

fn pairing_hint_for(bot_username: &str) -> String {
    let who = if bot_username.is_empty() {
        "your Slack bot".to_string()
    } else {
        format!("@{bot_username}")
    };
    format!("In Slack, open a DM with {who} (or invite it to a private channel) and send:")
}

fn status_from_cfg(sid: &str, c: Option<&BridgeConfig>) -> Value {
    let empty = BridgeConfig::default();
    let c = c.unwrap_or(&empty);
    let (err, connected, pending) = rt()
        .lock()
        .ok()
        .and_then(|r| {
            r.bridges
                .get(sid)
                .map(|b| (b.last_error.clone(), b.connected, b.pending_setup.clone()))
        })
        .unwrap_or_default();

    if let Some(p) = pending {
        return json!({
            "sessionId": sid,
            "configured": false,
            "enabled": false,
            "connected": false,
            "botUsername": p.bot_username,
            "tokenTail": token_tail(&p.bot_token),
            "inputPrompt": pending_input_prompt(&p),
            "lastError": err,
        });
    }

    let configured = c.is_configured();
    let is_paired = c.channel_id.is_some();
    json!({
        "sessionId": sid,
        "configured": configured,
        "enabled": c.enabled,
        "connected": connected && c.enabled && configured,
        "botUsername": c.bot_username,
        "tokenTail": if c.bot_token.is_empty() { String::new() } else { token_tail(&c.bot_token) },
        "paired": c.channel_id.as_ref().map(|ch| json!({
            "id": ch,
            "name": if c.user_name.is_empty() { ch.clone() } else { c.user_name.clone() }
        })),
        "pairCode": if configured && !is_paired { c.pair_code.clone() } else { String::new() },
        "pairingHint": if configured && !is_paired { pairing_hint_for(&c.bot_username) } else { String::new() },
        "lastError": err,
    })
}

fn status_for_path(path: &Path, sid: &str) -> Value {
    let s = load_store_from(path);
    status_from_cfg(sid, s.bridges.get(sid))
}

fn status_all_path(path: &Path) -> Value {
    let s = load_store_from(path);
    let mut map = serde_json::Map::new();
    for (sid, cfg) in &s.bridges {
        if !cfg.is_configured() {
            continue;
        }
        map.insert(sid.clone(), status_from_cfg(sid, Some(cfg)));
    }
    json!({ "bridges": map })
}

fn drop_for_path(path: &Path, sid: &str) {
    let mut s = load_store_from(path);
    if s.bridges.remove(sid).is_some() {
        let _ = save_store_to(path, &s);
    }
    if let Ok(mut r) = rt().lock() {
        r.store_gen += 1;
        r.bridges.remove(sid);
    }
}

async fn handle_bridge_rpc_at(
    api_url: &str,
    cfg_path: &Path,
    method: &str,
    params: &Value,
) -> Result<Value, String> {
    let sid = || {
        params
            .get("sessionId")
            .or_else(|| params.get("session_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .ok_or_else(|| "missing sessionId".to_string())
    };

    match method {
        "bridge_list" => Ok(status_all_path(cfg_path)),
        "bridge_status" => Ok(status_for_path(cfg_path, &sid()?)),
        "bridge_save" => {
            let sid = sid()?;
            let token = params
                .get("token")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_string();

            let pending = rt()
                .lock()
                .ok()
                .and_then(|r| r.bridges.get(&sid).and_then(|b| b.pending_setup.clone()));

            if let Some(p) = pending {
                // Step 2: validate xapp-... App-Level Token
                validate_app_token_format(&token)?;
                let open = slack_api_post_at(api_url, &token, "apps.connections.open", None)
                    .await
                    .map_err(|e| {
                        format!(
                            "Slack rejected the App-Level Token ({e}). Make sure Socket Mode is enabled and the token has the connections:write scope."
                        )
                    })?;
                let url = open.get("url").and_then(Value::as_str).unwrap_or("");
                if url.is_empty() {
                    return Err("Slack apps.connections.open returned no WebSocket URL.".into());
                }

                let mut s = load_store_from(cfg_path);
                let c = s.bridges.entry(sid.clone()).or_default();
                if c.bot_token != p.bot_token {
                    c.user_id = None;
                    c.channel_id = None;
                    c.user_name.clear();
                }
                c.bot_token = p.bot_token;
                c.app_token = token;
                c.enabled = true;
                c.bot_user_id = p.bot_user_id;
                c.bot_id = p.bot_id;
                c.bot_username = p.bot_username;
                c.team_name = p.team_name;
                if c.channel_id.is_none() {
                    c.pair_code = new_pair_code();
                }
                save_store_to(cfg_path, &s)?;

                if let Ok(mut r) = rt().lock() {
                    r.store_gen += 1;
                    let b = r.bridges.entry(sid.clone()).or_default();
                    b.pending_setup = None;
                    b.connected = true;
                    b.last_error.clear();
                }
                Ok(status_for_path(cfg_path, &sid))
            } else {
                // Step 1: validate xoxb-... Bot User OAuth Token
                validate_bot_token_format(&token)?;
                let auth = slack_api_post_at(api_url, &token, "auth.test", None)
                    .await
                    .map_err(|e| {
                        format!(
                            "Slack rejected the Bot Token ({e}). Check OAuth & Permissions → Bot User OAuth Token."
                        )
                    })?;
                let bot_user_id = auth
                    .get("user_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let bot_id = auth
                    .get("bot_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let bot_username = auth
                    .get("user")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("slack-bot")
                    .to_string();
                let team_name = auth
                    .get("team")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if bot_user_id.is_empty() {
                    return Err("Slack auth.test returned no user_id.".into());
                }

                if let Ok(mut r) = rt().lock() {
                    let b = r.bridges.entry(sid.clone()).or_default();
                    b.pending_setup = Some(PendingSetup {
                        bot_token: token,
                        bot_user_id,
                        bot_id,
                        bot_username,
                        team_name,
                    });
                    b.last_error.clear();
                }
                Ok(status_for_path(cfg_path, &sid))
            }
        }
        "bridge_set" => {
            let sid = sid()?;
            let mut s = load_store_from(cfg_path);
            let c = s
                .bridges
                .get_mut(&sid)
                .ok_or("no Slack bot configured for this session")?;
            if let Some(e) = params.get("enabled").and_then(Value::as_bool) {
                c.enabled = e;
            }
            save_store_to(cfg_path, &s)?;
            bump_all();
            Ok(status_for_path(cfg_path, &sid))
        }
        "bridge_unpair" => {
            let sid = sid()?;
            let mut s = load_store_from(cfg_path);
            let c = s
                .bridges
                .get_mut(&sid)
                .ok_or("no Slack bot configured for this session")?;
            c.user_id = None;
            c.channel_id = None;
            c.user_name.clear();
            c.pair_code = new_pair_code();
            save_store_to(cfg_path, &s)?;
            bump_all();
            Ok(status_for_path(cfg_path, &sid))
        }
        "bridge_remove" => {
            let sid = sid()?;
            drop_for_path(cfg_path, &sid);
            Ok(status_for_path(cfg_path, &sid))
        }
        _ => Err(format!("unknown bridge method {method}")),
    }
}

async fn handle_bridge_rpc(method: &str, params: &Value) -> Result<Value, String> {
    handle_bridge_rpc_at(&api_base(), &config_path(), method, params).await
}

// ---------- Turn event mirroring (host -> Slack) ----------

async fn handle_turn_event(params: &Value) {
    let Some(sid) = params.get("sessionId").and_then(Value::as_str) else {
        return;
    };
    let Some(event) = params.get("event").and_then(Value::as_str) else {
        return;
    };
    let turn_id = params
        .get("turnId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let Some(cfg) = load_store().bridges.remove(sid) else {
        return;
    };
    if !cfg.is_configured() || !cfg.enabled {
        return;
    }
    let Some(channel_id) = cfg.channel_id else {
        return;
    };

    match event {
        "started" => {
            if let Some(ts) = post_message(&cfg.bot_token, &channel_id, "⏳ Working…", None).await
            {
                if let Ok(mut r) = rt().lock() {
                    let b = r.bridges.entry(sid.to_string()).or_default();
                    b.progress_ts.insert(turn_id.clone(), ts.clone());
                    b.turns.insert(
                        turn_id.clone(),
                        TurnProgress {
                            started: Instant::now(),
                            current_tool: None,
                            tool_started: None,
                            tool_lines: Vec::new(),
                            snapshot: String::new(),
                        },
                    );
                }
                let token = cfg.bot_token.clone();
                let channel_id = channel_id.clone();
                let sid = sid.to_string();
                tokio::spawn(async move {
                    let mut tick = tokio::time::interval(Duration::from_secs(12));
                    tick.tick().await; // skip immediate tick
                    loop {
                        tick.tick().await;
                        if !update_progress(&token, &channel_id, &ts, &sid, &turn_id).await {
                            break;
                        }
                    }
                });
            }
        }
        "progress" => {
            let text = params.get("text").and_then(Value::as_str).unwrap_or("");
            if text.is_empty() {
                return;
            }
            let ts = rt()
                .lock()
                .ok()
                .and_then(|mut r| {
                    let b = r.bridges.get_mut(sid)?;
                    let ts = b.progress_ts.get(&turn_id)?.clone();
                    let state = b.turns.get_mut(&turn_id)?;
                    state.snapshot = text.to_string();
                    let observed = text.lines().rev().find_map(|line| {
                        let line = line.trim().strip_prefix("▸ ")?;
                        let name = line.split(':').next().unwrap_or(line).trim();
                        (!name.is_empty()).then(|| name.to_string())
                    });
                    if let Some(name) = observed {
                        if state.current_tool.as_deref() != Some(name.as_str()) {
                            state.current_tool = Some(name);
                            state.tool_started = Some(Instant::now());
                        }
                    }
                    Some(ts)
                })
                .unwrap_or_default();
            if !ts.is_empty() {
                update_progress(&cfg.bot_token, &channel_id, &ts, sid, &turn_id).await;
            }
        }
        "toolstart" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("tool");
            let args = params.get("args").unwrap_or(&Value::Null);
            let detail = args
                .get("command")
                .or_else(|| args.get("description"))
                .or_else(|| args.get("query"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let label = if detail.is_empty() {
                name.to_string()
            } else {
                format!("{name}: {detail}")
            };
            let ts = rt()
                .lock()
                .ok()
                .and_then(|mut r| {
                    let b = r.bridges.get_mut(sid)?;
                    let ts = b.progress_ts.get(&turn_id)?.clone();
                    let state = b.turns.get_mut(&turn_id)?;
                    state.current_tool = Some(name.to_string());
                    state.tool_started = Some(Instant::now());
                    state.tool_lines.push(compact_tool_line(&label));
                    Some(ts)
                })
                .unwrap_or_default();
            if !ts.is_empty() {
                update_progress(&cfg.bot_token, &channel_id, &ts, sid, &turn_id).await;
            }
        }
        "toolend" => {
            let ts = rt()
                .lock()
                .ok()
                .and_then(|mut r| {
                    let b = r.bridges.get_mut(sid)?;
                    let ts = b.progress_ts.get(&turn_id)?.clone();
                    let state = b.turns.get_mut(&turn_id)?;
                    state.current_tool = None;
                    state.tool_started = None;
                    Some(ts)
                })
                .unwrap_or_default();
            if !ts.is_empty() {
                update_progress(&cfg.bot_token, &channel_id, &ts, sid, &turn_id).await;
            }
        }
        "error" => {
            let message = params
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Turn failed");
            finish_progress(sid, &turn_id);
            let marker = format!(
                "❌ Error: {}",
                message.chars().take(300).collect::<String>()
            );
            if let Some(ts) = rt().lock().ok().and_then(|r| {
                r.bridges
                    .get(sid)
                    .and_then(|b| b.progress_ts.get(&turn_id).cloned())
            }) {
                let lock = msg_lock(&ts);
                let _g = lock.lock().await;
                update_message(&cfg.bot_token, &channel_id, &ts, &marker, None).await;
                drop(_g);
                drop_msg_lock(&ts);
            }
        }
        "ask" => {
            let q = params
                .get("question")
                .and_then(Value::as_str)
                .unwrap_or("This session has a question");
            let opts: Vec<String> = params
                .get("options")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let freeform = params
                .get("freeform")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let (text, blocks) = build_ask_payload(q, &opts, freeform);
            post_message(&cfg.bot_token, &channel_id, &text, blocks).await;
        }
        "image" => {
            let Some(data) = params.get("data").and_then(Value::as_str) else {
                eprintln!("vicuna-plugin-slack: image event missing data for session {sid}");
                return;
            };
            let bytes = match base64::engine::general_purpose::STANDARD.decode(data) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!(
                        "vicuna-plugin-slack: failed to decode base64 image for session {sid}: {e}"
                    );
                    return;
                }
            };
            let mime = params
                .get("mime")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or("image/png");
            let filename = params
                .get("filename")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or("image.png");
            let caption = params
                .get("caption")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty());
            if let Err(e) =
                upload_image(&cfg.bot_token, &channel_id, bytes, mime, filename, caption).await
            {
                eprintln!(
                    "vicuna-plugin-slack: failed to upload image {filename} to {channel_id}: {e}"
                );
            }
        }
        "finished" => {
            let reply = params
                .get("reply")
                .and_then(Value::as_str)
                .unwrap_or("(no reply text)");
            let ts = rt().lock().ok().and_then(|mut r| {
                let b = r.bridges.get_mut(sid)?;
                b.turns.remove(&turn_id);
                b.progress_ts.remove(&turn_id)
            });
            let stopped = params.get("stopped").and_then(Value::as_bool) == Some(true);
            let terminal = if stopped { "⏹ Stopped" } else { "✅ Done" };
            let reply = format!("{reply}\n\n{terminal}");
            let parts = chunks(&reply);
            let mut it = parts.iter();
            if let (Some(t), Some(first)) = (ts, it.next()) {
                // the turn is already removed above, so any progress edit
                // waiting on this lock sees no state and skips
                let lock = msg_lock(&t);
                let _g = lock.lock().await;
                update_message(&cfg.bot_token, &channel_id, &t, first, None).await;
                drop(_g);
                drop_msg_lock(&t);
            } else if let Some(first) = parts.first() {
                post_message(&cfg.bot_token, &channel_id, first, None).await;
            }
            for p in it {
                post_message(&cfg.bot_token, &channel_id, p, None).await;
            }
        }
        _ => {}
    }
}

fn finish_progress(sid: &str, turn_id: &str) {
    if let Ok(mut r) = rt().lock() {
        if let Some(b) = r.bridges.get_mut(sid) {
            b.turns.remove(turn_id);
        }
    }
}

// ---------- Socket Mode supervisor & multiplexed connection loop ----------

fn spawn_socket_supervisor(host: Arc<HostClient>) {
    tokio::spawn(async move {
        // Keyed by unique `app_token` so sessions sharing the same Slack app
        // multiplex over a single Socket Mode WebSocket connection.
        let mut active: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
        loop {
            let s = load_store();
            let store_gen = rt().lock().map(|r| r.store_gen).unwrap_or(0);

            let desired_apps: HashSet<String> = s
                .bridges
                .values()
                .filter(|c| c.enabled && c.is_configured())
                .map(|c| c.app_token.clone())
                .collect();

            let to_stop: Vec<String> = active
                .keys()
                .filter(|tok| !desired_apps.contains(*tok))
                .cloned()
                .collect();
            for tok in to_stop {
                if let Some(h) = active.remove(&tok) {
                    h.abort();
                }
                set_app_connected(&tok, false, None);
            }

            // Mark disabled sessions disconnected
            for (sid, cfg) in &s.bridges {
                if !cfg.enabled || !cfg.is_configured() {
                    set_connected(sid, false, false);
                }
            }

            for tok in desired_apps {
                let needs_spawn = match active.get(&tok) {
                    Some(h) => h.is_finished(),
                    None => true,
                };
                if needs_spawn {
                    let h = tokio::spawn(run_socket_loop(host.clone(), tok.clone()));
                    active.insert(tok, h);
                }
            }

            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if rt().lock().map(|r| r.store_gen).unwrap_or(0) != store_gen {
                    break;
                }
            }
        }
    });
}

fn app_still_active(app_token: &str) -> bool {
    load_store()
        .bridges
        .values()
        .any(|c| c.enabled && c.is_configured() && c.app_token == app_token)
}

async fn run_socket_loop(host: Arc<HostClient>, app_token: String) {
    let mut backoff_secs: u64 = 1;

    loop {
        if !app_still_active(&app_token) {
            set_app_connected(&app_token, false, None);
            return;
        }

        let ws_url = match slack_api_post(&app_token, "apps.connections.open", None).await {
            Ok(v) => match v.get("url").and_then(Value::as_str) {
                Some(u) if !u.is_empty() => u.to_string(),
                _ => {
                    set_app_connected(
                        &app_token,
                        false,
                        Some("apps.connections.open returned no WebSocket URL"),
                    );
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(30);
                    continue;
                }
            },
            Err(e) => {
                set_app_connected(&app_token, false, Some(&e));
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(30);
                continue;
            }
        };

        let (ws_stream, _) = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok(c) => c,
            Err(e) => {
                set_app_connected(
                    &app_token,
                    false,
                    Some(&format!("Socket Mode connect failed: {e}")),
                );
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(30);
                continue;
            }
        };

        let (mut ws_tx, mut ws_rx) = ws_stream.split();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<WsMsg>();

        let writer = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                if ws_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        let mut clean_disconnect = false;

        loop {
            if !app_still_active(&app_token) {
                clean_disconnect = true;
                break;
            }

            let next = tokio::time::timeout(Duration::from_secs(2), ws_rx.next()).await;
            let frame = match next {
                Err(_) => continue,
                Ok(Some(Ok(m))) => m,
                Ok(Some(Err(e))) => {
                    set_app_connected(&app_token, false, Some(&format!("Socket Mode error: {e}")));
                    break;
                }
                Ok(None) => break,
            };

            match frame {
                WsMsg::Ping(data) => {
                    let _ = out_tx.send(WsMsg::Pong(data));
                }
                WsMsg::Pong(_) => {}
                WsMsg::Close(_) => {
                    break;
                }
                WsMsg::Text(text) => {
                    let Ok(env) = serde_json::from_str::<Value>(&text) else {
                        continue;
                    };
                    // Immediately ACK every envelope with an envelope_id before dispatching.
                    if let Some(eid) = env.get("envelope_id").and_then(Value::as_str) {
                        let ack = json!({ "envelope_id": eid }).to_string();
                        let _ = out_tx.send(WsMsg::Text(ack.into()));
                    }
                    let env_type = env.get("type").and_then(Value::as_str).unwrap_or("");
                    match env_type {
                        "hello" => {
                            backoff_secs = 1;
                            set_app_connected(&app_token, true, None);
                        }
                        "disconnect" => {
                            // Slack requests a clean reconnect (e.g. refresh_requested).
                            clean_disconnect = true;
                            backoff_secs = 1;
                            break;
                        }
                        "events_api" => {
                            let payload = env.get("payload").cloned().unwrap_or(Value::Null);
                            let h = host.clone();
                            let tok = app_token.clone();
                            tokio::spawn(async move {
                                handle_events_api(&h, &tok, &payload).await;
                            });
                        }
                        "interactive" => {
                            let payload = env.get("payload").cloned().unwrap_or(Value::Null);
                            let h = host.clone();
                            let tok = app_token.clone();
                            tokio::spawn(async move {
                                handle_interactive(&h, &tok, &payload).await;
                            });
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }

        writer.abort();
        if !app_still_active(&app_token) {
            set_app_connected(&app_token, false, None);
            return;
        }
        if clean_disconnect {
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            set_app_connected(&app_token, false, None);
            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = (backoff_secs * 2).min(30);
        }
    }
}

// ---------- Inbound Slack event & interactive handling ----------

async fn handle_events_api(host: &Arc<HostClient>, app_token: &str, payload: &Value) {
    let Some(event) = payload.get("event") else {
        return;
    };
    let store = load_store();
    let bot_uids: HashSet<String> = store
        .bridges
        .values()
        .filter(|c| c.app_token == app_token && !c.bot_user_id.is_empty())
        .map(|c| c.bot_user_id.clone())
        .collect();
    if should_ignore_message_event(event, &bot_uids) {
        return;
    }

    let channel_id = event
        .get("channel")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if channel_id.is_empty() {
        return;
    }
    let from_user = event
        .get("user")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let text = event
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let channel_type = event
        .get("channel_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    let is_dm = channel_type == "im" || channel_id.starts_with('D');

    match route_incoming_message(&store, app_token, &channel_id, &text) {
        RouteTarget::PairMatch(sid) => {
            if let Err(e) = complete_pairing(host, &sid, &channel_id, &from_user).await {
                set_error(&sid, e);
            }
        }
        RouteTarget::Paired(sid) => {
            if let Err(e) = handle_paired_message(host, &sid, &channel_id, &from_user, event).await
            {
                set_error(&sid, e);
            }
        }
        RouteTarget::UnpairedHint { bot_token } => {
            if is_dm || text.trim().starts_with("!pair") {
                post_message(
                    &bot_token,
                    &channel_id,
                    "This bot is private. Open Vicuna → right-click a session → Slack, and send the `!pair <code>` shown there.",
                    None,
                )
                .await;
            }
        }
        RouteTarget::None => {}
    }
}

async fn complete_pairing(
    host: &Arc<HostClient>,
    sid: &str,
    channel_id: &str,
    from_user: &str,
) -> Result<(), String> {
    let mut s = load_store();
    let Some(c) = s.bridges.get_mut(sid) else {
        return Ok(());
    };
    let bot_token = c.bot_token.clone();
    let user_name = lookup_user_name(&bot_token, from_user).await;
    c.user_id = Some(from_user.to_string());
    c.channel_id = Some(channel_id.to_string());
    c.user_name = user_name;
    c.pair_code.clear();
    save_store(&s)?;

    let info = host
        .call("session_info", json!({ "sessionId": sid }))
        .await
        .unwrap_or(Value::Null);
    let title = info
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("this session");
    post_message(
        &bot_token,
        channel_id,
        &format!(
            "✅ Paired with Vicuna. Messages you send here go to \"{title}\".\n\n`!status` · `!model` · `!now` · `!stop` · `!unpair` · `!help`"
        ),
        None,
    )
    .await;
    Ok(())
}

async fn handle_paired_message(
    host: &Arc<HostClient>,
    sid: &str,
    channel_id: &str,
    from_user: &str,
    event: &Value,
) -> Result<(), String> {
    let s = load_store();
    let Some(c) = s.bridges.get(sid) else {
        return Ok(());
    };
    if c.user_id.as_deref() != Some(from_user) {
        return Ok(());
    }
    let bot_token = c.bot_token.clone();
    let text = event
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let t = text.trim();

    let info = host
        .call("session_info", json!({ "sessionId": sid }))
        .await?;
    if info.get("exists").and_then(Value::as_bool) != Some(true) {
        post_message(
            &bot_token,
            channel_id,
            "⚠️ The session bound to this Slack bridge no longer exists in Vicuna.",
            None,
        )
        .await;
        return Ok(());
    }
    let title = info
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("This session")
        .to_string();

    if let Some(cmd) = parse_command(t) {
        match cmd {
            Command::Help => {
                post_message(
                    &bot_token,
                    channel_id,
                    &format!(
                        "Send any message to talk to \"{title}\". Images and file attachments work too.\n`!status`: what this session is doing\n`!model`: switch harness and model (`!model claude haiku`)\n`!now` (or `!send_now`): interrupt the running turn and send queued messages now\n`!stop`: stop the running turn\n`!unpair`: disconnect this channel from the session"
                    ),
                    None,
                )
                .await;
                return Ok(());
            }
            Command::Status => {
                let state = if info.get("working").and_then(Value::as_bool) == Some(true) {
                    "working"
                } else {
                    "idle"
                };
                let engine = info.get("engine").and_then(Value::as_str).unwrap_or("?");
                let model = info
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or("default");
                post_message(
                    &bot_token,
                    channel_id,
                    &format!("\"{title}\" is {state} · {engine}/{model}"),
                    None,
                )
                .await;
                return Ok(());
            }
            Command::Stop => {
                let r = host.call("stop_turn", json!({ "sessionId": sid })).await?;
                if r.get("stopped").and_then(Value::as_bool) == Some(true) {
                    post_message(
                        &bot_token,
                        channel_id,
                        &format!("⏹ Stopping \"{title}\"…"),
                        None,
                    )
                    .await;
                } else {
                    post_message(
                        &bot_token,
                        channel_id,
                        &format!("\"{title}\" is idle."),
                        None,
                    )
                    .await;
                }
                return Ok(());
            }
            Command::SendNow => {
                send_now_reply(host, sid, &bot_token, channel_id).await;
                return Ok(());
            }
            Command::Unpair => {
                let mut s_mut = load_store();
                if let Some(cfg) = s_mut.bridges.get_mut(sid) {
                    cfg.user_id = None;
                    cfg.channel_id = None;
                    cfg.user_name.clear();
                    cfg.pair_code = new_pair_code();
                    save_store(&s_mut)?;
                }
                post_message(
                    &bot_token,
                    channel_id,
                    &format!("🔌 Unpaired from \"{title}\". Send a new `!pair <code>` from Vicuna to pair again."),
                    None,
                )
                .await;
                return Ok(());
            }
            Command::ModelOverview => {
                let (out, blocks) = model_overview(host, &info).await;
                post_message(&bot_token, channel_id, &out, blocks).await;
                return Ok(());
            }
            Command::ModelEngine(eng) => {
                let (out, blocks) = pick_engine(host, sid, &eng).await;
                post_message(&bot_token, channel_id, &out, blocks).await;
                return Ok(());
            }
            Command::ModelSet(eng, model) => {
                let out = match host
                    .call(
                        "set_model",
                        json!({ "sessionId": sid, "engine": eng, "model": model }),
                    )
                    .await
                {
                    Ok(v) => v
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("Updated.")
                        .to_string(),
                    Err(e) => format!("⚠️ {e}"),
                };
                post_message(&bot_token, channel_id, &out, None).await;
                return Ok(());
            }
        }
    }

    // Free-form text reply answers a waiting ask card
    if !t.is_empty() && info.get("pendingAsk").and_then(Value::as_bool) == Some(true) {
        match host
            .call("answer_ask", json!({ "sessionId": sid, "text": t }))
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                post_message(&bot_token, channel_id, &format!("⚠️ {e}"), None).await;
                return Ok(());
            }
        }
    }

    // Slack attachments (`files[]`)
    let mut text_out = t.to_string();
    let mut images: Vec<Value> = Vec::new();
    let mut upload_notes: Vec<String> = Vec::new();

    if let Some(files) = event.get("files").and_then(Value::as_array) {
        for f in files {
            let Some(url) = f
                .get("url_private_download")
                .or_else(|| f.get("url_private"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let name = f
                .get("name")
                .or_else(|| f.get("title"))
                .and_then(Value::as_str)
                .unwrap_or("file")
                .to_string();
            let mime = f.get("mimetype").and_then(Value::as_str).unwrap_or("");
            let size = f.get("size").and_then(Value::as_u64).unwrap_or(0) as usize;
            if size > MAX_FILE {
                post_message(
                    &bot_token,
                    channel_id,
                    &format!("⚠️ {name} is over 20 MB."),
                    None,
                )
                .await;
                continue;
            }
            let Ok(resp) = http().get(url).bearer_auth(&bot_token).send().await else {
                post_message(
                    &bot_token,
                    channel_id,
                    &format!("⚠️ Couldn't download {name}."),
                    None,
                )
                .await;
                continue;
            };
            if !resp.status().is_success() {
                post_message(
                    &bot_token,
                    channel_id,
                    &format!("⚠️ Couldn't download {name} (HTTP {}).", resp.status()),
                    None,
                )
                .await;
                continue;
            }
            let resp_ct = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            if resp_ct.starts_with("text/html") && !mime.starts_with("text/html") {
                post_message(
                    &bot_token,
                    channel_id,
                    &format!("⚠️ Couldn't download {name} (check that the bot has the `files:read` scope)."),
                    None,
                )
                .await;
                continue;
            }
            let Ok(bytes) = resp.bytes().await else {
                continue;
            };
            if bytes.len() > MAX_FILE {
                continue;
            }

            let is_img = mime.starts_with("image/") || is_image_name(&name);
            if is_img && bytes.len() <= MAX_IMAGE_INLINE {
                let effective_mime = if mime.starts_with("image/") {
                    mime
                } else {
                    image_mime(&name)
                };
                images.push(json!(format!(
                    "data:{effective_mime};base64,{}",
                    base64::engine::general_purpose::STANDARD.encode(&bytes)
                )));
            } else {
                let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                match host
                    .call(
                        "stage_upload",
                        json!({ "sessionId": sid, "name": name, "base64": b64 }),
                    )
                    .await
                {
                    Ok(v) => {
                        if let Some(note) = v.get("note").and_then(Value::as_str) {
                            upload_notes.push(note.to_string());
                        }
                    }
                    Err(e) => {
                        post_message(
                            &bot_token,
                            channel_id,
                            &format!("⚠️ Couldn't save {name}: {e}"),
                            None,
                        )
                        .await;
                    }
                }
            }
        }
    }

    for note in upload_notes {
        text_out.push_str(&note);
    }

    let final_text = text_out.trim();
    if final_text.is_empty() && images.is_empty() {
        return Ok(());
    }

    let res = host
        .call(
            "deliver_message",
            json!({ "sessionId": sid, "text": final_text, "images": images }),
        )
        .await;
    match res {
        Ok(v) => {
            if v.get("queued").and_then(Value::as_bool) == Some(true) {
                let msg = "📨 Queued for the running turn. Click Send now (or `!now`) to interrupt it and send right away.";
                let blocks = build_button_blocks(msg, &[("⚡ Send now".into(), "sn".into())]);
                post_message(&bot_token, channel_id, msg, blocks).await;
            }
        }
        Err(e) => {
            post_message(&bot_token, channel_id, &format!("⚠️ {e}"), None).await;
        }
    }
    Ok(())
}

async fn handle_interactive(host: &Arc<HostClient>, app_token: &str, payload: &Value) {
    let Some(act) = parse_block_action(payload) else {
        return;
    };
    let store = load_store();
    let Some(sid) = route_interactive(&store, app_token, &act.channel_id) else {
        return;
    };
    let Some(cfg) = store.bridges.get(&sid) else {
        return;
    };
    if cfg.user_id.as_deref() != Some(act.user_id.as_str()) {
        return;
    }
    let bot_token = cfg.bot_token.clone();

    if act.action_id == "sn" {
        if let Some(ts) = &act.message_ts {
            update_message(
                &bot_token,
                &act.channel_id,
                ts,
                &act.orig_text,
                Some(json!([])),
            )
            .await;
        }
        send_now_reply(host, &sid, &bot_token, &act.channel_id).await;
        return;
    }

    if let Some(idx_str) = act.action_id.strip_prefix("a:") {
        let idx: usize = idx_str.parse().unwrap_or(usize::MAX);
        match host
            .call("answer_ask", json!({ "sessionId": sid, "choice": idx }))
            .await
        {
            Ok(v) => {
                let shown = v.get("shown").and_then(Value::as_str).unwrap_or("answered");
                let updated = if act.orig_text.is_empty() {
                    format!("→ {shown}")
                } else {
                    format!("{}\n\n→ {shown}", act.orig_text)
                };
                if let Some(ts) = &act.message_ts {
                    update_message(&bot_token, &act.channel_id, ts, &updated, Some(json!([])))
                        .await;
                }
            }
            Err(e) => {
                post_message(&bot_token, &act.channel_id, &format!("⚠️ {e}"), None).await;
            }
        }
        return;
    }

    if let Some(eng) = act.action_id.strip_prefix("e:") {
        let (text, blocks) = pick_engine(host, &sid, eng).await;
        if let Some(ts) = &act.message_ts {
            update_message(
                &bot_token,
                &act.channel_id,
                ts,
                &text,
                Some(blocks.unwrap_or_else(|| json!([]))),
            )
            .await;
        } else {
            post_message(&bot_token, &act.channel_id, &text, blocks).await;
        }
        return;
    }

    if let Some(idx_str) = act.action_id.strip_prefix("m:") {
        let out = pick_model(host, &sid, idx_str.parse().unwrap_or(usize::MAX)).await;
        if let Some(ts) = &act.message_ts {
            update_message(&bot_token, &act.channel_id, ts, &out, Some(json!([]))).await;
        } else {
            post_message(&bot_token, &act.channel_id, &out, None).await;
        }
    }
}

async fn send_now_reply(host: &Arc<HostClient>, sid: &str, bot_token: &str, channel_id: &str) {
    post_message(
        bot_token,
        channel_id,
        "⚡ Interrupting the running turn…",
        None,
    )
    .await;
    let host = host.clone();
    let (sid, bot_token, channel_id) = (
        sid.to_string(),
        bot_token.to_string(),
        channel_id.to_string(),
    );
    tokio::spawn(async move {
        match host.call("send_now", json!({ "sessionId": sid })).await {
            Ok(v) if v.get("sent").and_then(Value::as_u64) == Some(0) => {
                post_message(
                    &bot_token,
                    &channel_id,
                    "Nothing is queued: your messages already reached the session.",
                    None,
                )
                .await;
            }
            Ok(_) => {}
            Err(e) => {
                post_message(&bot_token, &channel_id, &format!("⚠️ {e}"), None).await;
            }
        }
    });
}

async fn model_overview(host: &Arc<HostClient>, info: &Value) -> (String, Option<Value>) {
    let engine = info.get("engine").and_then(Value::as_str).unwrap_or("?");
    let model = info
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("default");
    let engines = host
        .call("list_engines", json!({}))
        .await
        .ok()
        .and_then(|v| v.get("engines").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    let items: Vec<(String, String)> = engines
        .iter()
        .filter(|e| e.get("installed") == Some(&json!(true)))
        .map(|e| {
            let id = e.get("id").and_then(Value::as_str).unwrap_or("");
            let label = e.get("label").and_then(Value::as_str).unwrap_or(id);
            let mark = if id == engine { "✓ " } else { "" };
            (format!("{mark}{label}"), format!("e:{id}"))
        })
        .collect();
    let text = format!("This session runs `{engine}/{model}`.\nPick a harness:");
    let blocks = build_button_blocks(&text, &items);
    (text, blocks)
}

async fn pick_engine(host: &Arc<HostClient>, sid: &str, eng: &str) -> (String, Option<Value>) {
    let models = match host.call("list_models", json!({ "engine": eng })).await {
        Ok(v) => v
            .get("models")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        Err(e) => return (format!("⚠️ {e}"), None),
    };
    let shown: Vec<String> = models.into_iter().take(24).collect();
    let mut items = vec![("Default".to_string(), "m:0".to_string())];
    for (i, m) in shown.iter().enumerate() {
        items.push((m.chars().take(75).collect(), format!("m:{}", i + 1)));
    }
    let mut ids = vec!["default".to_string()];
    ids.extend(shown);
    if let Ok(mut r) = rt().lock() {
        r.bridges.entry(sid.to_string()).or_default().model_menu = Some((eng.to_string(), ids));
    }
    let text = format!("`{eng}` models:");
    let blocks = build_button_blocks(&text, &items);
    (text, blocks)
}

async fn pick_model(host: &Arc<HostClient>, sid: &str, i: usize) -> String {
    let pick = rt()
        .lock()
        .ok()
        .and_then(|r| r.bridges.get(sid).and_then(|b| b.model_menu.clone()));
    let Some((engine, ids)) = pick else {
        return "⚠️ That menu expired, send `!model` again.".into();
    };
    let Some(model) = ids.get(i).cloned() else {
        return "⚠️ Unknown model.".into();
    };
    match host
        .call(
            "set_model",
            json!({ "sessionId": sid, "engine": engine, "model": model }),
        )
        .await
    {
        Ok(v) => v
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Updated.")
            .to_string(),
        Err(e) => format!("⚠️ {e}"),
    }
}

fn is_image_name(n: &str) -> bool {
    let l = n.to_ascii_lowercase();
    [".jpg", ".jpeg", ".png", ".webp", ".gif"]
        .iter()
        .any(|e| l.ends_with(e))
}

fn image_mime(n: &str) -> &'static str {
    let l = n.to_ascii_lowercase();
    if l.ends_with(".png") {
        "image/png"
    } else if l.ends_with(".webp") {
        "image/webp"
    } else if l.ends_with(".gif") {
        "image/gif"
    } else {
        "image/jpeg"
    }
}

// ---------- main stdio JSON-RPC loop ----------

#[tokio::main]
async fn main() {
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let host = Arc::new(HostClient::new(out_tx.clone()));

    // Stdout writer
    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(mut line) = out_rx.recv().await {
            line.push('\n');
            if stdout.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = stdout.flush().await;
        }
    });

    spawn_socket_supervisor(host.clone());

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(v) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = v.get("id").and_then(Value::as_u64);
        let method = v.get("method").and_then(Value::as_str).map(str::to_string);

        if let (Some(req_id), Some(m)) = (id, method.clone()) {
            let params = v.get("params").cloned().unwrap_or(Value::Null);
            let tx_c = out_tx.clone();
            tokio::spawn(async move {
                let resp = match handle_bridge_rpc(&m, &params).await {
                    Ok(res) => json!({ "id": req_id, "result": res }),
                    Err(err) => json!({ "id": req_id, "error": err }),
                };
                let _ = tx_c.send(resp.to_string());
            });
        } else if let Some(resp_id) = id {
            if let Some(err) = v.get("error").and_then(Value::as_str) {
                host.resolve(resp_id, Err(err.to_string()));
            } else {
                host.resolve(resp_id, Ok(v.get("result").cloned().unwrap_or(Value::Null)));
            }
        } else if let Some(m) = method {
            let params = v.get("params").cloned().unwrap_or(Value::Null);
            match m.as_str() {
                "session_deleted" => {
                    if let Some(sid) = params.get("sessionId").and_then(Value::as_str) {
                        drop_for_path(&config_path(), sid);
                    }
                }
                "turn_event" => {
                    tokio::spawn(async move {
                        handle_turn_event(&params).await;
                    });
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_mock_slack_api() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first = req.lines().next().unwrap_or("");
                    let body = if first.contains("/auth.test") {
                        if req.contains("Bearer xoxb-bad-token-1234567890") {
                            json!({ "ok": false, "error": "invalid_auth" }).to_string()
                        } else {
                            json!({
                                "ok": true,
                                "user": "vicuna_bot",
                                "user_id": "U_BOT_1",
                                "bot_id": "B_BOT_1",
                                "team": "Acme"
                            })
                            .to_string()
                        }
                    } else if first.contains("/apps.connections.open") {
                        if req.contains("Bearer xapp-1-bad-token-1234567890") {
                            json!({ "ok": false, "error": "invalid_auth" }).to_string()
                        } else {
                            json!({
                                "ok": true,
                                "url": "wss://wss-primary.slack.com/link/?ticket=abc"
                            })
                            .to_string()
                        }
                    } else {
                        json!({ "ok": false, "error": "unknown_method" }).to_string()
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        format!("http://{addr}")
    }

    fn temp_cfg_path(tag: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp");
        let _ = std::fs::create_dir_all(&dir);
        let p = dir.join(format!("slack-{tag}-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn wrong_token_prefix_validation() {
        let err = validate_bot_token_format("xapp-1-A123-45678901234567890").unwrap_err();
        assert!(err.contains("App-Level Token"), "{err}");
        assert!(err.contains("xoxb-"), "{err}");

        let err2 = validate_bot_token_format("not-a-token").unwrap_err();
        assert!(err2.contains("xoxb-"), "{err2}");

        assert!(validate_bot_token_format("xoxb-1234567890-1234567890-abcdef").is_ok());

        let err3 = validate_app_token_format("xoxb-1234567890-1234567890-abcdef").unwrap_err();
        assert!(err3.contains("Bot Token"), "{err3}");
        assert!(err3.contains("xapp-"), "{err3}");

        let err4 = validate_app_token_format("short").unwrap_err();
        assert!(err4.contains("xapp-"), "{err4}");

        assert!(validate_app_token_format("xapp-1-A1234567890-1234567890-abcdef").is_ok());
    }

    #[tokio::test]
    async fn two_step_token_flow_and_start_over() {
        let api = spawn_mock_slack_api().await;
        let cfg = temp_cfg_path("two-step");
        let sid = "test-sess-two-step";

        // Initial unconfigured status
        let st0 = handle_bridge_rpc_at(&api, &cfg, "bridge_status", &json!({ "sessionId": sid }))
            .await
            .unwrap();
        assert_eq!(st0["configured"], false);
        assert!(st0.get("inputPrompt").is_none());

        // Step 1: pasting xapp- in step 1 returns clear error
        let e1 = handle_bridge_rpc_at(
            &api,
            &cfg,
            "bridge_save",
            &json!({ "sessionId": sid, "token": "xapp-1-A1234567890-1234567890-abcdef" }),
        )
        .await
        .unwrap_err();
        assert!(e1.contains("xoxb-"), "{e1}");

        // Step 1: valid xoxb- returns inputPrompt for xapp-
        let st1 = handle_bridge_rpc_at(
            &api,
            &cfg,
            "bridge_save",
            &json!({ "sessionId": sid, "token": "xoxb-1234567890-1234567890-abcdef" }),
        )
        .await
        .unwrap();
        assert_eq!(st1["configured"], false);
        assert_eq!(st1["botUsername"], "vicuna_bot");
        let prompt = &st1["inputPrompt"];
        assert_eq!(prompt["secret"], true);
        assert_eq!(prompt["placeholder"], "xapp-1-…");
        assert_eq!(prompt["submitLabel"], "Connect");
        assert!(prompt["note"].as_str().unwrap().contains("@vicuna_bot"));

        // Step 2: pasting xoxb- instead of xapp- returns clear error and keeps pending prompt
        let e2 = handle_bridge_rpc_at(
            &api,
            &cfg,
            "bridge_save",
            &json!({ "sessionId": sid, "token": "xoxb-1234567890-1234567890-abcdef" }),
        )
        .await
        .unwrap_err();
        assert!(e2.contains("xapp-"), "{e2}");

        // "Start over" via bridge_remove clears pending setup
        let cleared =
            handle_bridge_rpc_at(&api, &cfg, "bridge_remove", &json!({ "sessionId": sid }))
                .await
                .unwrap();
        assert_eq!(cleared["configured"], false);
        assert!(cleared.get("inputPrompt").is_none());

        // Re-run Step 1 + Step 2 to completion
        let _ = handle_bridge_rpc_at(
            &api,
            &cfg,
            "bridge_save",
            &json!({ "sessionId": sid, "token": "xoxb-1234567890-1234567890-abcdef" }),
        )
        .await
        .unwrap();
        let st2 = handle_bridge_rpc_at(
            &api,
            &cfg,
            "bridge_save",
            &json!({ "sessionId": sid, "token": "xapp-1-A1234567890-1234567890-abcdef" }),
        )
        .await
        .unwrap();
        assert_eq!(st2["configured"], true);
        assert_eq!(st2["connected"], true);
        assert_eq!(st2["botUsername"], "vicuna_bot");
        assert_eq!(st2["tokenTail"], "••••cdef");
        assert_eq!(st2["pairCode"].as_str().unwrap().len(), 6);
        assert!(st2["pairingHint"].as_str().unwrap().contains("@vicuna_bot"));
        assert!(st2.get("inputPrompt").is_none());

        // Verify persisted config on disk with both tokens
        let loaded = load_store_from(&cfg);
        let saved = loaded.bridges.get(sid).unwrap();
        assert_eq!(saved.bot_token, "xoxb-1234567890-1234567890-abcdef");
        assert_eq!(saved.app_token, "xapp-1-A1234567890-1234567890-abcdef");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&cfg).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }

        // Cleanup
        let _ =
            handle_bridge_rpc_at(&api, &cfg, "bridge_remove", &json!({ "sessionId": sid })).await;
        let _ = std::fs::remove_file(&cfg);
    }

    #[test]
    fn config_save_and_load_roundtrip() {
        let path = temp_cfg_path("roundtrip");
        let mut store = Store::default();
        store.bridges.insert(
            "s1".into(),
            BridgeConfig {
                bot_token: "xoxb-test-1234567890-abcdef".into(),
                app_token: "xapp-1-test-1234567890-abcdef".into(),
                enabled: true,
                bot_user_id: "U1".into(),
                bot_id: "B1".into(),
                bot_username: "bot".into(),
                team_name: "Team".into(),
                user_id: Some("U_USER".into()),
                channel_id: Some("D_CHAN".into()),
                user_name: "Alice".into(),
                pair_code: String::new(),
            },
        );
        save_store_to(&path, &store).unwrap();
        let loaded = load_store_from(&path);
        assert_eq!(loaded, store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn chunks_respect_slack_limit_and_utf8() {
        let long = "a".repeat(MAX_MSG * 2 + 25);
        let parts = chunks(&long);
        assert_eq!(parts.len(), 3);
        assert!(parts.iter().all(|p| p.len() <= MAX_MSG));

        let unicode = "🦀".repeat(MAX_MSG); // 4 bytes each
        let uparts = chunks(&unicode);
        assert!(uparts.iter().all(|p| p.len() <= MAX_MSG));
        assert_eq!(uparts.concat(), unicode);

        assert_eq!(chunks("hello\nworld"), vec!["hello\nworld".to_string()]);
    }

    #[test]
    fn block_kit_ask_payload_and_action_parsing() {
        let (text, blocks) = build_ask_payload(
            "Approve running tests?",
            &["Allow".into(), "Deny".into()],
            true,
        );
        assert!(text.contains("Approve running tests?"));
        assert!(text.contains("Click a choice or reply with text"));
        let arr = blocks.unwrap();
        let arr = arr.as_array().unwrap();
        assert_eq!(arr[0]["type"], "section");
        assert_eq!(arr[1]["type"], "actions");
        let btns = arr[1]["elements"].as_array().unwrap();
        assert_eq!(btns.len(), 2);
        assert_eq!(btns[0]["action_id"], "a:0");
        assert_eq!(btns[0]["text"]["text"], "Allow");
        assert_eq!(btns[1]["action_id"], "a:1");

        let interactive = json!({
            "type": "block_actions",
            "user": { "id": "U123" },
            "channel": { "id": "D456" },
            "message": { "ts": "1700000000.000100", "text": "❓ Approve?" },
            "actions": [{ "type": "button", "action_id": "a:1", "value": "a:1" }]
        });
        let parsed = parse_block_action(&interactive).unwrap();
        assert_eq!(parsed.user_id, "U123");
        assert_eq!(parsed.channel_id, "D456");
        assert_eq!(parsed.message_ts.as_deref(), Some("1700000000.000100"));
        assert_eq!(parsed.action_id, "a:1");
    }

    #[tokio::test]
    async fn progress_edit_after_finish_is_skipped() {
        // the final reply removed the turn: a late ticker/progress edit must
        // not touch the message (no HTTP is attempted, so this needs no network)
        let sent = update_progress("xoxb-x", "C1", "1.0", "no-such-bridge", "turn").await;
        assert!(!sent);
        drop_msg_lock("1.0");
    }

    #[test]
    fn command_parsing_covers_all_commands() {
        assert_eq!(parse_command("!help"), Some(Command::Help));
        assert_eq!(parse_command("<@U0123456> !status"), Some(Command::Status));
        assert!(is_pair_attempt("<@U0123456> !pair 463985", "463985"));
        assert_eq!(parse_command("!status"), Some(Command::Status));
        assert_eq!(parse_command("!stop"), Some(Command::Stop));
        assert_eq!(parse_command("!now"), Some(Command::SendNow));
        assert_eq!(parse_command("!send_now"), Some(Command::SendNow));
        assert_eq!(parse_command("!unpair"), Some(Command::Unpair));
        assert_eq!(parse_command("!model"), Some(Command::ModelOverview));
        assert_eq!(
            parse_command("!model claude"),
            Some(Command::ModelEngine("claude".into()))
        );
        assert_eq!(
            parse_command("!model claude sonnet"),
            Some(Command::ModelSet("claude".into(), "sonnet".into()))
        );
        assert_eq!(parse_command("hello world"), None);
    }

    #[test]
    fn multi_session_routing_and_bot_filtering() {
        let mut store = Store::default();
        store.bridges.insert(
            "sess-a".into(),
            BridgeConfig {
                bot_token: "xoxb-shared-1".into(),
                app_token: "xapp-shared".into(),
                enabled: true,
                channel_id: Some("C_A".into()),
                pair_code: String::new(),
                ..Default::default()
            },
        );
        store.bridges.insert(
            "sess-b".into(),
            BridgeConfig {
                bot_token: "xoxb-shared-1".into(),
                app_token: "xapp-shared".into(),
                enabled: true,
                channel_id: None,
                pair_code: "654321".into(),
                ..Default::default()
            },
        );
        store.bridges.insert(
            "sess-c".into(),
            BridgeConfig {
                bot_token: "xoxb-other-2".into(),
                app_token: "xapp-other".into(),
                enabled: true,
                channel_id: Some("C_C".into()),
                pair_code: String::new(),
                ..Default::default()
            },
        );

        assert_eq!(
            route_incoming_message(&store, "xapp-shared", "C_A", "hi"),
            RouteTarget::Paired("sess-a".into())
        );
        assert_eq!(
            route_incoming_message(&store, "xapp-shared", "D_NEW", "!pair 654321"),
            RouteTarget::PairMatch("sess-b".into())
        );
        assert_eq!(
            route_incoming_message(&store, "xapp-shared", "D_NEW", "random"),
            RouteTarget::UnpairedHint {
                bot_token: "xoxb-shared-1".into()
            }
        );
        // Distinct app isolation
        assert_eq!(
            route_incoming_message(&store, "xapp-shared", "C_C", "hi"),
            RouteTarget::UnpairedHint {
                bot_token: "xoxb-shared-1".into()
            }
        );
        assert_eq!(
            route_incoming_message(&store, "xapp-other", "C_C", "hi"),
            RouteTarget::Paired("sess-c".into())
        );
        assert_eq!(
            route_interactive(&store, "xapp-shared", "C_A"),
            Some("sess-a".into())
        );
        assert_eq!(route_interactive(&store, "xapp-other", "C_A"), None);

        // Bot / subtype filtering
        let mut bots = HashSet::new();
        bots.insert("U_BOT".into());
        assert!(should_ignore_message_event(
            &json!({"type":"message","bot_id":"B1","user":"U1","text":"hi"}),
            &bots
        ));
        assert!(should_ignore_message_event(
            &json!({"type":"message","user":"U_BOT","text":"hi"}),
            &bots
        ));
        assert!(should_ignore_message_event(
            &json!({"type":"message","subtype":"message_changed","user":"U1"}),
            &bots
        ));
        assert!(should_ignore_message_event(
            &json!({"type":"message","subtype":"message_deleted","user":"U1"}),
            &bots
        ));
        assert!(should_ignore_message_event(
            &json!({"type":"message","subtype":"bot_message","user":"U1"}),
            &bots
        ));
        assert!(!should_ignore_message_event(
            &json!({"type":"message","user":"U1","text":"hello"}),
            &bots
        ));
        assert!(!should_ignore_message_event(
            &json!({"type":"message","subtype":"file_share","user":"U1","text":""}),
            &bots
        ));
    }

    async fn read_http_request(sock: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
        let mut raw = Vec::new();
        let mut buf = [0u8; 4096];
        let header_end = loop {
            let n = sock.read(&mut buf).await.unwrap_or(0);
            if n == 0 {
                return (String::from_utf8_lossy(&raw).into_owned(), Vec::new());
            }
            raw.extend_from_slice(&buf[..n]);
            if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers = String::from_utf8_lossy(&raw[..header_end]).into_owned();
        let content_len: usize = headers
            .lines()
            .find_map(|line| {
                let (k, v) = line.split_once(':')?;
                if k.eq_ignore_ascii_case("content-length") {
                    v.trim().parse().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);
        while raw.len() < header_end + content_len {
            let n = sock.read(&mut buf).await.unwrap_or(0);
            if n == 0 {
                break;
            }
            raw.extend_from_slice(&buf[..n]);
        }
        let body = raw[header_end..(header_end + content_len).min(raw.len())].to_vec();
        (headers, body)
    }

    #[tokio::test]
    async fn three_step_image_upload() {
        #[derive(Default)]
        struct Recorded {
            step1_query: Option<String>,
            step1_auth: Option<String>,
            step2_content_type: Option<String>,
            step2_bytes: Option<Vec<u8>>,
            step3_auth: Option<String>,
            step3_body: Option<Value>,
        }

        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let rec_srv = recorded.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            for _ in 0..3 {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let rec = rec_srv.clone();
                tokio::spawn(async move {
                    let (headers, body) = read_http_request(&mut sock).await;
                    let first = headers.lines().next().unwrap_or("").to_string();
                    let auth = headers.lines().find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("authorization")
                            .then(|| v.trim().to_string())
                    });
                    let content_type = headers.lines().find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-type")
                            .then(|| v.trim().to_string())
                    });

                    let (status, resp_body) =
                        if first.starts_with("GET /files.getUploadURLExternal") {
                            let mut r = rec.lock().unwrap();
                            r.step1_query = Some(first.clone());
                            r.step1_auth = auth;
                            (
                                "200 OK",
                                json!({
                                    "ok": true,
                                    "upload_url": format!("http://{addr}/upload/F_TEST_1"),
                                    "file_id": "F_TEST_1"
                                })
                                .to_string(),
                            )
                        } else if first.starts_with("POST /upload/F_TEST_1") {
                            let mut r = rec.lock().unwrap();
                            r.step2_content_type = content_type;
                            r.step2_bytes = Some(body);
                            ("200 OK", "OK - 12".to_string())
                        } else if first.starts_with("POST /files.completeUploadExternal") {
                            let mut r = rec.lock().unwrap();
                            r.step3_auth = auth;
                            r.step3_body = serde_json::from_slice(&body).ok();
                            ("200 OK", json!({ "ok": true, "files": [] }).to_string())
                        } else {
                            (
                                "404 Not Found",
                                json!({ "ok": false, "error": "not_found" }).to_string(),
                            )
                        };

                    let resp = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        resp_body.len(),
                        resp_body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let payload = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3, 4];
        upload_image_at(
            &format!("http://{addr}"),
            "xoxb-test-token",
            "C_CHAN_1",
            payload.clone(),
            "image/png",
            "chart.png",
            Some("Here is the chart"),
        )
        .await
        .expect("3-step upload should succeed");

        let r = recorded.lock().unwrap();
        let q = r.step1_query.as_deref().unwrap_or("");
        assert!(q.contains("filename=chart.png"), "{q}");
        assert!(q.contains("length=12"), "{q}");
        assert_eq!(r.step1_auth.as_deref(), Some("Bearer xoxb-test-token"));
        assert_eq!(r.step2_content_type.as_deref(), Some("image/png"));
        assert_eq!(r.step2_bytes.as_deref(), Some(payload.as_slice()));
        assert_eq!(r.step3_auth.as_deref(), Some("Bearer xoxb-test-token"));
        assert_eq!(
            r.step3_body,
            Some(json!({
                "files": [{ "id": "F_TEST_1", "title": "chart.png" }],
                "channel_id": "C_CHAN_1",
                "initial_comment": "Here is the chart"
            }))
        );
    }
}
