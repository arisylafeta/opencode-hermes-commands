/**
 * opencode-hermes-commands.ts — OpenCode → WhatsApp relay plugin (Phase 3A)
 *
 * Captures high-signal opencode events and routes them through two paths:
 *
 *   1. Direct WhatsApp path (reliable fallback):
 *        session.error → WhatsApp bridge (http://127.0.0.1:3000/send)
 *        Always works, even if Hermes is down.
 *
 *   2. Hermes orchestrator path (Phase 3A: one-way summaries):
 *        session.status(idle) → Hermes /v1/runs (with idle debouncing)
 *        permission.asked    → Hermes /v1/runs
 *        Hermes decides whether/how to notify Ari via its own skills.
 *
 * Architecture (both paths share the same robustness patterns):
 *   event hook → dedup check → enqueue (SQLite, INSERT OR IGNORE) → return
 *   drain loop (5s) → atomic claim → [rate limit] → send → complete/release
 *
 * Reliability features:
 *   - Persistent queue (SQLite WAL): alerts survive opencode restarts.
 *   - Atomic row claiming: safe across multiple opencode processes.
 *   - Dedup at enqueue time (unique index on event_id): no duplicates.
 *   - Proper backoff via next_attempt_at: retries respect delays.
 *   - Backpressure: max 100 queued, drops newest (preserves oldest critical).
 *   - Rate limiting: max 10 sends/hour (WhatsApp path only; Hermes self-limits).
 *   - Single drain guard: no overlapping drain within a process.
 *   - Idle debouncing: prevents Hermes spam on busy↔idle cycling.
 *
 * Robustness contract:
 *   - The `event` hook is fire-and-forget. try/catch EVERYTHING, never throw.
 *   - The event hook only does fast SQLite inserts + detached async context
 *     reads (non-blocking).
 *   - The drain loop is wrapped in try/catch per iteration.
 *   - Detached async operations have .catch() to swallow rejections.
 *   - Logging via client.app.log() with console.error fallback.
 *
 * Config (env vars):
 *   HERMES_WHATSAPP_BRIDGE_URL  default http://127.0.0.1:3000
 *   HERMES_WHATSAPP_CHAT_ID     default 248730783625457@lid (Ari)
 *   HERMES_RELAY_ENABLED        default "true"
 *   HERMES_RELAY_LOG_LEVEL      default "error"
 *   HERMES_RELAY_DB_PATH        default ~/.hermes/plugins/opencode-hermes-commands/state.db
 *   HERMES_RELAY_MAX_QUEUE      default 100
 *   HERMES_RELAY_MAX_PER_HOUR  default 10
 *   HERMES_API_URL              default http://127.0.0.1:8642
 *   HERMES_API_KEY              default local-dench-hermes-key
 *   HERMES_PROFILE              default "coder"
 *   HERMES_RELAY_IDLE_DEBOUNCE        default 60000 (60s)
 *   HERMES_RELAY_IDLE_MIN_INTERVAL    default 600000 (10min)
 *   HERMES_RELAY_PERMISSION_GRACE     default 15000 (15s)
 *   HERMES_RELAY_MAX_CONTEXT          default 10 (last N messages)
 *
 * V1 plugin entrypoint: export default { id, server: async (input, options) => Hooks }
 */

import { Database } from "bun:sqlite";

// ── Config ──────────────────────────────────────────────────────────────────

const BRIDGE_URL =
  process.env.HERMES_WHATSAPP_BRIDGE_URL ?? "http://127.0.0.1:3000";
const CHAT_ID =
  process.env.HERMES_WHATSAPP_CHAT_ID ?? "248730783625457@lid";
const ENABLED = process.env.HERMES_RELAY_ENABLED !== "false";
const LOG_LEVEL = process.env.HERMES_RELAY_LOG_LEVEL ?? "error";
const DB_PATH =
  process.env.HERMES_RELAY_DB_PATH ??
  `${process.env.HOME ?? "/root"}/.hermes/plugins/opencode-hermes-commands/state.db`;
const MAX_QUEUE = parseInt(process.env.HERMES_RELAY_MAX_QUEUE ?? "100", 10);
const MAX_PER_HOUR = parseInt(process.env.HERMES_RELAY_MAX_PER_HOUR ?? "10", 10);
const RELAY_PLATFORM = process.env.HERMES_RELAY_PLATFORM ?? "whatsapp";
const TELEGRAM_BOT_TOKEN =
  process.env.HERMES_TELEGRAM_BOT_TOKEN ??
  process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID =
  process.env.HERMES_TELEGRAM_CHAT_ID ??
  process.env.TELEGRAM_HOME_CHANNEL;

// Events that warrant an immediate phone notification.
const PHONE_EVENTS = new Set<string>(["session.error"]);

const DRAIN_INTERVAL_MS = 5000;
const DEDUP_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
const SEND_TIMEOUT_MS = 5000;
const MAX_ATTEMPTS = 4;
// Larger delays: don't drop critical alerts after 20s.
const RETRY_DELAYS = [30_000, 120_000, 300_000]; // 30s, 2min, 5min
const LOCK_TIMEOUT_MS = 60_000; // stale lock recovery

// ── Hermes orchestrator config (Phase 3A: one-way summaries) ────────────────
// Hermes is the orchestrator that receives session summaries and decides
// whether to notify Ari via WhatsApp (using its own skills + rate limiting).
const HERMES_API_URL =
  process.env.HERMES_API_URL ?? "http://127.0.0.1:8642";
const HERMES_API_KEY =
  process.env.HERMES_API_KEY ?? "local-dench-hermes-key";
const HERMES_PROFILE = process.env.HERMES_PROFILE ?? "coder";
// Reply-command loop (approve/reject/answer/continue from WhatsApp). Enabled by
// default now that abort/undo noise is filtered and commands use guarded SDK
// interactions. Set HERMES_RELAY_ENABLE_COMMANDS=false to disable.
const COMMANDS_ENABLED = process.env.HERMES_RELAY_ENABLE_COMMANDS !== "false";
const IDLE_DEBOUNCE_MS = parseInt(
  process.env.HERMES_RELAY_IDLE_DEBOUNCE ?? "60000",
  10,
); // 60s — wait this long after busy→idle before notifying
const IDLE_MIN_INTERVAL_MS = parseInt(
  process.env.HERMES_RELAY_IDLE_MIN_INTERVAL ?? "600000",
  10,
); // 10min — never notify idle more often than this
const PERMISSION_GRACE_MS = parseInt(
  process.env.HERMES_RELAY_PERMISSION_GRACE ?? "15000",
  10,
); // 15s — grace window for permission prompts (reserved for future use)
const MAX_CONTEXT_MESSAGES = parseInt(
  process.env.HERMES_RELAY_MAX_CONTEXT ?? "10",
  10,
); // last N messages to include as context

// ── Types ───────────────────────────────────────────────────────────────────

interface OpencodeEvent {
  id: string;
  type: string;
  properties: Record<string, unknown>;
}

interface PluginInput {
  project: { id: string };
  directory: string;
  worktree: string;
  client?: {
    app?: {
      log?: (args: {
        body: { service: string; level: string; message: string };
      }) => Promise<unknown>;
    };
    session?: {
      get?: (params: { sessionID: string }) => Promise<unknown>;
      messages?: (params: { sessionID: string }) => Promise<{
        data: Array<{
          info: { role: string };
          parts: Array<{ type: string; text?: string }>;
        }>;
      }>;
      prompt?: (params: {
        sessionID: string;
        parts: Array<{ type: string; text: string }>;
      }) => Promise<unknown>;
      promptAsync?: (params: {
        sessionID: string;
        parts: Array<{ type: string; text: string }>;
      }) => Promise<unknown>;
    };
    permission?: {
      reply?: (params: {
        requestID: string;
        reply: string;
        message?: string;
      }) => Promise<unknown>;
    };
    question?: {
      reply?: (params: {
        requestID: string;
        answers: string[][];
      }) => Promise<unknown>;
    };
  };
}

interface Hooks {
  event?: (input: { event: OpencodeEvent }) => Promise<void>;
}

interface QueueRow {
  id: number;
  event_id: string;
  message: string;
  created_at: number;
  attempts: number;
  next_attempt_at: number;
  status: string;
}

// Mirrors QueueRow but carries typed Hermes context instead of a flat message.
interface HermesQueueRow {
  id: number;
  event_id: string;
  event_type: string;
  session_id: string;
  directory: string;
  context_json: string;
  created_at: number;
  attempts: number;
  next_attempt_at: number;
  status: string;
}

// ── Logging ─────────────────────────────────────────────────────────────────

let appLog:
  | ((args: {
      body: { service: string; level: string; message: string };
    }) => Promise<unknown>)
  | null = null;

// Phase 3B: stored client reference for command execution (approve/reject/
// answer/continue). Set in the entrypoint before the command drain loop starts.
let pluginClient: PluginInput["client"] | null = null;

// Session title tracking. Populated from session.created/session.updated events.
// Used to include a human-readable name in WhatsApp notifications.
const sessionTitles = new Map<string, string>();

// Last assistant message text per session. Populated from message.part.updated
// events. Used to give Hermes the actual final response for summarization.
const sessionLastAssistantText = new Map<string, string>();

// Child sessions created for subagents have parentID set. Use this instead of
// brittle title matching when deciding whether a session should notify.
const subagentSessions = new Set<string>();

// Track message roles so we only cache assistant-visible text parts.
const messageRoles = new Map<string, string>();

function log(level: string, message: string): void {
  if (level !== "error" && LOG_LEVEL !== "debug") return;
  try {
    if (appLog) {
      void appLog({
        body: { service: "opencode-hermes-commands", level, message },
      }).catch(() => {
        try {
          console.error(`[opencode-hermes-commands] ${message}`);
        } catch {
          /* swallow */
        }
      });
    } else {
      console.error(`[opencode-hermes-commands] ${message}`);
    }
  } catch {
    /* logging must never throw */
  }
}

// ── Message formatting ──────────────────────────────────────────────────────

function shortId(id: string): string {
  return id.length > 12 ? id.slice(0, 12) : id;
}

function truncate(s: string, max = 1200): string {
  return s.length > max ? `${s.slice(0, max)}…` : s;
}

function shortDir(dir: string): string {
  if (!dir || dir === "?") return "?";
  const parts = dir.replace(/\/+$/, "").split("/");
  if (parts.length <= 2) return dir;
  return `…/${parts.slice(-2).join("/")}`;
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function extractError(props: Record<string, unknown>): string {
  const error = props.error;
  if (typeof error === "string") return error;
  if (error && typeof error === "object") {
    const e = error as Record<string, unknown>;
    try {
      return String(e.message ?? e.code ?? safeStringify(error));
    } catch {
      return String(e.message ?? e.code ?? "object error");
    }
  }
  return "unknown error";
}

function isBenignSessionError(props: Record<string, unknown>): boolean {
  const error = props.error;
  const name =
    error && typeof error === "object" ? String((error as Record<string, unknown>).name ?? "") : "";
  const raw = `${name} ${extractError(props)}`.toLowerCase();
  return (
    raw.includes("messageabortederror") ||
    raw.includes("aborted") ||
    raw.includes("cancel") ||
    raw.includes("undo") ||
    raw.includes("rewind")
  );
}

function sanitizeErrorMessage(props: Record<string, unknown>): string {
  const error = props.error;
  if (error && typeof error === "object") {
    const e = error as Record<string, unknown>;
    const name = String(e.name ?? "session error").trim();
    const message = String(e.message ?? "session error").trim();
    return truncate(name && name !== message ? `${name}: ${message}` : message);
  }
  return truncate(extractError(props));
}

function clearSessionTracking(sessionId: string): void {
  if (!sessionId) return;
  sessionLastAssistantText.delete(sessionId);
}

function shouldNotifySession(sessionId: string): boolean {
  return !subagentSessions.has(sessionId);
}

function formatMessage(event: OpencodeEvent, input: PluginInput): string {
  const sid = shortId(String(event.properties?.sessionID ?? "unknown"));
  const dir = shortDir(input.directory ?? "?");
  const fullSid = String(event.properties?.sessionID ?? "");
  const title = sessionTitles.get(fullSid) ?? "session";

  if (event.type === "session.error") {
    const msg = sanitizeErrorMessage(event.properties ?? {});
    return `\u{1F534} opencode error\n*${title}*\n${msg}`;
  }

  return `\u{26A0}\u{FE0F} opencode: ${event.type}`;
}

// ── Persistent store (SQLite) ───────────────────────────────────────────────

let db: Database | null = null;

function initStore(): void {
  db = new Database(DB_PATH);
  db.run("PRAGMA journal_mode = WAL");
  db.run("PRAGMA busy_timeout = 3000");
  db.run(`
    CREATE TABLE IF NOT EXISTS queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      message TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending'
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS dedup (
      event_id TEXT PRIMARY KEY,
      created_at INTEGER NOT NULL
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS sent_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sent_at INTEGER NOT NULL
    )
  `);
  db.run("CREATE INDEX IF NOT EXISTS idx_queue_ready ON queue(status, next_attempt_at)");
  db.run("CREATE INDEX IF NOT EXISTS idx_sent_log_at ON sent_log(sent_at)");

  // Phase 3A: Hermes orchestrator queue. Same shape as `queue` but carries
  // typed context (event_type, session_id, directory, context_json) that the
  // Hermes drain loop forwards to the Hermes /v1/runs endpoint.
  db.run(`
    CREATE TABLE IF NOT EXISTS hermes_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id TEXT NOT NULL UNIQUE,
      event_type TEXT NOT NULL,
      session_id TEXT NOT NULL,
      directory TEXT NOT NULL,
      context_json TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pending'
    )
  `);
  db.run("CREATE INDEX IF NOT EXISTS idx_hermes_queue_ready ON hermes_queue(status, next_attempt_at)");

  // Phase 3A: idle debouncing. Prevents spamming Hermes with session.idle
  // notifications when a session rapidly cycles busy↔idle.
  db.run(`
    CREATE TABLE IF NOT EXISTS idle_debounce (
      session_id TEXT PRIMARY KEY,
      last_notified_at INTEGER NOT NULL,
      last_busy_at INTEGER NOT NULL
    )
  `);

  // Phase 3B: correlation table. Maps a short token (shortId of the opencode
  // session ID) to the full session ID, directory, event type, and request ID.
  // The bridge script writes commands keyed by token; the plugin looks up the
  // correlation to recover the session ID + request ID needed to act.
  db.run(`
    CREATE TABLE IF NOT EXISTS correlations (
      token TEXT PRIMARY KEY,
      opencode_session_id TEXT NOT NULL,
      directory TEXT NOT NULL,
      event_type TEXT NOT NULL,
      request_id TEXT,
      created_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL
    )
  `);

  // Phase 3B: command queue. Written by the bridge script
  // (/usr/local/bin/opencode_bridge.py) and drained by this plugin. Each row
  // is a single action (approve/reject/answer/continue) targeting a token.
  db.run(`
    CREATE TABLE IF NOT EXISTS commands (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      token TEXT NOT NULL,
      action TEXT NOT NULL,
      payload TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      claimed_at INTEGER,
      result TEXT,
      expires_at INTEGER NOT NULL
    )
  `);
  db.run("CREATE INDEX IF NOT EXISTS idx_commands_pending ON commands(token, status)");

  // Lightweight migration: add columns if upgrading from an older schema.
  migrateSchema();
}

function migrateSchema(): void {
  if (!db) return;
  try {
    const cols = db
      .query("PRAGMA table_info(queue)")
      .all() as { name: string }[];
    const colNames = new Set(cols.map((c) => c.name));
    if (!colNames.has("next_attempt_at")) {
      db.run(
        "ALTER TABLE queue ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0",
      );
    }
    if (!colNames.has("status")) {
      db.run(
        "ALTER TABLE queue ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'",
      );
    }
  } catch (err) {
    log("error", `schema migration failed: ${err}`);
  }
}

function isDuplicate(eventId: string): boolean {
  if (!db) return false;
  try {
    const row = db
      .query("SELECT 1 FROM dedup WHERE event_id = ?")
      .get(eventId) as { "1": number } | null;
    // bun:sqlite returns null for no rows, not undefined.
    // Use != null to catch both null and undefined.
    return row != null;
  } catch (err) {
    log("error", `dedup check failed: ${err}`);
    return false; // fail open
  }
}

function enqueue(eventId: string, message: string): void {
  if (!db) return;
  try {
    // Backpressure: if queue is full, reject the incoming event.
    // Never delete existing queued alerts (they may be in-flight or critical).
    const count = db.query("SELECT COUNT(*) as c FROM queue").get() as {
      c: number;
    };
    if (count.c >= MAX_QUEUE) {
      log("error", `queue full (${MAX_QUEUE}), dropped incoming event ${eventId}`);
      return;
    }
    // INSERT OR IGNORE: unique index on event_id prevents duplicates.
    db.run(
      `INSERT OR IGNORE INTO queue (event_id, message, created_at, next_attempt_at, status)
       VALUES (?, ?, ?, 0, 'pending')`,
      [eventId, message, Date.now()],
    );
  } catch (err) {
    log("error", `enqueue failed: ${err}`);
  }
}

// Atomically claim the next ready row. Safe across multiple processes.
// Uses UPDATE...RETURNING to atomically select + lock in one statement.
function claimNextRow(): QueueRow | null {
  if (!db) return null;
  try {
    const now = Date.now();
    // Recover stale locks first.
    db.run(
      `UPDATE queue SET status = 'pending'
       WHERE status = 'sending' AND next_attempt_at < ?`,
      [now - LOCK_TIMEOUT_MS],
    );
    // Atomic claim: mark as 'sending' and return the row.
    const row = db
      .query(
        `UPDATE queue
         SET status = 'sending', next_attempt_at = ?
         WHERE id = (
           SELECT id FROM queue
           WHERE status = 'pending' AND next_attempt_at <= ?
           ORDER BY id LIMIT 1
         )
         RETURNING id, event_id, message, created_at, attempts, next_attempt_at, status`,
      )
      .get(now + LOCK_TIMEOUT_MS, now) as QueueRow | undefined;
    return row ?? null;
  } catch (err) {
    log("error", `claim failed: ${err}`);
    return null;
  }
}

function completeRow(id: number, eventId: string): void {
  if (!db) return;
  try {
    // Transactional: delete from queue + mark dedup atomically.
    // If process dies between these, a duplicate could re-enqueue.
    db.run("BEGIN");
    db.run("DELETE FROM queue WHERE id = ?", [id]);
    db.run(
      "INSERT OR IGNORE INTO dedup (event_id, created_at) VALUES (?, ?)",
      [eventId, Date.now()],
    );
    db.run("COMMIT");
  } catch (err) {
    log("error", `complete failed: ${err}`);
    try {
      db?.run("ROLLBACK");
    } catch {
      /* ignore rollback errors */
    }
  }
}

function releaseRow(id: number, nextAttemptAt: number): void {
  if (!db) return;
  try {
    db.run(
      "UPDATE queue SET status = 'pending', next_attempt_at = ? WHERE id = ?",
      [nextAttemptAt, id],
    );
  } catch (err) {
    log("error", `release failed: ${err}`);
  }
}

function rescheduleRow(id: number, attempts: number): void {
  if (!db) return;
  try {
    const delayIdx = Math.min(attempts, RETRY_DELAYS.length - 1);
    const delay = RETRY_DELAYS[delayIdx];
    db.run(
      `UPDATE queue
       SET status = 'pending', attempts = ?, next_attempt_at = ?
       WHERE id = ?`,
      [attempts + 1, Date.now() + delay, id],
    );
  } catch (err) {
    log("error", `reschedule failed: ${err}`);
  }
}

function dropRow(id: number): void {
  if (!db) return;
  try {
    db.run("DELETE FROM queue WHERE id = ?", [id]);
  } catch (err) {
    log("error", `drop failed: ${err}`);
  }
}

// ── Hermes queue operations (mirror the WhatsApp queue, typed context) ──────
// These mirror the `queue` operations above but operate on `hermes_queue`.
// The Hermes path has NO rate limiting (Hermes handles its own throttling via
// its skills), but keeps backpressure, atomic claiming, and retry/backoff.

function enqueueHermes(
  eventId: string,
  eventType: string,
  sessionId: string,
  directory: string,
  contextJson: string,
): void {
  if (!db) return;
  try {
    // Backpressure: same MAX_QUEUE cap as the WhatsApp queue.
    const count = db.query("SELECT COUNT(*) as c FROM hermes_queue").get() as {
      c: number;
    };
    if (count.c >= MAX_QUEUE) {
      log(
        "error",
        `hermes_queue full (${MAX_QUEUE}), dropped incoming event ${eventId}`,
      );
      return;
    }
    // INSERT OR IGNORE: unique index on event_id prevents duplicates.
    db.run(
      `INSERT OR IGNORE INTO hermes_queue
       (event_id, event_type, session_id, directory, context_json, created_at, next_attempt_at, status)
       VALUES (?, ?, ?, ?, ?, ?, 0, 'pending')`,
      [eventId, eventType, sessionId, directory, contextJson, Date.now()],
    );
  } catch (err) {
    log("error", `hermes enqueue failed: ${err}`);
  }
}

// Atomically claim the next ready Hermes row. Same UPDATE...RETURNING pattern.
function claimNextHermesRow(): HermesQueueRow | null {
  if (!db) return null;
  try {
    const now = Date.now();
    // Recover stale locks first.
    db.run(
      `UPDATE hermes_queue SET status = 'pending'
       WHERE status = 'sending' AND next_attempt_at < ?`,
      [now - LOCK_TIMEOUT_MS],
    );
    // Atomic claim: mark as 'sending' and return the row.
    const row = db
      .query(
        `UPDATE hermes_queue
         SET status = 'sending', next_attempt_at = ?
         WHERE id = (
           SELECT id FROM hermes_queue
           WHERE status = 'pending' AND next_attempt_at <= ?
           ORDER BY id LIMIT 1
         )
         RETURNING id, event_id, event_type, session_id, directory, context_json, created_at, attempts, next_attempt_at, status`,
      )
      .get(now + LOCK_TIMEOUT_MS, now) as HermesQueueRow | undefined;
    return row ?? null;
  } catch (err) {
    log("error", `hermes claim failed: ${err}`);
    return null;
  }
}

function completeHermesRow(id: number, eventId: string): void {
  if (!db) return;
  try {
    // Transactional: delete from hermes_queue + mark dedup atomically.
    db.run("BEGIN");
    db.run("DELETE FROM hermes_queue WHERE id = ?", [id]);
    db.run(
      "INSERT OR IGNORE INTO dedup (event_id, created_at) VALUES (?, ?)",
      [eventId, Date.now()],
    );
    db.run("COMMIT");
  } catch (err) {
    log("error", `hermes complete failed: ${err}`);
    try {
      db?.run("ROLLBACK");
    } catch {
      /* ignore rollback errors */
    }
  }
}

function releaseHermesRow(id: number, nextAttemptAt: number): void {
  if (!db) return;
  try {
    db.run(
      "UPDATE hermes_queue SET status = 'pending', next_attempt_at = ? WHERE id = ?",
      [nextAttemptAt, id],
    );
  } catch (err) {
    log("error", `hermes release failed: ${err}`);
  }
}

function rescheduleHermesRow(id: number, attempts: number): void {
  if (!db) return;
  try {
    const delayIdx = Math.min(attempts, RETRY_DELAYS.length - 1);
    const delay = RETRY_DELAYS[delayIdx];
    db.run(
      `UPDATE hermes_queue
       SET status = 'pending', attempts = ?, next_attempt_at = ?
       WHERE id = ?`,
      [attempts + 1, Date.now() + delay, id],
    );
  } catch (err) {
    log("error", `hermes reschedule failed: ${err}`);
  }
}

function dropHermesRow(id: number): void {
  if (!db) return;
  try {
    db.run("DELETE FROM hermes_queue WHERE id = ?", [id]);
  } catch (err) {
    log("error", `hermes drop failed: ${err}`);
  }
}

// ── Idle debouncing ─────────────────────────────────────────────────────────
// Prevents Hermes spam when a session rapidly cycles busy↔idle.
// Two guards: a minimum interval between notifications, and a debounce window
// after the last "busy" state (so we don't notify on micro-idles).

function shouldNotifyIdle(sessionId: string): boolean {
  if (!db) return true;
  try {
    const now = Date.now();
    const row = db
      .query(
        "SELECT last_notified_at, last_busy_at FROM idle_debounce WHERE session_id = ?",
      )
      .get(sessionId) as
      | { last_notified_at: number; last_busy_at: number }
      | undefined;
    if (!row) return true; // first time
    // Don't notify if we notified recently (10 min per session).
    if (now - row.last_notified_at < IDLE_MIN_INTERVAL_MS) return false;
    return true;
  } catch {
    return true; // fail open
  }
}

function markIdleNotified(sessionId: string): void {
  if (!db) return;
  try {
    const now = Date.now();
    db.run(
      "INSERT OR REPLACE INTO idle_debounce (session_id, last_notified_at, last_busy_at) VALUES (?, ?, ?)",
      [sessionId, now, now],
    );
  } catch (err) {
    log("error", `idle debounce mark failed: ${err}`);
  }
}

function markSessionBusy(sessionId: string): void {
  if (!db) return;
  try {
    const now = Date.now();
    const existing = db
      .query("SELECT 1 FROM idle_debounce WHERE session_id = ?")
      .get(sessionId);
    if (existing) {
      db.run(
        "UPDATE idle_debounce SET last_busy_at = ? WHERE session_id = ?",
        [now, sessionId],
      );
    }
  } catch {
    /* non-critical */
  }
}

function cleanupExpired(): void {
  if (!db) return;
  try {
    const cutoff = Date.now() - DEDUP_TTL_MS;
    db.run("DELETE FROM dedup WHERE created_at < ?", [cutoff]);
  } catch {
    /* non-critical */
  }
}

// ── Rate limiting (SQLite-persisted, process-safe, rolling hour window) ─────

function canSend(): boolean {
  if (!db) return true; // fail open if no DB
  try {
    const hourAgo = Date.now() - 60 * 60 * 1000;
    const row = db
      .query("SELECT COUNT(*) as c FROM sent_log WHERE sent_at > ?")
      .get(hourAgo) as { c: number };
    return row.c < MAX_PER_HOUR;
  } catch (err) {
    log("error", `rate limit check failed: ${err}`);
    return true; // fail open
  }
}

function recordSend(): void {
  if (!db) return;
  try {
    db.run("INSERT INTO sent_log (sent_at) VALUES (?)", [Date.now()]);
  } catch (err) {
    log("error", `sent_log insert failed: ${err}`);
  }
}

function cleanupSentLog(): void {
  if (!db) return;
  try {
    const cutoff = Date.now() - 2 * 60 * 60 * 1000; // keep 2h for safety
    db.run("DELETE FROM sent_log WHERE sent_at < ?", [cutoff]);
  } catch {
    /* non-critical */
  }
}

// ── Message delivery (WhatsApp or Telegram) ─────────────────────────────────

async function sendMessage(message: string): Promise<boolean> {
  const platform = RELAY_PLATFORM.toLowerCase();
  if (platform === "telegram") {
    return sendTelegram(message);
  }
  return sendWhatsApp(message);
}

async function sendWhatsApp(message: string): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), SEND_TIMEOUT_MS);

    try {
      const res = await fetch(`${BRIDGE_URL}/send`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ chatId: CHAT_ID, message }),
        signal: controller.signal,
      });
      if (!res.ok) {
        log("error", `WhatsApp bridge returned ${res.status}`);
        return false;
      }
      return true;
    } finally {
      clearTimeout(timeout);
    }
  } catch (err) {
    log("error", `WhatsApp send failed: ${err}`);
    return false;
  }
}

async function sendTelegram(message: string): Promise<boolean> {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    log("error", "Telegram bot token or chat ID not configured");
    return false;
  }

  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;

  const sendWithMode = async (parseMode?: string): Promise<Response> => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), SEND_TIMEOUT_MS);
    const body: Record<string, string> = {
      chat_id: TELEGRAM_CHAT_ID,
      text: message,
    };
    if (parseMode) {
      body.parse_mode = parseMode;
    }

    try {
      return await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeout);
    }
  };

  try {
    let res = await sendWithMode("Markdown");
    if (!res.ok && res.status === 400) {
      let data: { description?: string } | undefined;
      try {
        data = await res.json();
      } catch {
        /* ignore */
      }
      if (data?.description?.toLowerCase().includes("markdown")) {
        res = await sendWithMode();
      }
    }
    if (!res.ok) {
      const text = await res.text().catch(() => String(res.status));
      log("error", `Telegram API returned ${res.status}: ${text}`);
      return false;
    }
    return true;
  } catch (err) {
    log("error", `Telegram send failed: ${err}`);
    return false;
  }
}

// ── Hermes orchestrator delivery (Phase 3A: one-way summaries) ──────────────
// Two-path architecture:
//   • session.error  → direct WhatsApp queue (reliable fallback, always works)
//   • session.idle / permission.asked → Hermes queue (orchestrator decides
//     whether/how to notify Ari, using its own skills + rate limiting)
//
// The Hermes path reads session context (last N messages) so the orchestrator
// has enough information to write a useful summary. Context reading happens in
// a detached async operation so the event hook stays fast.

async function readSessionContext(
  client: PluginInput["client"],
  sessionID: string,
): Promise<string> {
  try {
    if (!client?.session?.messages) return "";
    const res = await client.session.messages({ sessionID });
    const messages = res.data ?? [];
    // Take last N messages, extract text parts.
    const recent = messages.slice(-MAX_CONTEXT_MESSAGES);
    const lines: string[] = [];
    for (const { info, parts } of recent) {
      const texts = parts
        .filter((p) => p.type === "text" && p.text)
        .map((p) => p.text as string);
      if (texts.length > 0) {
        lines.push(`${info.role}: ${texts.join(" ")}`);
      }
    }
    return truncate(lines.join("\n"), 3000); // bound context size
  } catch (err) {
    log("error", `failed to read session context: ${err}`);
    return "";
  }
}

function buildHermesInput(
  eventType: string,
  sessionID: string,
  directory: string,
  details: string,
  token: string,
  sessionTitle: string,
): string {
  const dir = shortDir(directory);
  const title = sessionTitle || "untitled session";
  const agentText = sessionLastAssistantText.get(sessionID) ?? "";
  const detailBlock = details ? `\n\nDetails:\n${details}` : "";

  if (eventType === "session.idle") {
    return `opencode session "${title}" (${token}, dir: ${dir}) finished its turn.\n\nAgent's final message:\n${agentText || "(no message captured)"}`;
  }
  if (eventType === "permission.asked") {
    return `opencode session "${title}" (${token}, dir: ${dir}) needs permission to continue.\n\nAgent's last message:\n${agentText || "(no message captured)"}${detailBlock}`;
  }
  if (eventType === "question.asked") {
    return `opencode session "${title}" (${token}, dir: ${dir}) is asking a question and is blocked.\n\nAgent's last message:\n${agentText || "(no message captured)"}${detailBlock}`;
  }
  return `opencode session "${title}" (${token}, dir: ${dir}) event: ${eventType}\n\n${agentText || "(no message)"}${detailBlock}`;
}

async function postToHermes(
  eventType: string,
  sessionID: string,
  directory: string,
  context: string,
): Promise<boolean> {
  try {
    let token = shortId(sessionID);
    const sessionTitle = sessionTitles.get(sessionID) ?? "";
    let details = "";
    try {
      const parsed = context ? JSON.parse(context) : {};
      if (parsed && typeof parsed === "object") {
        if (typeof parsed.token === "string" && parsed.token) token = parsed.token;
        if (typeof parsed.details === "string") details = parsed.details;
      }
    } catch {
      details = context;
    }
    const input_text = buildHermesInput(
      eventType,
      sessionID,
      directory,
      details,
      token,
      sessionTitle,
    );

    // Design the exact WhatsApp message format. Hermes fills in the summary
    // line(s), and the plugin wraps it with the header + reply hint.
    const emoji =
      eventType === "session.idle" ? "\u{1F7E2}" :
      eventType === "permission.asked" ? "\u{1F7E1}" :
      eventType === "question.asked" ? "\u{2753}" :
      "\u{1F534}";

    const label =
      eventType === "session.idle" ? "done" :
      eventType === "permission.asked" ? "needs approval" :
      eventType === "question.asked" ? "is asking" :
      "error";

    const replyHint =
      eventType === "session.idle" ? `reply: /say ${token} <message>` :
      eventType === "permission.asked" ? `reply: /ok ${token} or /no ${token} [reason]` :
      eventType === "question.asked" ? `reply: /say ${token} <answer>` :
      "";

    const titleLine = sessionTitle || "session";

    const instructions = `You write concise WhatsApp summaries of opencode agent messages.

You receive the agent's final message. Summarize what it did or found in 1-2 sentences max.

OUTPUT RULES:
- Output ONLY the summary. Nothing else.
- No emoji, headers, titles, tokens, or reply hints. The plugin adds those.
- Do NOT repeat the session name or event type.
- Do NOT use em dashes. Use commas or periods.
- Be specific: mention files changed, commands run, tests passed/failed, decisions made.
- If the agent message is short (like "Hi!"), just say what happened briefly.
- Maximum 2 sentences. Cut filler.

EXAMPLES:
Agent said: "I fixed the JWT validation bug in lib/auth.ts and added tests. All passing."
Output: Fixed JWT validation in lib/auth.ts, added tests, all passing.

Agent said: "Hi!"
Output: Agent greeted the user and finished.

Agent said: "I need to run git push to deploy. Should I proceed?"
Output: Wants to run git push to deploy. Needs approval.`;

    // Use synchronous /v1/chat/completions instead of async /v1/runs.
    // This gives us the summary immediately, then we send it to WhatsApp
    // directly via the bridge (bypassing the send_message tool which may
    // not be available in the API server context).
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    try {
      const res = await fetch(`${HERMES_API_URL}/v1/chat/completions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${HERMES_API_KEY}`,
        },
        body: JSON.stringify({
          model: "hermes-agent",
          messages: [
            { role: "system", content: instructions },
            { role: "user", content: input_text },
          ],
          profile: HERMES_PROFILE,
        }),
        signal: controller.signal,
      });
      if (!res.ok) {
        log("error", `Hermes API returned ${res.status}`);
        return false;
      }
      const data = await res.json();
      const summary =
        data?.choices?.[0]?.message?.content ?? "opencode event (no summary)";

      // Build the final WhatsApp message with header + summary + reply hint.
      const message = `${emoji} opencode ${label}\n*${titleLine}*\n${summary}\n${replyHint}`;

      // Send the Hermes-generated summary to the configured messaging platform.
      const sent = await sendMessage(message);
      if (sent) {
        recordSend();
        log("debug", `Hermes summary sent to WhatsApp: ${message.slice(0, 80)}`);
      }
      return sent;
    } finally {
      clearTimeout(timeout);
    }
  } catch (err) {
    log("error", `Hermes POST failed: ${err}`);
    return false;
  }
}

// ── Drain loop ──────────────────────────────────────────────────────────────

let drainTimer: ReturnType<typeof setInterval> | null = null;
let draining = false;

async function drainOnce(): Promise<void> {
  const row = claimNextRow();
  if (!row) return;

  // Max attempts exceeded — drop poison messages.
  if (row.attempts >= MAX_ATTEMPTS) {
    log(
      "error",
      `dropping alert after ${MAX_ATTEMPTS} attempts: ${row.message.slice(0, 80)}`,
    );
    dropRow(row.id);
    return;
  }

  // Rate limit: defer (don't suppress). Release the row for next cycle.
  if (!canSend()) {
    releaseRow(row.id, Date.now()); // try again next cycle
    return;
  }

  const ok = await sendMessage(row.message);
  if (ok) {
    recordSend();
    completeRow(row.id, row.event_id);
    log("debug", "notification sent");
  } else {
    rescheduleRow(row.id, row.attempts);
  }
}

function startDrainLoop(): void {
  if (drainTimer) return;
  drainTimer = setInterval(async () => {
    if (draining) return; // prevent overlapping drain within this process
    draining = true;
    try {
      await drainOnce();
      cleanupExpired();
      cleanupSentLog();
    } catch (err) {
      log("error", `drain loop error: ${err}`);
    } finally {
      draining = false;
    }
  }, DRAIN_INTERVAL_MS);

  if (drainTimer && typeof drainTimer.unref === "function") {
    drainTimer.unref();
  }
}

// ── Hermes drain loop (separate from the WhatsApp drain) ───────────────────
// Drains hermes_queue → postToHermes(). No rate limiting here (Hermes handles
// its own throttling via skills), but keeps atomic claiming + retry/backoff.

let hermesDrainTimer: ReturnType<typeof setInterval> | null = null;
let hermesDraining = false;

async function hermesDrainOnce(): Promise<void> {
  const row = claimNextHermesRow();
  if (!row) return;

  if (row.attempts >= MAX_ATTEMPTS) {
    log(
      "error",
      `dropping Hermes alert after ${MAX_ATTEMPTS} attempts: ${row.event_type}`,
    );
    dropHermesRow(row.id);
    return;
  }

  // No rate limiting for Hermes (Hermes handles its own rate limiting).
  const ok = await postToHermes(
    row.event_type,
    row.session_id,
    row.directory,
    row.context_json,
  );
  if (ok) {
    completeHermesRow(row.id, row.event_id);
    log("debug", "Hermes notification sent");
  } else {
    rescheduleHermesRow(row.id, row.attempts);
  }
}

function startHermesDrainLoop(): void {
  if (hermesDrainTimer) return;
  hermesDrainTimer = setInterval(async () => {
    if (hermesDraining) return;
    hermesDraining = true;
    try {
      await hermesDrainOnce();
    } catch (err) {
      log("error", `Hermes drain loop error: ${err}`);
    } finally {
      hermesDraining = false;
    }
  }, DRAIN_INTERVAL_MS);
  if (hermesDrainTimer && typeof hermesDrainTimer.unref === "function") {
    hermesDrainTimer.unref();
  }
}

// ── Command execution (Phase 3B: bidirectional) ─────────────────────────────
// The bridge script (/usr/local/bin/opencode_bridge.py) writes commands to the
// `commands` table. This drain loop polls that table, claims the next pending
// command atomically, looks up the correlation to recover the session ID +
// request ID, and executes the command via the stored plugin client.
//
// Supported actions:
//   approve   → permission.reply (reply: "once")
//   reject    → permission.reply (reply: "reject", message)
//   answer    → question.reply (answers: [[answer]])
//   continue  → session.prompt (sends a new message to the session)
//
// Correlations are created when enqueuing permission.asked / question.asked /
// session.idle events to Hermes, so the token in the Hermes notification
// matches a row here.

const CORRELATION_TTL_MS = 30 * 60 * 1000; // 30 minutes

interface CommandRow {
  id: number;
  token: string;
  action: string;
  payload: string;
  created_at: number;
}

function createCorrelation(
  token: string,
  sessionId: string,
  directory: string,
  eventType: string,
  requestId: string | null,
): void {
  if (!db) return;
  try {
    const now = Date.now();
    // INSERT OR IGNORE: don't overwrite existing correlations.
    // Each event gets a unique token (derived from event.id), so collisions
    // only happen if the same event is processed twice (dedup handles that).
    db.run(
      `INSERT OR IGNORE INTO correlations
       (token, opencode_session_id, directory, event_type, request_id, created_at, expires_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [token, sessionId, directory, eventType, requestId, now, now + CORRELATION_TTL_MS],
    );
  } catch (err) {
    log("error", `correlation creation failed: ${err}`);
  }
}

// Module-level variable to store this plugin instance's directory.
// Used for ownership-safe command claiming.
let pluginDirectory: string = "";

function claimNextCommand(): CommandRow | null {
  if (!db) return null;
  try {
    const now = Date.now();
    // Expire old commands first.
    db.run("DELETE FROM commands WHERE expires_at < ? AND status = 'pending'", [now]);
    // Recover stale claims: commands claimed but not completed within 60s.
    db.run(
      "UPDATE commands SET status = 'pending', claimed_at = NULL WHERE status = 'claimed' AND claimed_at < ?",
      [now - 60_000],
    );
    // Atomic claim: only claim commands whose correlation belongs to THIS
    // plugin instance (matched by directory). This prevents wrong-process
    // execution in multi-TUI setups.
    const row = db
      .query(
        `UPDATE commands
         SET status = 'claimed', claimed_at = ?
         WHERE id = (
           SELECT c.id FROM commands c
           JOIN correlations corr ON corr.token = c.token
           WHERE c.status = 'pending'
             AND corr.directory = ?
             AND corr.expires_at > ?
           ORDER BY c.id LIMIT 1
         )
         RETURNING id, token, action, payload, created_at`,
      )
      .get(now, pluginDirectory, now) as CommandRow | undefined;
    return row ?? null;
  } catch (err) {
    log("error", `command claim failed: ${err}`);
    return null;
  }
}

async function executeCommand(
  row: CommandRow,
): Promise<{ ok: boolean; result: string }> {
  try {
    // Look up the correlation to get session ID and request ID.
    if (!db) return { ok: false, result: "no database" };
    const corr = db.query("SELECT * FROM correlations WHERE token = ?").get(
      row.token,
    ) as {
      opencode_session_id: string;
      directory: string;
      event_type: string;
      request_id: string | null;
    } | undefined;
    if (!corr) {
      return { ok: false, result: `no correlation for token ${row.token}` };
    }

    const payload = JSON.parse(row.payload);
    const sessionId = corr.opencode_session_id;
    const requestId = corr.request_id;

    // Verify this plugin instance owns the session.
    // Try to get the session — if it fails, requeue (don't fail) in case
    // this is a transient error or another process should handle it.
    if (pluginClient?.session?.get) {
      try {
        await pluginClient.session.get({ sessionID: sessionId });
      } catch {
        // Requeue: release back to pending so another process can try.
        if (db) {
          try {
            db.run(
              "UPDATE commands SET status = 'pending', claimed_at = NULL WHERE id = ?",
              [row.id],
            );
          } catch {
            /* ignore */
          }
        }
        return {
          ok: false,
          result: `session ${sessionId} not accessible, requeued`,
        };
      }
    }

    switch (row.action) {
      case "approve": {
        if (!pluginClient?.permission?.reply || !requestId) {
          return {
            ok: false,
            result: "permission.reply not available or no request_id",
          };
        }
        await pluginClient.permission.reply({
          requestID: requestId,
          reply: payload.reply || "once",
        });
        return { ok: true, result: "permission approved" };
      }

      case "reject": {
        if (!pluginClient?.permission?.reply || !requestId) {
          return {
            ok: false,
            result: "permission.reply not available or no request_id",
          };
        }
        await pluginClient.permission.reply({
          requestID: requestId,
          reply: "reject",
          message: payload.message || "",
        });
        return { ok: true, result: "permission rejected" };
      }

      case "answer": {
        // Question reply: try client.question.reply, fall back to fetch.
        if (!requestId) {
          return { ok: false, result: "no request_id for question" };
        }
        const answer = payload.answer || "";
        // Try the SDK client first.
        if (pluginClient?.question?.reply) {
          await pluginClient.question.reply({
            requestID: requestId,
            answers: [[answer]],
          });
          return { ok: true, result: `question answered: ${answer}` };
        }
        // Fallback: use fetch to the in-process server.
        // In TUI mode, input.client uses in-process fetch, but we don't have
        // direct access to it. Log a warning — this needs the SDK to expose
        // question.reply or the server URL.
        log("error", "question.reply not available in SDK client");
        return { ok: false, result: "question.reply not available in SDK" };
      }

      case "continue":
      case "say": {
        if (!pluginClient?.session?.promptAsync && !pluginClient?.session?.prompt) {
          return { ok: false, result: "session.promptAsync not available" };
        }
        const message = payload.message || "";
        if (pluginClient.session.promptAsync) {
          await pluginClient.session.promptAsync({
            sessionID: sessionId,
            parts: [{ type: "text", text: message }],
          });
        } else {
          await pluginClient.session.prompt!({
            sessionID: sessionId,
            parts: [{ type: "text", text: message }],
          });
        }
        return { ok: true, result: `session continued with: ${message}` };
      }

      case "skip":
        return { ok: true, result: "notification skipped, no action taken" };

      default:
        return { ok: false, result: `unknown action: ${row.action}` };
    }
  } catch (err) {
    return { ok: false, result: String(err) };
  }
}

function completeCommand(id: number, result: string): void {
  if (!db) return;
  try {
    db.run(
      "UPDATE commands SET status = 'done', result = ? WHERE id = ?",
      [result, id],
    );
  } catch (err) {
    log("error", `command complete failed: ${err}`);
  }
}

function failCommand(id: number, error: string): void {
  if (!db) return;
  try {
    db.run(
      "UPDATE commands SET status = 'failed', result = ? WHERE id = ?",
      [error, id],
    );
  } catch (err) {
    log("error", `command fail failed: ${err}`);
  }
}

// ── Command drain loop (separate from WhatsApp and Hermes drains) ───────────

let commandDrainTimer: ReturnType<typeof setInterval> | null = null;
let commandDraining = false;

async function commandDrainOnce(): Promise<void> {
  const row = claimNextCommand();
  if (!row) return;

  const result = await executeCommand(row);
  if (result.ok) {
    completeCommand(row.id, result.result);
    log("debug", `command ${row.action} executed: ${result.result}`);
  } else {
    failCommand(row.id, result.result);
    log("error", `command ${row.action} failed: ${result.result}`);
  }
}

function startCommandDrainLoop(): void {
  if (!COMMANDS_ENABLED) return;
  if (commandDrainTimer) return;
  commandDrainTimer = setInterval(async () => {
    if (commandDraining) return;
    commandDraining = true;
    try {
      await commandDrainOnce();
    } catch (err) {
      log("error", `command drain loop error: ${err}`);
    } finally {
      commandDraining = false;
    }
  }, DRAIN_INTERVAL_MS);
  if (commandDrainTimer && typeof commandDrainTimer.unref === "function") {
    commandDrainTimer.unref();
  }
}

// ── Plugin entrypoint (V1) ──────────────────────────────────────────────────

export default {
  id: "opencode-hermes-commands",
  server: async (input: PluginInput): Promise<Hooks> => {
    try {
      appLog = input.client?.app?.log ?? null;
    } catch {
      appLog = null;
    }
    pluginClient = input.client;
    pluginDirectory = input.directory;

    if (!ENABLED) {
      return {};
    }

    try {
      initStore();
      startDrainLoop();
      startHermesDrainLoop();
      startCommandDrainLoop();
      log(
        "debug",
        `relay active — bridge=${BRIDGE_URL} chat=${CHAT_ID} db=${DB_PATH} hermes=${HERMES_API_URL}`,
      );
    } catch (err) {
      log("error", `failed to initialize store/drain: ${err}`);
    }

    return {
      event: async ({ event }: { event: OpencodeEvent }): Promise<void> => {
        try {
          if (event.type === "session.deleted") {
            const sid = String(event.properties?.sessionID ?? "");
            if (sid) {
              sessionTitles.delete(sid);
              subagentSessions.delete(sid);
              clearSessionTracking(sid);
            }
            return;
          }

          // Track session titles from created/updated events.
          if (event.type === "session.created" || event.type === "session.updated") {
            const sid = String(event.properties?.sessionID ?? "");
            const info = (event.properties as any)?.info;
            if (sid) {
              if (info?.parentID) subagentSessions.add(sid);
              else subagentSessions.delete(sid);
            }
            if (sid && info?.title) {
              sessionTitles.set(sid, info.title);
            }
            if (sid && info?.slug && !sessionTitles.has(sid)) {
              sessionTitles.set(sid, info.slug);
            }
            return;
          }

          if (event.type === "message.updated") {
            const info = (event.properties as any)?.info;
            const sid = String(event.properties?.sessionID ?? info?.sessionID ?? "");
            const messageId = String(info?.id ?? "");
            const role = String(info?.role ?? "");
            if (messageId && role) {
              messageRoles.set(messageId, role);
            }
            if (sid && role === "user") {
              clearSessionTracking(sid);
            }
            return;
          }

          // Track last assistant message text from message.part.updated.
          // This captures the agent's final response so Hermes can summarize it.
          if (event.type === "message.part.updated") {
            const part = (event.properties as any)?.part;
            const sid = String(event.properties?.sessionID ?? "");
            const messageId = String(part?.messageID ?? "");
            const role = messageRoles.get(messageId) ?? "";
            if (
              sid &&
              role === "assistant" &&
              part?.type === "text" &&
              typeof part.text === "string" &&
              part.text.length > 0
            ) {
              sessionLastAssistantText.set(sid, part.text);
            }
            return;
          }

          // Track busy state for idle debouncing (fast SQLite update).
          if (
            event.type === "session.status" &&
            (event.properties as any)?.status?.type === "busy"
          ) {
            const sid = String(event.properties?.sessionID ?? "");
            if (sid) {
              clearSessionTracking(sid);
              markSessionBusy(sid);
            }
            return;
          }

          // session.error → direct WhatsApp (reliable fallback).
          if (event.type === "session.error") {
            if (!shouldNotifySession(String(event.properties?.sessionID ?? ""))) {
              return;
            }
            if (isBenignSessionError(event.properties ?? {})) {
              return;
            }
            if (isDuplicate(event.id)) {
              log("debug", `skipping duplicate event ${event.id}`);
              return;
            }
            const message = formatMessage(event, input);
            enqueue(event.id, message);
            return;
          }

          // session.status(idle) → Hermes (with debouncing).
          if (
            event.type === "session.status" &&
            (event.properties as any)?.status?.type === "idle"
          ) {
            const sid = String(event.properties?.sessionID ?? "");
            if (!sid) return;
            if (!shouldNotifySession(sid)) return;
            if (isDuplicate(event.id)) return;
            if (!shouldNotifyIdle(sid)) return;
            markIdleNotified(sid);

            const token = shortId(sid);
            createCorrelation(token, sid, input.directory, "session.idle", null);
            enqueueHermes(
              event.id,
              "session.idle",
              sid,
              input.directory,
              JSON.stringify({ token }),
            );
            return;
          }

          // permission.asked → Hermes.
          if (event.type === "permission.asked") {
            const sid = String(event.properties?.sessionID ?? "");
            if (!sid) return;
            if (!shouldNotifySession(sid)) return;
            if (isDuplicate(event.id)) return;

            const permType = String(
              (event.properties as any)?.permission ?? "unknown",
            );
            const patterns = JSON.stringify(
              (event.properties as any)?.patterns ?? [],
            );
            const requestId = String((event.properties as any)?.id ?? "");

            const token = shortId(event.id);
            createCorrelation(token, sid, input.directory, "permission.asked", requestId);
            enqueueHermes(
              event.id,
              "permission.asked",
              sid,
              input.directory,
              JSON.stringify({
                token,
                details: `Permission requested: ${permType}\nPatterns: ${patterns}`,
              }),
            );
            return;
          }

          // question.asked → Hermes (agent is blocked waiting for an answer).
          if (event.type === "question.asked") {
            const sid = String(event.properties?.sessionID ?? "");
            if (!sid) return;
            if (!shouldNotifySession(sid)) return;
            if (isDuplicate(event.id)) return;

            const questions = (event.properties as any)?.questions ?? [];
            const requestId = String((event.properties as any)?.id ?? "");

            const questionLines: string[] = [];
            for (const q of questions) {
              questionLines.push(`Q: ${q.question ?? "?"}`);
              if (q.header) questionLines.push(`  (${q.header})`);
              if (q.options && q.options.length > 0) {
                const opts = q.options
                  .map((o: any) => `  - ${o.label}: ${o.description ?? ""}`)
                  .join("\n");
                questionLines.push(opts);
              }
              if (q.custom) questionLines.push("  (custom answer allowed)");
            }
            const token = shortId(event.id);
            createCorrelation(token, sid, input.directory, "question.asked", requestId);
            enqueueHermes(
              event.id,
              "question.asked",
              sid,
              input.directory,
              JSON.stringify({ token, details: questionLines.join("\n") }),
            );
            return;
          }
        } catch (err) {
          log("error", `event handler error: ${err}`);
        }
      },
    };
  },
};
