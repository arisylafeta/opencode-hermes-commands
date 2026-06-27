// @bun
import { appendFileSync, mkdirSync } from "node:fs";
import { spawn } from "node:child_process";
import path from "node:path";
import { Database } from "bun:sqlite";

const DB_PATH = process.env.HERMES_RELAY_DB_PATH ??
  `${process.env.HOME ?? "/root"}/.hermes/plugins/opencode-hermes-commands/state.db`;

let pluginClient = null;
let pluginDirectory = "";
let db = null;
let commandDrainTimer = null;
let commandDraining = false;
const COMMAND_DRAIN_INTERVAL_MS = 5000;
const COMMAND_TTL_MS = 300_000;
const SYSTEM_COMMAND_TOKEN = "__global__";

function initStore() {
  if (db) return;
  try {
    const dir = path.dirname(DB_PATH);
    mkdirSync(dir, { recursive: true });
    db = new Database(DB_PATH);
    db.run("PRAGMA journal_mode = WAL;");
    db.run("PRAGMA busy_timeout = 3000;");
    db.run(`
      CREATE TABLE IF NOT EXISTS sessions (
        session_id TEXT PRIMARY KEY,
        short_id INTEGER UNIQUE NOT NULL,
        directory TEXT NOT NULL,
        title TEXT,
        status TEXT,
        parent_id TEXT,
        is_child INTEGER NOT NULL DEFAULT 0,
        deleted INTEGER NOT NULL DEFAULT 0,
        last_assistant_text TEXT,
        last_activity_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_sessions_short_id ON sessions(short_id);
      CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions(deleted, is_child, last_activity_at);

      CREATE TABLE IF NOT EXISTS commands (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_kind TEXT NOT NULL DEFAULT 'correlation',
        token TEXT NOT NULL,
        action TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        claimed_at INTEGER,
        result TEXT,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_commands_pending ON commands(status, target_kind, token);

      CREATE TABLE IF NOT EXISTS correlations (
        token TEXT PRIMARY KEY,
        opencode_session_id TEXT NOT NULL,
        directory TEXT NOT NULL,
        event_type TEXT NOT NULL,
        request_id TEXT,
        details TEXT,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
      );
    `);
    try {
      db.prepare("SELECT target_kind FROM commands LIMIT 0").all();
    } catch {
      db.run("ALTER TABLE commands ADD COLUMN target_kind TEXT NOT NULL DEFAULT 'correlation'");
    }
    try {
      db.prepare("SELECT details FROM correlations LIMIT 0").all();
    } catch {
      db.run("ALTER TABLE correlations ADD COLUMN details TEXT");
    }
  } catch (err) {
    logPluginError("initStore", err);
    throw err;
  }
}

function ensureDbSession(sessionId, directory, title, status, isChild, parentId) {
  const db2 = getDb();
  if (!db2 || !directory) return;
  const now = Date.now();
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      db2.prepare(
        `INSERT OR IGNORE INTO sessions
         (session_id, short_id, directory, title, status, is_child, parent_id, created_at, updated_at, last_activity_at)
         VALUES (?, (SELECT COALESCE(MAX(short_id), 0) + 1 FROM sessions), ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(sessionId, directory, title ?? null, status ?? "unknown", isChild ? 1 : 0, parentId ?? null, now, now, now);
      db2.prepare(
        `UPDATE sessions SET directory = ?, title = COALESCE(?, title), status = COALESCE(?, status),
         is_child = ?, parent_id = COALESCE(?, parent_id),
         updated_at = ?, last_activity_at = ? WHERE session_id = ?`
      ).run(directory, title ?? null, status ?? null, isChild ? 1 : 0, parentId ?? null, now, now, sessionId);
      return;
    } catch (err) {
      if (attempt < 2) continue;
      logPluginError("ensureDbSession", err);
    }
  }
}

function updateDbSessionTitleStatus(sessionId, title, status) {
  if (!db) return;
  const now = Date.now();
  try {
    const stmt = db.prepare(`
      UPDATE sessions SET
        title = COALESCE(?, title),
        status = COALESCE(?, status),
        updated_at = ?,
        last_activity_at = ?
      WHERE session_id = ?
    `);
    stmt.run(title ?? null, status ?? null, now, now, sessionId);
  } catch (err) {
    logPluginError("updateDbSessionTitleStatus", err);
  }
}

function updateDbSessionStatus(sessionId, status) {
  if (!db) return;
  const now = Date.now();
  try {
    const stmt = db.prepare(`
      UPDATE sessions SET status = ?, updated_at = ?, last_activity_at = ? WHERE session_id = ?
    `);
    stmt.run(status, now, now, sessionId);
  } catch (err) {
    logPluginError("updateDbSessionStatus", err);
  }
}

function updateDbLastAssistantText(sessionId, text) {
  if (!db) return;
  const now = Date.now();
  try {
    const stmt = db.prepare(`
      UPDATE sessions SET last_assistant_text = ?, last_activity_at = ?, updated_at = ? WHERE session_id = ?
    `);
    stmt.run(text, now, now, sessionId);
  } catch (err) {
    logPluginError("updateDbLastAssistantText", err);
  }
}

function markDbSessionDeleted(sessionId) {
  if (!db) return;
  const now = Date.now();
  try {
    const stmt = db.prepare(`UPDATE sessions SET deleted = 1, updated_at = ?, last_activity_at = ? WHERE session_id = ?`);
    stmt.run(now, now, sessionId);
  } catch (err) {
    logPluginError("markDbSessionDeleted", err);
  }
}

function getDbShortId(sessionId) {
  if (!db) return;
  try {
    const row = db.prepare(`SELECT short_id FROM sessions WHERE session_id = ?`).get(sessionId);
    return row?.short_id;
  } catch (err) {
    logPluginError("getDbShortId", err);
  }
}

function getDb() {
  return db;
}

function shortId(id) {
  if (typeof id !== "string") return String(id);
  return id.length > 12 ? id.slice(0, 12) : id;
}

function createCorrelation(token, sessionId, directory, eventType, requestId, details) {
  try {
    const db2 = getDb();
    if (!db2) return;
    const now = Date.now();
    db2.prepare(
      `INSERT OR IGNORE INTO correlations
       (token, opencode_session_id, directory, event_type, request_id, details, created_at, expires_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    ).run(token, sessionId, directory, eventType, requestId ?? null, details ? JSON.stringify(details) : null, now, now + COMMAND_TTL_MS);
  } catch (err) {
    logPluginError("createCorrelation", err);
  }
}

function startCommandDrainLoop() {
  if (commandDrainTimer) return;
  commandDrainTimer = setInterval(async () => {
    if (commandDraining) return;
    commandDraining = true;
    try {
      await commandDrainOnce();
    } catch (err) {
      logPluginError("commandDrain", err);
    } finally {
      commandDraining = false;
    }
  }, COMMAND_DRAIN_INTERVAL_MS);
  if (commandDrainTimer && typeof commandDrainTimer.unref === "function") {
    commandDrainTimer.unref();
  }
}

async function commandDrainOnce() {
  if (!db || !pluginClient) return;
  const now = Date.now();

  // Recover stale claims and mark expired pending commands.
  try {
    db.prepare(
      "UPDATE commands SET status = 'pending', claimed_at = NULL WHERE status = 'claimed' AND claimed_at < ?"
    ).run(now - 60_000);
    db.prepare(
      "UPDATE commands SET status = 'expired' WHERE status = 'pending' AND expires_at < ?"
    ).run(now);
  } catch (err) {
    logPluginError("commandStaleCleanup", err);
  }

  let command;
  try {
    const claim = db.prepare(`
      UPDATE commands SET status = 'claimed', claimed_at = ?
      WHERE id = (
        SELECT c.id FROM commands c
        LEFT JOIN sessions s ON c.target_kind = 'session' AND s.short_id = CAST(c.token AS INTEGER)
        LEFT JOIN correlations corr ON c.target_kind = 'correlation' AND c.token = corr.token
        WHERE c.status = 'pending' AND c.expires_at > ?
          AND (
            (c.target_kind = 'session' AND s.directory = ?)
            OR (c.target_kind = 'correlation' AND corr.directory = ?)
            OR (c.target_kind = 'system' AND (c.token = ? OR c.token = ?))
          )
        ORDER BY c.id ASC
        LIMIT 1
      )
      RETURNING *
    `);
    command = claim.get(now, now, pluginDirectory, pluginDirectory, pluginDirectory, SYSTEM_COMMAND_TOKEN);
  } catch (err) {
    logPluginError("commandClaim", err);
    return;
  }
  if (!command) return;

  let targetId = null;
  let requestId = null;
  try {
    if (command.target_kind === "session") {
      const row = db.prepare(`SELECT session_id FROM sessions WHERE short_id = ? AND directory = ?`).get(command.token, pluginDirectory);
      targetId = row?.session_id ?? null;
    } else if (command.target_kind === "system") {
      targetId = SYSTEM_COMMAND_TOKEN;
    } else {
      const row = db.prepare(`SELECT opencode_session_id, request_id FROM correlations WHERE token = ? AND directory = ?`).get(command.token, pluginDirectory);
      targetId = row?.opencode_session_id ?? null;
      requestId = row?.request_id ?? null;
    }
  } catch (err) {
    logPluginError("commandResolve", err);
    await markCommandStatus(command.id, "failed", String(err));
    return;
  }

  if (!targetId) {
    await markCommandStatus(command.id, "failed", "target not found");
    return;
  }

  if (command.target_kind !== "system" && !await sessionOwnedByCurrentClient(targetId)) {
    await releaseClaimedCommand(command.id);
    return;
  }

  let payload;
  try {
    payload = JSON.parse(command.payload ?? "{}");
  } catch {
    payload = {};
  }

  let result;
  let failed = false;
  try {
    switch (command.action) {
      case "approve": {
        await pluginClient.permission.reply({ requestID: requestId, reply: "once" });
        break;
      }
      case "reject": {
        await pluginClient.permission.reply({ requestID: requestId, reply: "reject", message: payload.message });
        break;
      }
      case "answer": {
        await pluginClient.question.reply({ requestID: requestId, answers: [[payload.answer]] });
        break;
      }
      case "continue":
      case "say": {
        if (pluginClient.session.promptAsync) {
          await pluginClient.session.promptAsync({ sessionID: targetId, parts: [{ type: "text", text: payload.message }] });
        } else {
          await pluginClient.session.prompt({ sessionID: targetId, parts: [{ type: "text", text: payload.message }] });
        }
        break;
      }
      case "new_session": {
        result = await spawnNewSession(payload);
        if (!result.ok) {
          failed = true;
        }
        break;
      }
      case "kill_sessions": {
        result = await killSessions(payload);
        if (!result.ok) {
          failed = true;
        }
        break;
      }
      case "skip": {
        break;
      }
      default: {
        failed = true;
        result = `unknown action ${command.action}`;
      }
    }
  } catch (err) {
    failed = true;
    result = err instanceof Error ? err.message : String(err);
  }

  await markCommandStatus(command.id, failed ? "failed" : "done", result !== undefined ? JSON.stringify(result) : undefined);
}

async function markCommandStatus(id, status, result) {
  if (!db) return;
  try {
    db.prepare(`UPDATE commands SET status = ?, result = ? WHERE id = ?`).run(status, result ?? null, id);
  } catch (err) {
    logPluginError("markCommandStatus", err);
  }
}
async function releaseClaimedCommand(id) {
  if (!db) return;
  try {
    db.prepare(`UPDATE commands SET status = 'pending', claimed_at = NULL WHERE id = ? AND status = 'claimed'`).run(id);
  } catch (err) {
    logPluginError("releaseClaimedCommand", err);
  }
}
async function sessionOwnedByCurrentClient(sessionId) {
  if (!pluginClient?.session?.get) {
    return true;
  }
  try {
    await pluginClient.session.get({ sessionID: sessionId });
    return true;
  } catch {
    return false;
  }
}
function safeSessionName(value) {
  return String(value || "oc-session").toLowerCase().replace(/[^a-z0-9_.-]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 48) || "oc-session";
}
function buildSpawnInput(payload) {
  const lines = [];
  if (payload.preset) {
    lines.push(`/preset ${payload.preset}`);
  }
  if (payload.prompt) {
    lines.push(String(payload.prompt).trim());
  }
  return lines.join("\n") + "\n";
}
async function spawnNewSession(payload) {
  const prompt = typeof payload?.prompt === "string" ? payload.prompt.trim() : "";
  if (!prompt) {
    return { ok: false, error: "missing prompt" };
  }
  const cwd = typeof payload?.dir === "string" && payload.dir.trim() ? payload.dir.trim() : pluginDirectory;
  const args = ["run"];
  if (typeof payload?.agent === "string" && payload.agent.trim()) {
    args.push("--agent", payload.agent.trim());
  }
  if (typeof payload?.model === "string" && payload.model.trim()) {
    args.push("--model", payload.model.trim());
  }
  const sessionName = `${safeSessionName(prompt.slice(0, 32))}-${Date.now().toString(36).slice(-6)}`;
  args.push("--dir", cwd, "--title", sessionName);
  if (payload?.preset) {
    args.push("--command", `preset ${payload.preset}`);
  }
  args.push(prompt);
  try {
    const child = spawn("opencode", args, {
      cwd,
      stdio: ["ignore", "ignore", "ignore"],
      detached: true,
      windowsHide: true,
      env: { ...process.env }
    });
    child.unref();
    return {
      ok: true,
      pid: child.pid,
      session_name: sessionName,
      cwd,
      agent: payload?.agent || null,
      model: payload?.model || null,
      preset: payload?.preset || null
    };
  } catch (err) {
    return {
      ok: false,
      error: err instanceof Error ? err.message : String(err)
    };
  }
}
function runOpencodeCommand(args, cwd) {
  return new Promise((resolve) => {
    const child = spawn("opencode", args, {
      cwd,
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
      env: { ...process.env }
    });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr?.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", (error) => {
      resolve({ ok: false, code: null, stdout, stderr, error: error instanceof Error ? error.message : String(error) });
    });
    child.on("close", (code) => {
      resolve({ ok: code === 0, code, stdout: stdout.trim(), stderr: stderr.trim() });
    });
  });
}
async function killSessions(payload) {
  const requested = Array.isArray(payload?.sessions) ? payload.sessions : [];
  if (!requested.length) {
    return { ok: false, error: "missing sessions" };
  }
  const results = [];
  let anyFailed = false;
  for (const item of requested) {
    const shortId = Number(item?.short_id);
    const sessionId = typeof item?.session_id === "string" ? item.session_id : "";
    if (!Number.isInteger(shortId) || !sessionId) {
      results.push({ short_id: item?.short_id ?? null, ok: false, error: "invalid session payload" });
      anyFailed = true;
      continue;
    }
    const response = await runOpencodeCommand(["session", "delete", sessionId], pluginDirectory);
    if (response.ok) {
      try {
        db?.prepare("UPDATE sessions SET deleted = 1, updated_at = ?, last_activity_at = ? WHERE session_id = ?").run(Date.now(), Date.now(), sessionId);
      } catch (err) {
        logPluginError("killSessions.markDeleted", err);
      }
      results.push({ short_id: shortId, session_id: sessionId, ok: true });
      continue;
    }
    anyFailed = true;
    results.push({
      short_id: shortId,
      session_id: sessionId,
      ok: false,
      error: response.stderr || response.stdout || response.error || `exit ${response.code}`
    });
  }
  return { ok: !anyFailed, results };
}

function extractLastAssistantText(messages) {
  if (!Array.isArray(messages)) return;
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg?.role !== "assistant") continue;
    const parts = Array.isArray(msg.parts) ? msg.parts : [];
    for (let j = parts.length - 1; j >= 0; j--) {
      const part = parts[j];
      if (part?.type === "text" && typeof part.text === "string") {
        return part.text;
      }
    }
  }
}

// src/config.ts
function parseBool(value, defaultValue) {
  if (value === undefined)
    return defaultValue;
  return value.toLowerCase() === "true";
}
function parseIntDefault(value, defaultValue) {
  if (value === undefined)
    return defaultValue;
  const parsed = parseInt(value, 10);
  return Number.isNaN(parsed) ? defaultValue : parsed;
}
function parseString(value, defaultValue) {
  return value ?? defaultValue;
}
function parseIntArray(value, defaultValue) {
  if (value === undefined)
    return defaultValue;
  try {
    const parsed = JSON.parse(value);
    if (Array.isArray(parsed) && parsed.every((v) => typeof v === "number")) {
      return parsed;
    }
  } catch {}
  return defaultValue;
}
function loadConfig() {
  return {
    enabled: parseBool(process.env.HERMES_RELAY_ENABLED, true),
    logLevel: parseString(process.env.HERMES_RELAY_LOG_LEVEL, "error"),
    platform: parseString(process.env.HERMES_RELAY_PLATFORM, "whatsapp"),
    whatsappBridgeUrl: parseString(process.env.HERMES_WHATSAPP_BRIDGE_URL, "http://127.0.0.1:3000"),
    whatsappChatId: parseString(process.env.HERMES_WHATSAPP_CHAT_ID, process.env.WHATSAPP_HOME_CHANNEL ?? ""),
    telegramBotToken: parseString(process.env.HERMES_TELEGRAM_BOT_TOKEN, process.env.TELEGRAM_BOT_TOKEN),
    telegramChatId: parseString(process.env.HERMES_TELEGRAM_CHAT_ID, process.env.TELEGRAM_HOME_CHANNEL),
    maxPerHour: parseIntDefault(process.env.HERMES_RELAY_MAX_PER_HOUR, 10),
    quiescenceMs: parseIntDefault(process.env.HERMES_RELAY_QUIESCENCE_MS, 300000),
    sendTimeoutMs: parseIntDefault(process.env.HERMES_RELAY_SEND_TIMEOUT_MS, 5000),
    maxAttempts: parseIntDefault(process.env.HERMES_RELAY_MAX_ATTEMPTS, 4),
    retryDelays: parseIntArray(process.env.HERMES_RELAY_RETRY_DELAYS, [30000, 120000, 300000])
  };
}
function logPluginError(scope, error, detail = "") {
  try {
    const path = process.env.HERMES_RELAY_ERROR_LOG ?? "/root/.hermes/plugins/opencode-hermes-commands/errors.log";
    const message = error instanceof Error ? `${error.name}: ${error.message}\n${error.stack ?? ""}` : String(error);
    appendFileSync(path, `[${new Date().toISOString()}] ${scope}${detail ? ` ${detail}` : ""}\n${message}\n\n`);
  } catch {}
}

// src/scheduler.ts
class Scheduler {
  config;
  sendFn;
  queue = [];
  timer = null;
  running = false;
  hourWindow = { count: 0, startAt: 0 };
  maxQueueSize = 100;
  constructor(config, sendFn) {
    this.config = config;
    this.sendFn = sendFn;
  }
  enqueue(notification) {
    try {
      if (this.queue.length >= this.maxQueueSize) {
        this.queue.shift();
      }
      this.queue.push({
        notification,
        attempts: 0,
        nextAttemptAt: Date.now()
      });
      if (this.running) {
        this.scheduleDrain();
      }
    } catch (error) {
      logPluginError("runtime.onEvent", error, event?.type);
    }
  }
  start() {
    this.running = true;
    this.scheduleDrain();
  }
  stop() {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.queue = [];
  }
  scheduleDrain() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (!this.running || this.queue.length === 0) {
      return;
    }
    const now = Date.now();
    const head = this.queue[0];
    const delay = Math.max(0, head.nextAttemptAt - now);
    this.timer = setTimeout(() => {
      this.drain().catch(() => {});
    }, delay);
    if (typeof this.timer.unref === "function") {
      this.timer.unref();
    }
  }
  async drain() {
    if (!this.running)
      return;
    this.timer = null;
    try {
      if (this.queue.length === 0)
        return;
      const now = Date.now();
      const head = this.queue[0];
      if (now < head.nextAttemptAt) {
        this.scheduleDrain();
        return;
      }
      if (this.hourWindow.startAt === 0 || now - this.hourWindow.startAt >= 3600000) {
        this.hourWindow = { count: 0, startAt: now };
      }
      if (this.hourWindow.count >= this.config.maxPerHour) {
        head.nextAttemptAt = this.hourWindow.startAt + 3600000;
        this.scheduleDrain();
        return;
      }
      let success = false;
      try {
        success = await this.sendFn(head.notification);
      } catch {
        success = false;
      }
      if (success) {
        this.hourWindow.count++;
        this.queue.shift();
      } else {
        head.attempts++;
        if (head.attempts >= this.config.maxAttempts) {
          this.queue.shift();
        } else {
          const delay = this.config.retryDelays[head.attempts - 1] ?? 30000;
          head.nextAttemptAt = now + delay;
        }
      }
    } catch {}
    this.scheduleDrain();
  }
}

// src/session-state.ts
var BENIGN_ERROR_NAMES = new Set(["MessageAbortedError", "AbortError"]);
var BENIGN_WORDS = new Set(["cancel", "undo", "rewind"]);
function isBenignError(error) {
  if (error === null || error === undefined)
    return false;
  if (typeof error === "object" && error !== null) {
    const err = error;
    const name = typeof err.name === "string" ? err.name : "";
    const message = typeof err.message === "string" ? err.message : "";
    if (BENIGN_ERROR_NAMES.has(name))
      return true;
    const words = (message + " " + name).toLowerCase().split(/[^a-z0-9]+/);
    for (const word of words) {
      if (BENIGN_WORDS.has(word))
        return true;
    }
    return false;
  }
  if (typeof error === "string") {
    const lower = error.toLowerCase();
    if (BENIGN_ERROR_NAMES.has(error))
      return true;
    const words = lower.split(/[^a-z0-9]+/);
    for (const word of words) {
      if (BENIGN_WORDS.has(word))
        return true;
    }
  }
  return false;
}
function getStringProp(props, key) {
  const value = props[key];
  if (typeof value === "string")
    return value;
  return;
}
function getNestedStringProp(props, objectKey, key) {
  const object = props[objectKey];
  if (typeof object !== "object" || object === null)
    return;
  const value = object[key];
  if (typeof value === "string")
    return value;
  return;
}
function getStatusType(props) {
  return getStringProp(props, "type") ?? getNestedStringProp(props, "status", "type");
}
function baseEventType(type) {
  if (typeof type !== "string") return "";
  return type.replace(/\.\d+$/, "");
}
function eventProperties(event) {
  if (event?.properties && typeof event.properties === "object") return event.properties;
  const data = event?.data;
  if (data && typeof data === "object") return data;
  if (typeof data === "string") {
    try {
      const parsed = JSON.parse(data);
      if (parsed && typeof parsed === "object") return parsed;
    } catch {}
  }
  return {};
}

function formatQuestionDetails(props) {
  const questions = Array.isArray(props?.questions) ? props.questions : [];
  if (!questions.length) {
    return getStringProp(props, "message") ?? "Question asked";
  }
  const lines = [];
  questions.forEach((q, index) => {
    const header = typeof q?.header === "string" && q.header ? q.header : `Question ${index + 1}`;
    const question = typeof q?.question === "string" ? q.question : "";
    lines.push(`${index + 1}. ${header}`);
    if (question) lines.push(question);
    const options = Array.isArray(q?.options) ? q.options : [];
    if (options.length) {
      lines.push("Options:");
      options.forEach((opt) => {
        const label = typeof opt?.label === "string" ? opt.label : String(opt ?? "");
        const desc = typeof opt?.description === "string" && opt.description ? ` - ${opt.description}` : "";
        lines.push(`- ${label}${desc}`);
      });
    }
    if (q?.multiple) lines.push("Multiple answers allowed.");
    if (q?.custom) lines.push("Custom answer allowed.");
    if (index < questions.length - 1) lines.push("");
  });
  return lines.join("\n");
}

function questionDetailsFromEvent(event) {
  const props = eventProperties(event);
  return {
    message: formatQuestionDetails(props),
    questions: Array.isArray(props.questions) ? props.questions : [],
    tool: props.tool ?? null
  };
}

class SessionTracker {
  sessions = new Map;
  messageRoles = new Map;
  assistantMessageTexts = new Map;
  ensureState(sessionId) {
    let state = this.sessions.get(sessionId);
    if (!state) {
      state = {
        id: sessionId,
        isChild: false,
        children: new Set,
        status: "unknown",
        lastActivityAt: Date.now(),
        lastUserMessageAt: 0,
        lastAssistantMessageAt: 0,
        lastAssistantText: "",
        pendingPermission: false,
        pendingQuestion: false,
        deleted: false,
        doneNotifiedAt: 0
      };
      this.sessions.set(sessionId, state);
    }
    return state;
  }
  trackEvent(event) {
    try {
      const props = eventProperties(event);
      const sessionId = getStringProp(props, "sessionID") ?? event.id;
      const eventType = baseEventType(event.type);
      switch (eventType) {
        case "session.created":
        case "session.updated": {
          const state = this.ensureState(sessionId);
          const parentId = getStringProp(props, "parentID") ?? getNestedStringProp(props, "info", "parentID");
          if (parentId) {
            state.parentId = parentId;
            state.isChild = true;
            const parent = this.ensureState(parentId);
            parent.children.add(sessionId);
          }
          const title = getStringProp(props, "title") ?? getStringProp(props, "slug") ?? getNestedStringProp(props, "info", "title") ?? getNestedStringProp(props, "info", "slug");
          if (title) {
            state.title = title;
          }
          state.lastActivityAt = Date.now();
          ensureDbSession(sessionId, pluginDirectory, state.title, state.status, state.isChild, parentId);
          break;
        }
        case "session.status": {
          const state = this.ensureState(sessionId);
          const statusType = getStatusType(props);
          if (statusType === "busy" || statusType === "idle") {
            state.status = statusType;
          } else {
            state.status = "unknown";
          }
          if (state.status === "busy") {
            state.doneNotifiedAt = 0;
          }
          state.lastActivityAt = Date.now();
          updateDbSessionStatus(sessionId, state.status);
          if (state.status === "busy" && state.parentId) {
            const parent = this.sessions.get(state.parentId);
            if (parent) {
              parent.doneNotifiedAt = 0;
            }
          }
          break;
        }
        case "session.deleted": {
          const state = this.sessions.get(sessionId);
          if (state) {
            if (state.lastAssistantMessageId) {
              this.assistantMessageTexts.delete(state.lastAssistantMessageId);
            }
            state.deleted = true;
            if (state.parentId) {
              const parent = this.sessions.get(state.parentId);
              if (parent) {
                parent.children.delete(sessionId);
              }
            }
            this.sessions.delete(sessionId);
          }
          markDbSessionDeleted(sessionId);
          break;
        }
        case "message.updated": {
          const state = this.ensureState(sessionId);
          const role = getStringProp(props, "role") ?? getNestedStringProp(props, "info", "role");
          const messageId = getStringProp(props, "id") ?? getNestedStringProp(props, "info", "id");
          if (messageId && role) {
            this.messageRoles.set(messageId, role);
            if (role === "assistant" && state.lastAssistantMessageId !== messageId) {
              state.lastAssistantMessageId = messageId;
              state.lastAssistantText = "";
              this.assistantMessageTexts.set(messageId, "");
            }
          }
          if (role === "user") {
            state.lastUserMessageAt = Date.now();
            state.lastActivityAt = Date.now();
            state.doneNotifiedAt = 0;
          }
          break;
        }
        case "message.part.updated": {
          const state = this.ensureState(sessionId);
          const messageId = getStringProp(props, "messageID") ?? getNestedStringProp(props, "part", "messageID");
          let role = getStringProp(props, "role") ?? getNestedStringProp(props, "info", "role");
          if (!role && messageId) {
            role = this.messageRoles.get(messageId);
          }
          const partType = getStringProp(props, "partType") ?? getStringProp(props, "type") ?? getNestedStringProp(props, "part", "type");
          if (role === "assistant" && partType === "text") {
            state.lastAssistantMessageAt = Date.now();
            state.lastActivityAt = Date.now();
            state.doneNotifiedAt = 0;
            const text = getStringProp(props, "text") ?? getNestedStringProp(props, "part", "text");
            if (text && messageId) {
              const previousText = this.assistantMessageTexts.get(messageId) ?? "";
              let nextText = text;
              if (previousText) {
                if (text.startsWith(previousText)) {
                  nextText = text;
                } else if (previousText.endsWith(text)) {
                  nextText = previousText;
                } else {
                  nextText = `${previousText}${text}`;
                }
              }
              this.assistantMessageTexts.set(messageId, nextText);
              state.lastAssistantMessageId = messageId;
              state.lastAssistantText = nextText;
              updateDbLastAssistantText(sessionId, nextText);
            } else if (text) {
              state.lastAssistantText = text;
              updateDbLastAssistantText(sessionId, text);
            }
          }
          break;
        }
        case "permission.asked": {
          const state = this.ensureState(sessionId);
          state.pendingPermission = true;
          state.lastActivityAt = Date.now();
          state.doneNotifiedAt = 0;
          break;
        }
        case "permission.replied": {
          const state = this.ensureState(sessionId);
          state.pendingPermission = false;
          state.lastActivityAt = Date.now();
          break;
        }
        case "question.asked":
        case "question.v2.asked": {
          const state = this.ensureState(sessionId);
          state.pendingQuestion = true;
          state.lastActivityAt = Date.now();
          state.doneNotifiedAt = 0;
          break;
        }
        case "question.replied":
        case "question.rejected":
        case "question.v2.replied":
        case "question.v2.rejected": {
          const state = this.ensureState(sessionId);
          state.pendingQuestion = false;
          state.lastActivityAt = Date.now();
          break;
        }
        case "session.error": {
          const state = this.ensureState(sessionId);
          state.lastActivityAt = Date.now();
          break;
        }
        default:
          break;
      }
    } catch {}
  }
  getState(sessionId) {
    return this.sessions.get(sessionId);
  }
  isChild(sessionId) {
    const state = this.sessions.get(sessionId);
    return state?.isChild ?? false;
  }
  hasActiveChildren(sessionId) {
    const state = this.sessions.get(sessionId);
    if (!state)
      return false;
    for (const childId of state.children) {
      const child = this.sessions.get(childId);
      if (child && !child.deleted && child.status === "busy") {
        return true;
      }
      if (child && !child.deleted && this.hasActiveChildren(childId)) {
        return true;
      }
    }
    return false;
  }
  hasPendingInTree(sessionId) {
    const state = this.sessions.get(sessionId);
    if (!state)
      return false;
    if (state.pendingPermission || state.pendingQuestion)
      return true;
    for (const childId of state.children) {
      const child = this.sessions.get(childId);
      if (child && !child.deleted && this.hasPendingInTree(childId)) {
        return true;
      }
    }
    return false;
  }
  getTreeLastActivity(sessionId) {
    const state = this.sessions.get(sessionId);
    if (!state)
      return 0;
    let max = state.lastActivityAt;
    for (const childId of state.children) {
      const childMax = this.getTreeLastActivity(childId);
      if (childMax > max)
        max = childMax;
    }
    return max;
  }
  isQuiescent(sessionId, now, quiescenceMs, includeChildren = false) {
    const state = this.sessions.get(sessionId);
    if (!state)
      return false;
    if (state.deleted)
      return false;
    if (state.isChild && !includeChildren)
      return false;
    if (state.status !== "idle")
      return false;
    if (this.hasPendingInTree(sessionId))
      return false;
    if (this.hasActiveChildren(sessionId))
      return false;
    const treeLast = this.getTreeLastActivity(sessionId);
    if (now - treeLast < quiescenceMs)
      return false;
    if (state.doneNotifiedAt !== 0)
      return false;
    return true;
  }
  shouldNotifyImmediate(event) {
    try {
      const eventType = baseEventType(event.type);
      if (eventType === "permission.asked")
        return true;
      if (eventType === "question.asked" || eventType === "question.v2.asked")
        return true;
      if (eventType === "session.error") {
        const error = eventProperties(event).error;
        if (isBenignError(error))
          return false;
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }
  shouldNotifyDone(sessionId, now, quiescenceMs) {
    return this.isQuiescent(sessionId, now, quiescenceMs);
  }
  markDoneNotified(sessionId) {
    const state = this.sessions.get(sessionId);
    if (state) {
      state.doneNotifiedAt = Date.now();
    }
  }
  resetDoneNotified(sessionId) {
    const state = this.sessions.get(sessionId);
    if (state) {
      state.doneNotifiedAt = 0;
    }
  }
  markDeleted(sessionId) {
    const state = this.sessions.get(sessionId);
    if (state) {
      state.deleted = true;
      this.sessions.delete(sessionId);
    }
  }
  clear() {
    this.sessions.clear();
  }
  buildNotification(event) {
    try {
      const props = eventProperties(event);
      const sessionId = getStringProp(props, "sessionID") ?? event.id;
      const eventType = baseEventType(event.type);
      const state = this.getState(sessionId);
      const baseTitle = state?.title ?? sessionId;
      const shortId = getDbShortId(sessionId);
      const title = shortId !== undefined ? `#${shortId} ${baseTitle}` : baseTitle;
      switch (eventType) {
        case "session.error": {
          const error = props.error;
          let message;
          if (typeof error === "string") {
            message = error;
          } else if (typeof error === "object" && error !== null) {
            const msg = error.message;
            message = typeof msg === "string" ? msg : JSON.stringify(error);
          } else {
            message = "Unknown error";
          }
          return {
            id: `${event.id}-${Date.now()}`,
            type: "error",
            sessionId,
            title,
            message,
            timestamp: Date.now()
          };
        }
        case "permission.asked": {
          const message = getStringProp(props, "message") ?? "Permission requested";
          return {
            id: `${event.id}-${Date.now()}`,
            type: "permission",
            token: shortId(event.id),
            sessionId,
            title,
            message,
            timestamp: Date.now()
          };
        }
        case "question.asked":
        case "question.v2.asked": {
          const message = formatQuestionDetails(props);
          return {
            id: `${event.id}-${Date.now()}`,
            type: "question",
            token: shortId(event.id),
            sessionId,
            title,
            message,
            timestamp: Date.now()
          };
        }
        default:
          return;
      }
    } catch {
      return;
    }
  }
  buildDoneNotification(sessionId) {
    try {
      const state = this.getState(sessionId);
      if (!state)
        return;
      const baseTitle = state.title ?? sessionId;
      const shortId = getDbShortId(sessionId);
      const title = shortId !== undefined ? `#${shortId} ${baseTitle}` : baseTitle;
      return {
        id: `${sessionId}-done-${Date.now()}`,
        type: state.isChild ? "progress" : "done",
        sessionId,
        title,
        message: state.isChild ? "Subtask completed" : "Session completed",
        timestamp: Date.now()
      };
    } catch {
      return;
    }
  }
}

// src/notifier.ts
class Notifier {
  config;
  constructor(config) {
    this.config = config;
  }
  formatMessage(notification) {
    const title = notification.title ?? "opencode";
    switch (notification.type) {
      case "error":
        return `\uD83D\uDD34 opencode error
*${title}*
${notification.message}`;
      case "permission": {
        const tokenLine = notification.token ? `\nReply: /oc ok ${notification.token} or /oc no ${notification.token} [reason]` : "";
        return `\uD83D\uDFE1 opencode needs approval
*${title}*
${notification.message}${tokenLine}`;
      }
      case "question": {
        const tokenLine = notification.token ? `\nReply: /oc answer ${notification.token} <answer>\nShow all: /oc questions` : "\nShow all: /oc questions";
        return `\u2753 opencode is asking
*${title}*
${notification.message}${tokenLine}`;
      }
      case "done":
        return `\uD83D\uDFE2 opencode done
*${title}*
${notification.message}`;
      case "progress":
        return `\uD83D\uDFE2 opencode progress
*${title}*
${notification.message}`;
      default:
        return `${title}
${notification.message}`;
    }
  }
  async sendWhatsApp(notification) {
    if (!this.config.whatsappChatId) {
      return false;
    }
    const message = this.formatMessage(notification);
    const url = `${this.config.whatsappBridgeUrl}/send`;
    const body = {
      chatId: this.config.whatsappChatId,
      message
    };
    const controller = new AbortController;
    const timeout = setTimeout(() => controller.abort(), this.config.sendTimeoutMs);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal
      });
      return response.status >= 200 && response.status < 300;
    } catch {
      return false;
    } finally {
      clearTimeout(timeout);
    }
  }
  async sendTelegram(notification) {
    const botToken = this.config.telegramBotToken;
    const chatId = this.config.telegramChatId;
    if (!botToken || !chatId) {
      return false;
    }
    const message = this.formatMessage(notification);
    const baseUrl = `https://api.telegram.org/bot${botToken}/sendMessage`;
    const sendWithMode = async (parseMode) => {
      const body = { chat_id: chatId, text: message };
      if (parseMode) {
        body.parse_mode = parseMode;
      }
      const controller = new AbortController;
      const timeout = setTimeout(() => controller.abort(), this.config.sendTimeoutMs);
      try {
        return await fetch(baseUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
          signal: controller.signal
        });
      } finally {
        clearTimeout(timeout);
      }
    };
    try {
      let response = await sendWithMode("Markdown");
      if (!response.ok && response.status === 400) {
        let data;
        try {
          data = await response.json();
        } catch {}
        if (data?.description?.toLowerCase().includes("markdown")) {
          response = await sendWithMode();
        }
      }
      if (!response.ok) {
        const text = await response.text().catch(() => String(response.status));
        logPluginError("notifier.telegram", new Error(`Telegram API ${response.status}: ${text}`));
        return false;
      }
      return true;
    } catch (err) {
      logPluginError("notifier.telegram", err);
      return false;
    }
  }
  async send(notification) {
    const platform = (this.config.platform || "whatsapp").toLowerCase();
    if (platform === "telegram") {
      return this.sendTelegram(notification);
    }
    return this.sendWhatsApp(notification);
  }
}

// src/runtime.ts
class HermesRelayRuntime {
  config;
  sessionTracker;
  notifier;
  scheduler;
  quiescenceTimers = new Map;
  constructor(config) {
    this.config = config;
    this.sessionTracker = new SessionTracker;
    this.notifier = new Notifier(config);
    this.scheduler = new Scheduler(config, (n) => this.notifier.send(n));
  }
  start() {
    this.scheduler.start();
  }
  stop() {
    this.scheduler.stop();
    this.clearQuiescenceTimers();
    this.sessionTracker.clear();
  }
  onEvent(event) {
    try {
      const props = eventProperties(event);
      const sessionId = typeof props.sessionID === "string" ? props.sessionID : event.id;
      const eventType = baseEventType(event.type);
      const parentChain = [];
      if (eventType === "session.deleted") {
        let currentId = sessionId;
        while (currentId) {
          const state = this.sessionTracker.getState(currentId);
          const parentId = state?.parentId;
          if (parentId) {
            parentChain.push(parentId);
          }
          currentId = parentId;
        }
      }
      this.sessionTracker.trackEvent(event);
      if (this.sessionTracker.shouldNotifyImmediate(event)) {
        const immediateEventType = eventType;
        if ((immediateEventType === "permission.asked" || immediateEventType === "question.asked" || immediateEventType === "question.v2.asked") && props.id) {
          const details = immediateEventType.startsWith("question") ? questionDetailsFromEvent(event) : undefined;
          createCorrelation(shortId(event.id), sessionId, pluginDirectory, immediateEventType, props.id, details);
        }
        const notification = this.sessionTracker.buildNotification(event);
        if (notification) {
          this.scheduler.enqueue(notification);
        }
      }
      switch (eventType) {
        case "session.status": {
          const statusType = getStatusType(props);
          if (statusType === "busy") {
            this.sessionTracker.resetDoneNotified(sessionId);
          }
          break;
        }
        case "message.updated": {
          const role = getStringProp(props, "role") ?? getNestedStringProp(props, "info", "role");
          if (role === "user") {
            this.sessionTracker.resetDoneNotified(sessionId);
          }
          break;
        }
        case "message.part.updated": {
          const role = getStringProp(props, "role") ?? getNestedStringProp(props, "info", "role");
          if (role === "assistant") {
            this.sessionTracker.resetDoneNotified(sessionId);
          }
          break;
        }
        case "permission.asked":
        case "question.asked":
        case "question.v2.asked": {
          this.sessionTracker.resetDoneNotified(sessionId);
          break;
        }
      }
      this.recomputeQuiescenceTimer(sessionId);
      if (eventType === "session.deleted") {
        for (const parentId of parentChain) {
          this.sessionTracker.resetDoneNotified(parentId);
          this.recomputeQuiescenceTimer(parentId);
        }
      } else {
        let ancestorId = sessionId;
        while (ancestorId) {
          const ancestorState = this.sessionTracker.getState(ancestorId);
          const parentId = ancestorState?.parentId;
          if (parentId) {
            this.sessionTracker.resetDoneNotified(parentId);
            this.recomputeQuiescenceTimer(parentId);
          }
          ancestorId = parentId;
        }
      }
    } catch {}
  }
  checkQuiescence(sessionId) {
    try {
      this.quiescenceTimers.delete(sessionId);
      const now = Date.now();
      if (this.sessionTracker.isQuiescent(sessionId, now, this.config.quiescenceMs, true)) {
        const notification = this.sessionTracker.buildDoneNotification(sessionId);
        if (notification) {
          this.scheduler.enqueue(notification);
          this.sessionTracker.markDoneNotified(sessionId);
        }
      }
    } catch {}
  }
  recomputeQuiescenceTimer(sessionId) {
    try {
      this.cancelQuiescenceTimer(sessionId);
      const state = this.sessionTracker.getState(sessionId);
      if (!state)
        return;
      if (state.deleted || state.status !== "idle")
        return;
      if (state.pendingPermission || state.pendingQuestion)
        return;
      if (this.sessionTracker.hasActiveChildren(sessionId))
        return;
      const now = Date.now();
      if (this.sessionTracker.isQuiescent(sessionId, now, this.config.quiescenceMs)) {
        this.checkQuiescence(sessionId);
        return;
      }
      const treeLast = this.sessionTracker.getTreeLastActivity(sessionId);
      const delay = Math.max(0, this.config.quiescenceMs - (now - treeLast));
      const timer = setTimeout(() => {
        this.checkQuiescence(sessionId);
      }, delay);
      if (typeof timer.unref === "function") {
        timer.unref();
      }
      this.quiescenceTimers.set(sessionId, timer);
    } catch {}
  }
  cancelQuiescenceTimer(sessionId) {
    const timer = this.quiescenceTimers.get(sessionId);
    if (timer) {
      clearTimeout(timer);
      this.quiescenceTimers.delete(sessionId);
    }
  }
  clearQuiescenceTimers() {
    for (const [, timer] of this.quiescenceTimers) {
      clearTimeout(timer);
    }
    this.quiescenceTimers.clear();
  }
}

// src/plugin.ts
var plugin_default = {
  id: "opencode-hermes-commands",
  server: async (input) => {
    const config = loadConfig();
    if (!config.enabled) {
      return {};
    }
    pluginClient = input.client;
    pluginDirectory = input.directory;
    initStore();
    startCommandDrainLoop();
    const runtime = new HermesRelayRuntime(config);
    runtime.start();
    return {
      event: ({ event }) => {
        runtime.onEvent(event);
      }
    };
  }
};
export {
  plugin_default as default
};
