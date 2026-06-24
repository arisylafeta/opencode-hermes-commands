// @bun
import { appendFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";

const DB_PATH = process.env.HERMES_RELAY_DB_PATH ??
  `${process.env.HOME ?? "/root"}/.hermes/plugins/opencode-hermes-commands/state.db`;

let pluginClient = null;
let pluginDirectory = "";
let db = null;
let commandDrainTimer = null;
let commandDraining = false;
const COMMAND_DRAIN_INTERVAL_MS = 5000;
const COMMAND_TTL_MS = 300_000;

function initStore() {
  if (db) return;
  try {
    const dir = path.dirname(DB_PATH);
    mkdirSync(dir, { recursive: true });
    db = new DatabaseSync(DB_PATH);
    db.exec("PRAGMA journal_mode = WAL;");
    db.exec(`
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
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL
      );
    `);
    try {
      db.prepare("SELECT target_kind FROM commands LIMIT 0").all();
    } catch {
      db.exec("ALTER TABLE commands ADD COLUMN target_kind TEXT NOT NULL DEFAULT 'correlation'");
    }
  } catch (err) {
    logPluginError("initStore", err);
    throw err;
  }
}

function ensureDbSession(sessionId, directory, title, status, isChild, parentId) {
  if (!db || !directory) return;
  const now = Date.now();
  try {
    const stmt = db.prepare(`
      INSERT INTO sessions (session_id, short_id, directory, title, status, is_child, parent_id, created_at, updated_at, last_activity_at)
      VALUES (?, (SELECT COALESCE(MAX(short_id), 0) + 1 FROM sessions), ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(session_id) DO UPDATE SET
        directory = EXCLUDED.directory,
        title = COALESCE(EXCLUDED.title, sessions.title),
        status = COALESCE(EXCLUDED.status, sessions.status),
        is_child = EXCLUDED.is_child,
        parent_id = COALESCE(EXCLUDED.parent_id, sessions.parent_id),
        updated_at = EXCLUDED.updated_at,
        last_activity_at = EXCLUDED.last_activity_at
    `);
    stmt.run(sessionId, directory, title ?? null, status ?? "unknown", isChild ? 1 : 0, parentId ?? null, now, now, now);
  } catch (err) {
    logPluginError("ensureDbSession", err);
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
          )
        ORDER BY c.id ASC
        LIMIT 1
      )
      RETURNING *
    `);
    command = claim.get(now, now, pluginDirectory, pluginDirectory);
  } catch (err) {
    logPluginError("commandClaim", err);
    return;
  }
  if (!command) return;

  let targetId = null;
  try {
    if (command.target_kind === "session") {
      const row = db.prepare(`SELECT session_id FROM sessions WHERE short_id = ? AND directory = ?`).get(command.token, pluginDirectory);
      targetId = row?.session_id ?? null;
    } else {
      const row = db.prepare(`SELECT opencode_session_id FROM correlations WHERE token = ? AND directory = ?`).get(command.token, pluginDirectory);
      targetId = row?.opencode_session_id ?? null;
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
        await pluginClient.permission.reply({ requestID: payload.requestID, reply: "once" });
        break;
      }
      case "reject": {
        await pluginClient.permission.reply({ requestID: payload.requestID, reply: "reject", message: payload.message });
        break;
      }
      case "answer": {
        await pluginClient.question.reply({ requestID: payload.requestID, answers: [[payload.answer]] });
        break;
      }
      case "continue":
      case "say": {
        await pluginClient.session.prompt({ sessionID: targetId, parts: [{ type: "text", text: payload.message }] });
        break;
      }
      case "show": {
        const messages = await pluginClient.session.messages({ sessionID: targetId });
        result = extractLastAssistantText(messages);
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
    whatsappBridgeUrl: parseString(process.env.HERMES_WHATSAPP_BRIDGE_URL, "http://127.0.0.1:3000"),
    whatsappChatId: parseString(process.env.HERMES_WHATSAPP_CHAT_ID, process.env.WHATSAPP_HOME_CHANNEL ?? ""),
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

class SessionTracker {
  sessions = new Map;
  messageRoles = new Map;
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
      const props = event.properties ?? {};
      const sessionId = getStringProp(props, "sessionID") ?? event.id;
      switch (event.type) {
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
            if (text) {
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
        case "question.asked": {
          const state = this.ensureState(sessionId);
          state.pendingQuestion = true;
          state.lastActivityAt = Date.now();
          state.doneNotifiedAt = 0;
          break;
        }
        case "question.replied":
        case "question.rejected": {
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
  isQuiescent(sessionId, now, quiescenceMs) {
    const state = this.sessions.get(sessionId);
    if (!state)
      return false;
    if (state.deleted)
      return false;
    if (state.isChild)
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
      if (event.type === "permission.asked")
        return true;
      if (event.type === "question.asked")
        return true;
      if (event.type === "session.error") {
        const error = event.properties?.error;
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
      const props = event.properties ?? {};
      const sessionId = getStringProp(props, "sessionID") ?? event.id;
      const state = this.getState(sessionId);
      const baseTitle = state?.title ?? sessionId;
      const shortId = getDbShortId(sessionId);
      const title = shortId !== undefined ? `#${shortId} ${baseTitle}` : baseTitle;
      switch (event.type) {
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
            sessionId,
            title,
            message,
            timestamp: Date.now()
          };
        }
        case "question.asked": {
          const message = getStringProp(props, "message") ?? "Question asked";
          return {
            id: `${event.id}-${Date.now()}`,
            type: "question",
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
        type: "done",
        sessionId,
        title,
        message: "Session completed",
        timestamp: Date.now()
      };
    } catch {
      return;
    }
  }
}

// src/notifier.ts
class WhatsAppNotifier {
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
      case "permission":
        return `\uD83D\uDFE1 opencode needs approval
*${title}*
${notification.message}`;
      case "question":
        return `\u2753 opencode is asking
*${title}*
${notification.message}`;
      case "done":
        return `\uD83D\uDFE2 opencode done
*${title}*
${notification.message}`;
      default:
        return `${title}
${notification.message}`;
    }
  }
  async send(notification) {
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
    this.notifier = new WhatsAppNotifier(config);
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
      const props = event.properties ?? {};
      const sessionId = typeof props.sessionID === "string" ? props.sessionID : event.id;
      const parentChain = [];
      if (event.type === "session.deleted") {
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
        const notification = this.sessionTracker.buildNotification(event);
        if (notification) {
          this.scheduler.enqueue(notification);
        }
      }
      switch (event.type) {
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
        case "question.asked": {
          this.sessionTracker.resetDoneNotified(sessionId);
          break;
        }
      }
      this.recomputeQuiescenceTimer(sessionId);
      if (event.type === "session.deleted") {
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
      if (this.sessionTracker.isQuiescent(sessionId, now, this.config.quiescenceMs)) {
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
      if (state.isChild || state.deleted || state.status !== "idle")
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
