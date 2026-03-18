/**
 * ClaudeCodeAdapterLive - Scoped live implementation for the Claude Code provider adapter.
 *
 * Wraps `@anthropic-ai/claude-agent-sdk` query sessions behind the generic
 * provider adapter contract and emits canonical runtime events.
 *
 * @module ClaudeCodeAdapterLive
 */
import {
  type CanUseTool,
  query,
  type Options as ClaudeQueryOptions,
  type PermissionMode,
  type PermissionResult,
  type PermissionUpdate,
  type SDKMessage,
  type SDKResultMessage,
  type SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";
import {
  ApprovalRequestId,
  type CanonicalItemType,
  type CanonicalRequestType,
  EventId,
  type ProviderApprovalDecision,
  ProviderItemId,
  type ProviderInteractionMode,
  type ProviderRuntimeEvent,
  type ProviderRuntimeTurnStatus,
  type ProviderSendTurnInput,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  RuntimeTaskId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { Cause, DateTime, Deferred, Effect, FileSystem, Layer, Queue, Random, Ref, Stream } from "effect";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { ClaudeCodeAdapter, type ClaudeCodeAdapterShape } from "../Services/ClaudeCodeAdapter.ts";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = "claudeCode" as const;

type PromptQueueItem =
  | {
      readonly type: "message";
      readonly message: SDKUserMessage;
    }
  | {
      readonly type: "terminate";
    };

interface ClaudeResumeState {
  readonly threadId?: ThreadId;
  readonly resume?: string;
  readonly resumeSessionAt?: string;
  readonly turnCount?: number;
}

interface ClaudeTurnState {
  readonly turnId: TurnId;
  readonly assistantItemId: string;
  readonly startedAt: string;
  readonly items: Array<unknown>;
  readonly messageCompleted: boolean;
  readonly emittedTextDelta: boolean;
  readonly fallbackAssistantText: string;
}

interface PendingApproval {
  readonly requestType: CanonicalRequestType;
  readonly detail?: string;
  readonly suggestions?: ReadonlyArray<PermissionUpdate>;
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

interface ToolInFlight {
  readonly itemId: string;
  readonly itemType: CanonicalItemType;
  readonly toolName: string;
  readonly title: string;
  readonly detail?: string;
  inputJsonChunks: string[];
}

interface ClaudeSessionContext {
  session: ProviderSession;
  /** Mutable: replaced when interaction mode changes (session restart). */
  promptQueue: Queue.Queue<PromptQueueItem>;
  /** Mutable: replaced when interaction mode changes (session restart). */
  query: ClaudeQueryRuntime;
  readonly startedAt: string;
  resumeSessionId: string | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly turns: Array<{
    id: TurnId;
    items: Array<unknown>;
  }>;
  readonly inFlightTools: Map<number, ToolInFlight>;
  turnState: ClaudeTurnState | undefined;
  lastAssistantUuid: string | undefined;
  lastThreadStartedId: string | undefined;
  stopped: boolean;
  /** Tracks the active interaction mode so we can detect changes between turns. */
  interactionMode: ProviderInteractionMode | undefined;
  /** True when the SDK stream ended unexpectedly (process died between turns). */
  queryDead: boolean;
  /** Stored to rebuild queryOptions on session restart (mode change). */
  readonly canUseTool: CanUseTool;
  readonly permissionMode: PermissionMode | undefined;
  readonly maxThinkingTokens: number | undefined;
  /** Mutable: updated when the user changes effort level between turns. */
  effort: "low" | "medium" | "high" | undefined;
  readonly pathToClaudeCodeExecutable: string | undefined;
  /** Whether to pass --chrome to the Claude CLI via extraArgs. */
  readonly chrome: boolean;
}

interface ClaudeQueryRuntime extends AsyncIterable<SDKMessage> {
  readonly interrupt: () => Promise<void>;
  readonly setModel: (model?: string) => Promise<void>;
  readonly setPermissionMode: (mode: PermissionMode) => Promise<void>;
  readonly setMaxThinkingTokens: (maxThinkingTokens: number | null) => Promise<void>;
  readonly close: () => void;
}

export interface ClaudeCodeAdapterLiveOptions {
  readonly createQuery?: (input: {
    readonly prompt: AsyncIterable<SDKUserMessage>;
    readonly options: ClaudeQueryOptions;
  }) => ClaudeQueryRuntime;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function isSyntheticClaudeThreadId(value: string): boolean {
  return value.startsWith("claude-thread-");
}

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error && cause.message.length > 0) {
    return cause.message;
  }
  return fallback;
}

function asRuntimeItemId(value: string): RuntimeItemId {
  return RuntimeItemId.makeUnsafe(value);
}

function asCanonicalTurnId(value: TurnId): TurnId {
  return value;
}

function asRuntimeRequestId(value: ApprovalRequestId): RuntimeRequestId {
  return RuntimeRequestId.makeUnsafe(value);
}

function toPermissionMode(value: unknown): PermissionMode | undefined {
  switch (value) {
    case "default":
    case "acceptEdits":
    case "bypassPermissions":
    case "plan":
    case "dontAsk":
      return value;
    default:
      return undefined;
  }
}

function readClaudeResumeState(resumeCursor: unknown): ClaudeResumeState | undefined {
  if (!resumeCursor || typeof resumeCursor !== "object") {
    return undefined;
  }
  const cursor = resumeCursor as {
    threadId?: unknown;
    resume?: unknown;
    sessionId?: unknown;
    resumeSessionAt?: unknown;
    turnCount?: unknown;
  };

  const threadIdCandidate = typeof cursor.threadId === "string" ? cursor.threadId : undefined;
  const threadId =
    threadIdCandidate && !isSyntheticClaudeThreadId(threadIdCandidate)
      ? ThreadId.makeUnsafe(threadIdCandidate)
      : undefined;
  const resumeCandidate =
    typeof cursor.resume === "string"
      ? cursor.resume
      : typeof cursor.sessionId === "string"
        ? cursor.sessionId
        : undefined;
  const resume = resumeCandidate && isUuid(resumeCandidate) ? resumeCandidate : undefined;
  const resumeSessionAt =
    typeof cursor.resumeSessionAt === "string" ? cursor.resumeSessionAt : undefined;
  const turnCountValue = typeof cursor.turnCount === "number" ? cursor.turnCount : undefined;

  return {
    ...(threadId ? { threadId } : {}),
    ...(resume ? { resume } : {}),
    ...(resumeSessionAt ? { resumeSessionAt } : {}),
    ...(turnCountValue !== undefined && Number.isInteger(turnCountValue) && turnCountValue >= 0
      ? { turnCount: turnCountValue }
      : {}),
  };
}

function classifyToolItemType(toolName: string): CanonicalItemType {
  const normalized = toolName.toLowerCase();
  if (
    normalized.includes("bash") ||
    normalized.includes("command") ||
    normalized.includes("shell") ||
    normalized.includes("terminal")
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("file") ||
    normalized.includes("patch") ||
    normalized.includes("replace") ||
    normalized.includes("create") ||
    normalized.includes("delete")
  ) {
    return "file_change";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  return "dynamic_tool_call";
}

function classifyRequestType(toolName: string): CanonicalRequestType {
  const normalized = toolName.toLowerCase();
  if (normalized === "read" || normalized.includes("read file") || normalized.includes("view")) {
    return "file_read_approval";
  }
  return classifyToolItemType(toolName) === "command_execution"
    ? "command_execution_approval"
    : "file_change_approval";
}

function summarizeToolRequest(toolName: string, input: Record<string, unknown>): string {
  const commandValue = input.command ?? input.cmd;
  const command = typeof commandValue === "string" ? commandValue : undefined;
  if (command && command.trim().length > 0) {
    return `${toolName}: ${command.trim().slice(0, 400)}`;
  }

  const serialized = JSON.stringify(input);
  if (serialized === "{}" || serialized === "null") {
    return toolName;
  }
  if (serialized.length <= 400) {
    return `${toolName}: ${serialized}`;
  }
  return `${toolName}: ${serialized.slice(0, 397)}...`;
}

function recalculateToolDetail(tool: ToolInFlight): string | undefined {
  if (tool.inputJsonChunks.length === 0) {
    return tool.detail;
  }
  const rawJson = tool.inputJsonChunks.join("");
  try {
    const parsedInput = JSON.parse(rawJson) as Record<string, unknown>;
    return summarizeToolRequest(tool.toolName, parsedInput);
  } catch {
    return `${tool.toolName}: ${rawJson.slice(0, 400)}`;
  }
}

function titleForTool(itemType: CanonicalItemType): string {
  switch (itemType) {
    case "command_execution":
      return "Command run";
    case "file_change":
      return "File change";
    case "mcp_tool_call":
      return "MCP tool call";
    case "dynamic_tool_call":
      return "Tool call";
    default:
      return "Item";
  }
}

/**
 * Wraps an Effect-based AsyncIterable so that fiber interruption errors
 * (from queue shutdown) become a clean end-of-stream signal instead of
 * throwing.  Without this, shutting down the prompt queue during an
 * interaction-mode restart causes the Claude SDK's streamInput to receive
 * an unhandled rejection that crashes the entire backend process.
 */
function safeAsyncIterable<T>(source: AsyncIterable<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      const iterator = source[Symbol.asyncIterator]();
      return {
        async next(): Promise<IteratorResult<T>> {
          try {
            return await iterator.next();
          } catch {
            return { done: true, value: undefined as T };
          }
        },
        async return(value?: T): Promise<IteratorResult<T>> {
          try {
            return await (iterator.return?.(value) ?? Promise.resolve({ done: true as const, value: undefined as T }));
          } catch {
            return { done: true, value: undefined as T };
          }
        },
      };
    },
  };
}

function buildPromptStream(queue: Queue.Queue<PromptQueueItem>): AsyncIterable<SDKUserMessage> {
  return safeAsyncIterable(
    Stream.fromQueue(queue).pipe(
      Stream.filter((item) => item.type === "message"),
      Stream.map((item) => item.message),
      Stream.toAsyncIterable,
    ),
  );
}

function buildUserMessage(
  input: ProviderSendTurnInput,
  fileSystem: FileSystem.FileSystem,
  stateDir: string,
): Effect.Effect<SDKUserMessage, ProviderAdapterError> {
  return Effect.gen(function* () {
    const contentBlocks: Array<Record<string, unknown>> = [];

    if (input.input && input.input.trim().length > 0) {
      contentBlocks.push({ type: "text", text: input.input.trim() });
    }

    for (const attachment of input.attachments ?? []) {
      if (attachment.type === "image") {
        const attachmentPath = resolveAttachmentPath({ stateDir, attachment });
        if (!attachmentPath) {
          yield* Effect.logWarning(
            `Could not resolve path for attachment '${attachment.id}' ('${attachment.name}'), skipping.`,
          );
          continue;
        }
        const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "turn/start",
                detail: `Failed to read attachment '${attachment.name}'.`,
                cause,
              }),
          ),
        );
        contentBlocks.push({
          type: "image",
          source: {
            type: "base64",
            media_type: attachment.mimeType,
            data: Buffer.from(bytes).toString("base64"),
          },
        });
      }
    }

    if (contentBlocks.length === 0) {
      contentBlocks.push({ type: "text", text: "" });
    }

    return {
      type: "user",
      session_id: "",
      parent_tool_use_id: null,
      message: {
        role: "user",
        content: contentBlocks,
      },
    } as SDKUserMessage;
  });
}

function turnStatusFromResult(result: SDKResultMessage): ProviderRuntimeTurnStatus {
  if (result.subtype === "success") {
    return "completed";
  }

  const errors = result.errors.join(" ").toLowerCase();
  if (errors.includes("interrupt")) {
    return "interrupted";
  }
  if (errors.includes("cancel")) {
    return "cancelled";
  }
  return "failed";
}

function streamKindFromDeltaType(deltaType: string): "assistant_text" | "reasoning_text" {
  return deltaType.includes("thinking") ? "reasoning_text" : "assistant_text";
}

function providerThreadRef(
  context: ClaudeSessionContext,
): { readonly providerThreadId: string } | {} {
  return context.resumeSessionId ? { providerThreadId: context.resumeSessionId } : {};
}

function extractAssistantText(message: SDKMessage): string {
  if (message.type !== "assistant") {
    return "";
  }

  const content = (message.message as { content?: unknown } | undefined)?.content;
  if (!Array.isArray(content)) {
    return "";
  }

  const fragments: string[] = [];
  for (const block of content) {
    if (!block || typeof block !== "object") {
      continue;
    }
    const candidate = block as { type?: unknown; text?: unknown };
    if (
      candidate.type === "text" &&
      typeof candidate.text === "string" &&
      candidate.text.length > 0
    ) {
      fragments.push(candidate.text);
    }
  }

  return fragments.join("");
}

function toSessionError(
  threadId: ThreadId,
  cause: unknown,
): ProviderAdapterSessionNotFoundError | ProviderAdapterSessionClosedError | undefined {
  const normalized = toMessage(cause, "").toLowerCase();
  if (normalized.includes("unknown session") || normalized.includes("not found")) {
    return new ProviderAdapterSessionNotFoundError({
      provider: PROVIDER,
      threadId,
      cause,
    });
  }
  if (normalized.includes("closed")) {
    return new ProviderAdapterSessionClosedError({
      provider: PROVIDER,
      threadId,
      cause,
    });
  }
  return undefined;
}

function toRequestError(threadId: ThreadId, method: string, cause: unknown): ProviderAdapterError {
  const sessionError = toSessionError(threadId, cause);
  if (sessionError) {
    return sessionError;
  }
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: toMessage(cause, `${method} failed`),
    cause,
  });
}

function sdkMessageType(value: unknown): string | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const record = value as { type?: unknown };
  return typeof record.type === "string" ? record.type : undefined;
}

function sdkMessageSubtype(value: unknown): string | undefined {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  const record = value as { subtype?: unknown };
  return typeof record.subtype === "string" ? record.subtype : undefined;
}

function sdkNativeMethod(message: SDKMessage): string {
  const subtype = sdkMessageSubtype(message);
  if (subtype) {
    return `claude/${message.type}/${subtype}`;
  }

  if (message.type === "stream_event") {
    const streamType = sdkMessageType(message.event);
    if (streamType) {
      const deltaType =
        streamType === "content_block_delta"
          ? sdkMessageType((message.event as { delta?: unknown }).delta)
          : undefined;
      if (deltaType) {
        return `claude/${message.type}/${streamType}/${deltaType}`;
      }
      return `claude/${message.type}/${streamType}`;
    }
  }

  return `claude/${message.type}`;
}

function sdkNativeItemId(message: SDKMessage): string | undefined {
  if (message.type === "assistant") {
    const maybeId = (message.message as { id?: unknown }).id;
    if (typeof maybeId === "string") {
      return maybeId;
    }
    return undefined;
  }

  if (message.type === "stream_event") {
    const event = message.event as {
      type?: unknown;
      content_block?: { id?: unknown };
    };
    if (event.type === "content_block_start" && typeof event.content_block?.id === "string") {
      return event.content_block.id;
    }
  }

  return undefined;
}

// ---------------------------------------------------------------------------
// Plan Mode support
// ---------------------------------------------------------------------------

/**
 * System prompt appended to Claude Code's default prompt when Plan Mode is
 * active.  Mirrors the 3-phase structure of CODEX_PLAN_MODE_DEVELOPER_INSTRUCTIONS
 * but is adapted for Claude Code's native tooling (no `request_user_input` tool).
 */
export const CLAUDE_CODE_PLAN_MODE_SYSTEM_PROMPT = `<collaboration_mode># Plan Mode (Conversational)

You work in 3 phases, and you should *chat your way* to a great plan before finalizing it. A great plan is very detailed—intent- and implementation-wise—so that it can be handed to another engineer or agent to be implemented right away. It must be **decision complete**, where the implementer does not need to make any decisions.

## Mode rules (strict)

You are in **Plan Mode** until the developer explicitly switches you back to Default mode.

Plan Mode is not changed by user intent, tone, or imperative language. If a user asks for execution while still in Plan Mode, treat it as a request to **plan the execution**, not perform it.

## Execution vs. mutation in Plan Mode

You may explore and execute **non-mutating** actions that improve the plan. You must not perform **mutating** actions.

### Allowed (non-mutating, plan-improving)

Actions that gather truth, reduce ambiguity, or validate feasibility without changing repo-tracked state. Examples:

* Reading or searching files, configs, schemas, types, manifests, and docs
* Static analysis, inspection, and repo exploration
* Dry-run style commands when they do not edit repo-tracked files
* Tests, builds, or checks that may write to caches or build artifacts (for example, \`target/\`, \`.cache/\`, or snapshots) so long as they do not edit repo-tracked files

### Not allowed (mutating, plan-executing)

Actions that implement the plan or change repo-tracked state. Examples:

* Editing or writing files
* Running formatters or linters that rewrite files
* Applying patches, migrations, or codegen that updates repo-tracked files
* Side-effectful commands whose purpose is to carry out the plan rather than refine it

When in doubt: if the action would reasonably be described as "doing the work" rather than "planning the work," do not do it.

## PHASE 1 - Ground in the environment (explore first, ask second)

Begin by grounding yourself in the actual environment. Eliminate unknowns in the prompt by discovering facts, not by asking the user. Resolve all questions that can be answered through exploration or inspection. Identify missing or ambiguous details only if they cannot be derived from the environment. Silent exploration between turns is allowed and encouraged.

Before asking the user any question, perform at least one targeted non-mutating exploration pass (for example: search relevant files, inspect likely entrypoints/configs, confirm current implementation shape), unless no local environment/repo is available.

Exception: you may ask clarifying questions about the user's prompt before exploring, ONLY if there are obvious ambiguities or contradictions in the prompt itself. However, if ambiguity might be resolved by exploring, always prefer exploring first.

Do not ask questions that can be answered from the repo or system (for example, "where is this struct?" or "which UI component should we use?" when exploration can make it clear). Only ask once you have exhausted reasonable non-mutating exploration.

## PHASE 2 - Intent chat (what they actually want)

* Keep asking until you can clearly state: goal + success criteria, audience, in/out of scope, constraints, current state, and the key preferences/tradeoffs.
* Bias toward questions over guessing: if any high-impact ambiguity remains, do NOT plan yet—ask.

## PHASE 3 - Implementation chat (what/how we'll build)

* Once intent is stable, keep asking until the spec is decision complete: approach, interfaces (APIs/schemas/I/O), data flow, edge cases/failure modes, testing + acceptance criteria, rollout/monitoring, and any migrations/compat constraints.

## Asking questions

Critical rules:

* Offer only meaningful multiple-choice options; don't include filler choices that are obviously wrong or irrelevant.
* In rare cases where an unavoidable, important question can't be expressed with reasonable multiple-choice options (due to extreme ambiguity), ask it directly.

You SHOULD ask many questions, but each question must:

* materially change the spec/plan, OR
* confirm/lock an assumption, OR
* choose between meaningful tradeoffs.
* not be answerable by non-mutating commands.

## Two kinds of unknowns (treat differently)

1. **Discoverable facts** (repo/system truth): explore first.

   * Before asking, run targeted searches and check likely sources of truth (configs/manifests/entrypoints/schemas/types/constants).
   * Ask only if: multiple plausible candidates; nothing found but you need a missing identifier/context; or ambiguity is actually product intent.
   * If asking, present concrete candidates (paths/service names) + recommend one.
   * Never ask questions you can answer from your environment (e.g., "where is this struct").

2. **Preferences/tradeoffs** (not discoverable): ask early.

   * These are intent or implementation preferences that cannot be derived from exploration.
   * Provide 2-4 mutually exclusive options + a recommended default.
   * If unanswered, proceed with the recommended option and record it as an assumption in the final plan.

## Finalization rule

Only output the final plan when it is decision complete and leaves no decisions to the implementer.

When you present the official plan, wrap it in a \`<proposed_plan>\` block so the client can render it specially:

1) The opening tag must be on its own line.
2) Start the plan content on the next line (no text on the same line as the tag).
3) The closing tag must be on its own line.
4) Use Markdown inside the block.
5) Keep the tags exactly as \`<proposed_plan>\` and \`</proposed_plan>\` (do not translate or rename them), even if the plan content is in another language.

Example:

<proposed_plan>
plan content
</proposed_plan>

plan content should be human and agent digestible. The final plan must be plan-only and include:

* A clear title
* A brief summary section
* Important changes or additions to public APIs/interfaces/types
* Test cases and scenarios
* Explicit assumptions and defaults chosen where needed

Do not ask "should I proceed?" in the final output. The user can easily switch out of Plan mode and request implementation if you have included a \`<proposed_plan>\` block in your response. Alternatively, they can decide to stay in Plan mode and continue refining the plan.

Only produce at most one \`<proposed_plan>\` block per turn, and only when you are presenting a complete spec.
</collaboration_mode>`;

const PROPOSED_PLAN_BLOCK_REGEX = /<proposed_plan>\s*([\s\S]*?)\s*<\/proposed_plan>/i;

function extractProposedPlanMarkdown(text: string | undefined): string | undefined {
  const match = text ? PROPOSED_PLAN_BLOCK_REGEX.exec(text) : null;
  const planMarkdown = match?.[1]?.trim();
  return planMarkdown && planMarkdown.length > 0 ? planMarkdown : undefined;
}

function buildSystemPromptForInteractionMode(
  interactionMode: ProviderInteractionMode | undefined,
): ClaudeQueryOptions["systemPrompt"] {
  if (interactionMode === "plan") {
    return {
      type: "preset",
      preset: "claude_code",
      append: CLAUDE_CODE_PLAN_MODE_SYSTEM_PROMPT,
    };
  }
  return undefined;
}

// ---------------------------------------------------------------------------

function makeClaudeCodeAdapter(options?: ClaudeCodeAdapterLiveOptions) {
  return Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const serverConfig = yield* Effect.service(ServerConfig);

    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
            stream: "native",
          })
        : undefined);

    const createQuery =
      options?.createQuery ??
      ((input: {
        readonly prompt: AsyncIterable<SDKUserMessage>;
        readonly options: ClaudeQueryOptions;
      }) => query({ prompt: input.prompt, options: input.options }) as ClaudeQueryRuntime);

    const sessions = new Map<ThreadId, ClaudeSessionContext>();
    const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const nextEventId = Effect.map(Random.nextUUIDv4, (id) => EventId.makeUnsafe(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
      Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

    const logNativeSdkMessage = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (!nativeEventLogger) {
          return;
        }

        const observedAt = new Date().toISOString();
        const itemId = sdkNativeItemId(message);

        yield* nativeEventLogger.write(
          {
            observedAt,
            event: {
              id:
                "uuid" in message && typeof message.uuid === "string"
                  ? message.uuid
                  : crypto.randomUUID(),
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method: sdkNativeMethod(message),
              threadId: context.session.threadId,
              ...(typeof message.session_id === "string"
                ? { providerThreadId: message.session_id }
                : {}),
              ...(context.turnState ? { turnId: asCanonicalTurnId(context.turnState.turnId) } : {}),
              ...(itemId ? { itemId: ProviderItemId.makeUnsafe(itemId) } : {}),
              payload: message,
            },
          },
          null,
        );
      });

    const snapshotThread = (
      context: ClaudeSessionContext,
    ): Effect.Effect<
      {
        threadId: ThreadId;
        turns: ReadonlyArray<{
          id: TurnId;
          items: ReadonlyArray<unknown>;
        }>;
      },
      ProviderAdapterValidationError
    > =>
      Effect.gen(function* () {
        const threadId = context.session.threadId;
        if (!threadId) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "readThread",
            issue: "Session thread id is not initialized yet.",
          });
        }
        return {
          threadId,
          turns: context.turns.map((turn) => ({
            id: turn.id,
            items: [...turn.items],
          })),
        };
      });

    const updateResumeCursor = (context: ClaudeSessionContext): Effect.Effect<void> =>
      Effect.gen(function* () {
        const threadId = context.session.threadId;
        if (!threadId) return;

        const resumeCursor = {
          threadId,
          ...(context.resumeSessionId ? { resume: context.resumeSessionId } : {}),
          ...(context.lastAssistantUuid ? { resumeSessionAt: context.lastAssistantUuid } : {}),
          turnCount: context.turns.length,
        };

        context.session = {
          ...context.session,
          resumeCursor,
          updatedAt: yield* nowIso,
        };
      });

    const ensureThreadId = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (typeof message.session_id !== "string" || message.session_id.length === 0) {
          return;
        }
        const nextThreadId = message.session_id;
        context.resumeSessionId = message.session_id;

        yield* updateResumeCursor(context);

        if (context.lastThreadStartedId !== nextThreadId) {
          context.lastThreadStartedId = nextThreadId;
          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "thread.started",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            threadId: context.session.threadId,
            payload: {
              providerThreadId: nextThreadId,
            },
            providerRefs: {},
            raw: {
              source: "claude.sdk.message",
              method: "claude/thread/started",
              payload: {
                session_id: message.session_id,
              },
            },
          });
        }
      });

    const emitRuntimeError = (
      context: ClaudeSessionContext,
      message: string,
      cause?: unknown,
      errorClass: "provider_error" | "context_overflow" = "provider_error",
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (cause !== undefined) {
          void cause;
        }
        const turnState = context.turnState;
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "runtime.error",
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId: context.session.threadId,
          ...(turnState ? { turnId: asCanonicalTurnId(turnState.turnId) } : {}),
          payload: {
            message,
            class: errorClass,
            ...(cause !== undefined ? { detail: cause } : {}),
          },
          providerRefs: {
            ...providerThreadRef(context),
            ...(turnState ? { providerTurnId: String(turnState.turnId) } : {}),
          },
        });
      });

    const emitRuntimeWarning = (
      context: ClaudeSessionContext,
      message: string,
      detail?: unknown,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const turnState = context.turnState;
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "runtime.warning",
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId: context.session.threadId,
          ...(turnState ? { turnId: asCanonicalTurnId(turnState.turnId) } : {}),
          payload: {
            message,
            ...(detail !== undefined ? { detail } : {}),
          },
          providerRefs: {
            ...providerThreadRef(context),
            ...(turnState ? { providerTurnId: String(turnState.turnId) } : {}),
          },
        });
      });

    const completeTurn = (
      context: ClaudeSessionContext,
      status: ProviderRuntimeTurnStatus,
      errorMessage?: string,
      result?: SDKResultMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const turnState = context.turnState;
        if (!turnState) {
          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "turn.completed",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            threadId: context.session.threadId,
            payload: {
              state: status,
              ...(result?.stop_reason !== undefined ? { stopReason: result.stop_reason } : {}),
              ...(result?.usage ? { usage: result.usage } : {}),
              ...(result?.modelUsage ? { modelUsage: result.modelUsage } : {}),
              ...(typeof result?.total_cost_usd === "number"
                ? { totalCostUsd: result.total_cost_usd }
                : {}),
              ...(errorMessage ? { errorMessage } : {}),
            },
            providerRefs: {},
          });
          return;
        }

        if (!turnState.messageCompleted) {
          if (!turnState.emittedTextDelta && turnState.fallbackAssistantText.length > 0) {
            const deltaStamp = yield* makeEventStamp();
            yield* offerRuntimeEvent({
              type: "content.delta",
              eventId: deltaStamp.eventId,
              provider: PROVIDER,
              createdAt: deltaStamp.createdAt,
              threadId: context.session.threadId,
              turnId: turnState.turnId,
              itemId: asRuntimeItemId(turnState.assistantItemId),
              payload: {
                streamKind: "assistant_text",
                delta: turnState.fallbackAssistantText,
              },
              providerRefs: {
                ...providerThreadRef(context),
                providerTurnId: String(turnState.turnId),
                providerItemId: ProviderItemId.makeUnsafe(turnState.assistantItemId),
              },
            });
          }

          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "item.completed",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            itemId: asRuntimeItemId(turnState.assistantItemId),
            threadId: context.session.threadId,
            turnId: turnState.turnId,
            payload: {
              itemType: "assistant_message",
              status: "completed",
              title: "Assistant message",
            },
            providerRefs: {
              ...providerThreadRef(context),
              providerTurnId: turnState.turnId,
              providerItemId: ProviderItemId.makeUnsafe(turnState.assistantItemId),
            },
          });
        }

        context.turns.push({
          id: turnState.turnId,
          items: [...turnState.items],
        });

        // Detect a <proposed_plan> block in the assistant's response and
        // emit a first-class plan event so the orchestration layer can
        // persist and surface it identically to the Codex provider.
        const proposedPlanMarkdown = extractProposedPlanMarkdown(
          turnState.fallbackAssistantText || undefined,
        );
        if (proposedPlanMarkdown) {
          const planStamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "turn.proposed.completed",
            eventId: planStamp.eventId,
            provider: PROVIDER,
            createdAt: planStamp.createdAt,
            threadId: context.session.threadId,
            turnId: turnState.turnId,
            payload: {
              planMarkdown: proposedPlanMarkdown,
            },
            providerRefs: {
              ...providerThreadRef(context),
              providerTurnId: String(turnState.turnId),
            },
          });
        }

        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "turn.completed",
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId: context.session.threadId,
          turnId: turnState.turnId,
          payload: {
            state: status,
            ...(result?.stop_reason !== undefined ? { stopReason: result.stop_reason } : {}),
            ...(result?.usage ? { usage: result.usage } : {}),
            ...(result?.modelUsage ? { modelUsage: result.modelUsage } : {}),
            ...(typeof result?.total_cost_usd === "number"
              ? { totalCostUsd: result.total_cost_usd }
              : {}),
            ...(errorMessage ? { errorMessage } : {}),
          },
          providerRefs: {
            ...providerThreadRef(context),
            providerTurnId: turnState.turnId,
          },
        });

        const updatedAt = yield* nowIso;
        context.turnState = undefined;
        context.session = {
          ...context.session,
          status: "ready",
          activeTurnId: undefined,
          updatedAt,
          ...(status === "failed" && errorMessage ? { lastError: errorMessage } : {}),
        };
        yield* updateResumeCursor(context);
      });

    const handleStreamEvent = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (message.type !== "stream_event") {
          return;
        }

        const { event } = message;

        if (event.type === "content_block_delta") {
          if (event.delta.type === "input_json_delta") {
            const tool = context.inFlightTools.get(event.index);
            if (tool && typeof event.delta.partial_json === "string") {
              tool.inputJsonChunks.push(event.delta.partial_json);
            }
            return;
          }

          if (
            event.delta.type === "text_delta" &&
            event.delta.text.length > 0 &&
            context.turnState
          ) {
            if (!context.turnState.emittedTextDelta) {
              context.turnState = {
                ...context.turnState,
                emittedTextDelta: true,
              };
            }
            const stamp = yield* makeEventStamp();
            yield* offerRuntimeEvent({
              type: "content.delta",
              eventId: stamp.eventId,
              provider: PROVIDER,
              createdAt: stamp.createdAt,
              threadId: context.session.threadId,
              turnId: context.turnState.turnId,
              itemId: asRuntimeItemId(context.turnState.assistantItemId),
              payload: {
                streamKind: streamKindFromDeltaType(event.delta.type),
                delta: event.delta.text,
              },
              providerRefs: {
                ...providerThreadRef(context),
                providerTurnId: context.turnState.turnId,
                providerItemId: ProviderItemId.makeUnsafe(context.turnState.assistantItemId),
              },
              raw: {
                source: "claude.sdk.message",
                method: "claude/stream_event/content_block_delta",
                payload: message,
              },
            });
          }
          return;
        }

        if (event.type === "content_block_start") {
          const { index, content_block: block } = event;
          if (
            block.type !== "tool_use" &&
            block.type !== "server_tool_use" &&
            block.type !== "mcp_tool_use"
          ) {
            return;
          }

          const toolName = block.name;
          const itemType = classifyToolItemType(toolName);
          const toolInput =
            typeof block.input === "object" && block.input !== null
              ? (block.input as Record<string, unknown>)
              : {};
          const itemId = block.id;
          const detail = summarizeToolRequest(toolName, toolInput);

          const tool: ToolInFlight = {
            itemId,
            itemType,
            toolName,
            title: titleForTool(itemType),
            detail,
            inputJsonChunks: [],
          };
          context.inFlightTools.set(index, tool);

          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "item.started",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            threadId: context.session.threadId,
            ...(context.turnState ? { turnId: asCanonicalTurnId(context.turnState.turnId) } : {}),
            itemId: asRuntimeItemId(tool.itemId),
            payload: {
              itemType: tool.itemType,
              status: "inProgress",
              title: tool.title,
              ...(tool.detail ? { detail: tool.detail } : {}),
              data: {
                toolName: tool.toolName,
                input: toolInput,
              },
            },
            providerRefs: {
              ...providerThreadRef(context),
              ...(context.turnState ? { providerTurnId: String(context.turnState.turnId) } : {}),
              providerItemId: ProviderItemId.makeUnsafe(tool.itemId),
            },
            raw: {
              source: "claude.sdk.message",
              method: "claude/stream_event/content_block_start",
              payload: message,
            },
          });
          return;
        }

        if (event.type === "content_block_stop") {
          const { index } = event;
          const tool = context.inFlightTools.get(index);
          if (!tool) {
            return;
          }
          context.inFlightTools.delete(index);

          // Recalculate detail from accumulated input JSON deltas
          const finalDetail = recalculateToolDetail(tool);

          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "item.completed",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            threadId: context.session.threadId,
            ...(context.turnState ? { turnId: asCanonicalTurnId(context.turnState.turnId) } : {}),
            itemId: asRuntimeItemId(tool.itemId),
            payload: {
              itemType: tool.itemType,
              status: "completed",
              title: tool.title,
              ...(finalDetail ? { detail: finalDetail } : {}),
            },
            providerRefs: {
              ...providerThreadRef(context),
              ...(context.turnState ? { providerTurnId: String(context.turnState.turnId) } : {}),
              providerItemId: ProviderItemId.makeUnsafe(tool.itemId),
            },
            raw: {
              source: "claude.sdk.message",
              method: "claude/stream_event/content_block_stop",
              payload: message,
            },
          });
        }
      });

    const handleAssistantMessage = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (message.type !== "assistant") {
          return;
        }

        if (context.turnState) {
          context.turnState.items.push(message.message);
          const fallbackAssistantText = extractAssistantText(message);
          if (
            fallbackAssistantText.length > 0 &&
            fallbackAssistantText !== context.turnState.fallbackAssistantText
          ) {
            context.turnState = {
              ...context.turnState,
              fallbackAssistantText,
            };
          }

          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "item.updated",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            threadId: context.session.threadId,
            turnId: context.turnState.turnId,
            itemId: asRuntimeItemId(context.turnState.assistantItemId),
            payload: {
              itemType: "assistant_message",
              status: "inProgress",
              title: "Assistant message",
              data: message.message,
            },
            providerRefs: {
              ...providerThreadRef(context),
              providerTurnId: context.turnState.turnId,
              providerItemId: ProviderItemId.makeUnsafe(context.turnState.assistantItemId),
            },
            raw: {
              source: "claude.sdk.message",
              method: "claude/assistant",
              payload: message,
            },
          });
        }

        context.lastAssistantUuid = message.uuid;
        yield* updateResumeCursor(context);
      });

    const handleResultMessage = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (message.type !== "result") {
          return;
        }

        const status = turnStatusFromResult(message);
        const errorMessage = message.subtype === "success" ? undefined : message.errors[0];

        yield* Effect.log(`Turn result: ${status}`).pipe(
          Effect.annotateLogs({
            threadId: String(context.session.threadId),
            status,
            ...(errorMessage ? { error: errorMessage } : {}),
          }),
        );

        if (status === "failed") {
          const isContextOverflow =
            errorMessage != null &&
            errorMessage.toLowerCase().includes("prompt is too long");
          yield* emitRuntimeError(
            context,
            errorMessage ?? "Claude turn failed.",
            undefined,
            isContextOverflow ? "context_overflow" : "provider_error",
          );
        }

        yield* completeTurn(context, status, errorMessage, message);
      });

    const handleSystemMessage = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (message.type !== "system") {
          return;
        }

        const stamp = yield* makeEventStamp();
        const base = {
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId: context.session.threadId,
          ...(context.turnState ? { turnId: asCanonicalTurnId(context.turnState.turnId) } : {}),
          providerRefs: {
            ...providerThreadRef(context),
            ...(context.turnState ? { providerTurnId: context.turnState.turnId } : {}),
          },
          raw: {
            source: "claude.sdk.message" as const,
            method: sdkNativeMethod(message),
            messageType: `${message.type}:${message.subtype}`,
            payload: message,
          },
        };

        switch (message.subtype) {
          case "init":
            yield* offerRuntimeEvent({
              ...base,
              type: "session.configured",
              payload: {
                config: message as Record<string, unknown>,
              },
            });
            return;
          case "status":
            yield* offerRuntimeEvent({
              ...base,
              type: "session.state.changed",
              payload: {
                state: message.status === "compacting" ? "waiting" : "running",
                reason: `status:${message.status ?? "active"}`,
                detail: message,
              },
            });
            return;
          case "compact_boundary":
            yield* offerRuntimeEvent({
              ...base,
              type: "thread.state.changed",
              payload: {
                state: "compacted",
                detail: message,
              },
            });
            return;
          case "hook_started":
            yield* offerRuntimeEvent({
              ...base,
              type: "hook.started",
              payload: {
                hookId: message.hook_id,
                hookName: message.hook_name,
                hookEvent: message.hook_event,
              },
            });
            return;
          case "hook_progress":
            yield* offerRuntimeEvent({
              ...base,
              type: "hook.progress",
              payload: {
                hookId: message.hook_id,
                output: message.output,
                stdout: message.stdout,
                stderr: message.stderr,
              },
            });
            return;
          case "hook_response":
            yield* offerRuntimeEvent({
              ...base,
              type: "hook.completed",
              payload: {
                hookId: message.hook_id,
                outcome: message.outcome,
                output: message.output,
                stdout: message.stdout,
                stderr: message.stderr,
                ...(typeof message.exit_code === "number" ? { exitCode: message.exit_code } : {}),
              },
            });
            return;
          case "task_started":
            yield* offerRuntimeEvent({
              ...base,
              type: "task.started",
              payload: {
                taskId: RuntimeTaskId.makeUnsafe(message.task_id),
                description: message.description,
                ...(message.task_type ? { taskType: message.task_type } : {}),
              },
            });
            return;
          case "task_progress":
            yield* offerRuntimeEvent({
              ...base,
              type: "task.progress",
              payload: {
                taskId: RuntimeTaskId.makeUnsafe(message.task_id),
                description: message.description,
                ...(message.usage ? { usage: message.usage } : {}),
                ...(message.last_tool_name ? { lastToolName: message.last_tool_name } : {}),
              },
            });
            return;
          case "task_notification":
            yield* offerRuntimeEvent({
              ...base,
              type: "task.completed",
              payload: {
                taskId: RuntimeTaskId.makeUnsafe(message.task_id),
                status: message.status,
                ...(message.summary ? { summary: message.summary } : {}),
                ...(message.usage ? { usage: message.usage } : {}),
              },
            });
            return;
          case "files_persisted":
            yield* offerRuntimeEvent({
              ...base,
              type: "files.persisted",
              payload: {
                files: Array.isArray(message.files)
                  ? message.files.map((file: { filename: string; file_id: string }) => ({
                      filename: file.filename,
                      fileId: file.file_id,
                    }))
                  : [],
                ...(Array.isArray(message.failed)
                  ? {
                      failed: message.failed.map((entry: { filename: string; error: string }) => ({
                        filename: entry.filename,
                        error: entry.error,
                      })),
                    }
                  : {}),
              },
            });
            return;
          default:
            yield* emitRuntimeWarning(
              context,
              `Unhandled Claude system message subtype '${message.subtype}'.`,
              message,
            );
            return;
        }
      });

    const handleSdkTelemetryMessage = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const stamp = yield* makeEventStamp();
        const base = {
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId: context.session.threadId,
          ...(context.turnState ? { turnId: asCanonicalTurnId(context.turnState.turnId) } : {}),
          providerRefs: {
            ...providerThreadRef(context),
            ...(context.turnState ? { providerTurnId: context.turnState.turnId } : {}),
          },
          raw: {
            source: "claude.sdk.message" as const,
            method: sdkNativeMethod(message),
            messageType: message.type,
            payload: message,
          },
        };

        if (message.type === "tool_progress") {
          yield* offerRuntimeEvent({
            ...base,
            type: "tool.progress",
            payload: {
              toolUseId: message.tool_use_id,
              toolName: message.tool_name,
              elapsedSeconds: message.elapsed_time_seconds,
              ...(message.task_id ? { summary: `task:${message.task_id}` } : {}),
            },
          });
          return;
        }

        if (message.type === "tool_use_summary") {
          yield* offerRuntimeEvent({
            ...base,
            type: "tool.summary",
            payload: {
              summary: message.summary,
              ...(message.preceding_tool_use_ids.length > 0
                ? { precedingToolUseIds: message.preceding_tool_use_ids }
                : {}),
            },
          });
          return;
        }

        if (message.type === "auth_status") {
          yield* offerRuntimeEvent({
            ...base,
            type: "auth.status",
            payload: {
              isAuthenticating: message.isAuthenticating,
              output: message.output,
              ...(message.error ? { error: message.error } : {}),
            },
          });
          return;
        }

        if (message.type === "rate_limit_event") {
          yield* offerRuntimeEvent({
            ...base,
            type: "account.rate-limits.updated",
            payload: {
              rateLimits: message,
            },
          });
          return;
        }
      });

    const handleSdkMessage = (
      context: ClaudeSessionContext,
      message: SDKMessage,
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* logNativeSdkMessage(context, message);
        yield* ensureThreadId(context, message);

        switch (message.type) {
          case "stream_event":
            yield* handleStreamEvent(context, message);
            return;
          case "user":
            return;
          case "assistant":
            yield* handleAssistantMessage(context, message);
            return;
          case "result":
            yield* handleResultMessage(context, message);
            return;
          case "system":
            yield* handleSystemMessage(context, message);
            return;
          case "tool_progress":
          case "tool_use_summary":
          case "auth_status":
          case "rate_limit_event":
            yield* handleSdkTelemetryMessage(context, message);
            return;
          default:
            yield* emitRuntimeWarning(
              context,
              `Unhandled Claude SDK message type '${message.type}'.`,
              message,
            );
            return;
        }
      });

    const runSdkStream = (context: ClaudeSessionContext): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* Effect.log("SDK stream starting");
        yield* Stream.fromAsyncIterable(context.query, (cause) => cause).pipe(
          Stream.takeWhile(() => !context.stopped),
          Stream.runForEach((message) => handleSdkMessage(context, message)),
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              if (Cause.hasInterruptsOnly(cause) || context.stopped) {
                return;
              }
              const message = toMessage(Cause.squash(cause), "Claude runtime stream failed.");
              yield* Effect.logError("SDK stream failed", { error: message });
              yield* emitRuntimeError(context, message, cause);
              yield* completeTurn(context, "failed", message);
            }),
          ),
        );
        yield* Effect.log("SDK stream ended");
        // If the stream ended cleanly but there's still an open turn, the
        // claude process exited without emitting a result message (e.g. OOM,
        // context-overflow crash, or unexpected exit with code 1).  Fail the
        // turn now so the UI doesn't hang silently.
        if (context.turnState && !context.stopped) {
          const errorMessage = "Claude process exited unexpectedly.";
          yield* Effect.logWarning("SDK stream ended with an open turn — failing turn", {
            turnId: String(context.turnState.turnId),
          });
          yield* emitRuntimeError(context, errorMessage);
          yield* completeTurn(context, "failed", errorMessage);
        }

        // Mark the query as dead so the next sendTurn can auto-restart
        // instead of crashing with "ProcessTransport is not ready".
        if (!context.stopped) {
          context.queryDead = true;
        }
      }).pipe(Effect.annotateLogs({ threadId: String(context.session.threadId) }));

    /**
     * Restarts the underlying SDK query session when the interaction mode
     * changes between turns (Option B — clean restart with resume).
     *
     * - Closes the old query and shuts down the old prompt queue.
     * - Creates a new prompt queue + query with the updated systemPrompt.
     * - Resumes from the current `resumeSessionId` / `lastAssistantUuid` so
     *   conversation history is preserved by Claude Code.
     * - Mutates `context.promptQueue`, `context.query`, and
     *   `context.interactionMode` in place so all existing closures
     *   (canUseTool, runSdkStream) keep working without re-wiring.
     */
    const restartQueryForInteractionMode = (
      context: ClaudeSessionContext,
      newInteractionMode: ProviderInteractionMode,
    ): Effect.Effect<void, ProviderAdapterProcessError> =>
      Effect.gen(function* () {
        yield* Effect.log("Restarting query for interaction mode change").pipe(
          Effect.annotateLogs({
            threadId: String(context.session.threadId),
            previousMode: context.interactionMode ?? "default",
            newMode: newInteractionMode,
          }),
        );

        // Stop the old query and drain the old prompt queue.
        context.query.close();
        yield* Queue.shutdown(context.promptQueue);

        // Build a fresh prompt queue and async-iterable prompt stream.
        const newPromptQueue = yield* Queue.unbounded<PromptQueueItem>();
        const newPrompt = buildPromptStream(newPromptQueue);

        // Build new query options reusing all session-level settings, but with
        // an updated systemPrompt and the current resume cursor.
        const systemPrompt = buildSystemPromptForInteractionMode(newInteractionMode);
        const newQueryOptions: ClaudeQueryOptions = {
          ...(context.session.cwd ? { cwd: context.session.cwd } : {}),
          ...(context.session.model ? { model: context.session.model } : {}),
          ...(context.pathToClaudeCodeExecutable
            ? { pathToClaudeCodeExecutable: context.pathToClaudeCodeExecutable }
            : {}),
          ...(context.permissionMode ? { permissionMode: context.permissionMode } : {}),
          ...(context.permissionMode === "bypassPermissions"
            ? { allowDangerouslySkipPermissions: true }
            : {}),
          ...(context.maxThinkingTokens !== undefined
            ? { maxThinkingTokens: context.maxThinkingTokens }
            : {}),
          ...(context.effort ? { effort: context.effort } : {}),
          ...(context.chrome ? { extraArgs: { chrome: null } } : {}),
          // Resume from where the last turn left off.
          ...(context.resumeSessionId ? { resume: context.resumeSessionId } : {}),
          ...(context.lastAssistantUuid ? { resumeSessionAt: context.lastAssistantUuid } : {}),
          includePartialMessages: true,
          canUseTool: context.canUseTool,
          env: process.env,
          ...(context.session.cwd ? { additionalDirectories: [context.session.cwd] } : {}),
          ...(systemPrompt !== undefined ? { systemPrompt } : {}),
        };

        const newQueryRuntime = yield* Effect.try({
          try: () => createQuery({ prompt: newPrompt, options: newQueryOptions }),
          catch: (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: context.session.threadId,
              detail: toMessage(
                cause,
                "Failed to restart Claude runtime session for interaction mode change.",
              ),
              cause,
            }),
        });

        // Swap the mutable fields on the context in place.
        context.promptQueue = newPromptQueue;
        context.query = newQueryRuntime;
        context.interactionMode = newInteractionMode;
        context.queryDead = false;

        // Fork a new SDK stream over the new query.
        Effect.runFork(runSdkStream(context));
      });

    const stopSessionInternal = (
      context: ClaudeSessionContext,
      options?: { readonly emitExitEvent?: boolean },
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (context.stopped) return;

        context.stopped = true;

        yield* Effect.log("Session stopped").pipe(
          Effect.annotateLogs({ threadId: String(context.session.threadId) }),
        );

        for (const [requestId, pending] of context.pendingApprovals) {
          yield* Deferred.succeed(pending.decision, "cancel");
          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "request.resolved",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            threadId: context.session.threadId,
            ...(context.turnState ? { turnId: asCanonicalTurnId(context.turnState.turnId) } : {}),
            requestId: asRuntimeRequestId(requestId),
            payload: {
              requestType: pending.requestType,
              decision: "cancel",
            },
            providerRefs: {
              ...providerThreadRef(context),
              ...(context.turnState ? { providerTurnId: String(context.turnState.turnId) } : {}),
              providerRequestId: requestId,
            },
          });
        }
        context.pendingApprovals.clear();

        if (context.turnState) {
          yield* completeTurn(context, "interrupted", "Session stopped.");
        }

        yield* Queue.shutdown(context.promptQueue);

        context.query.close();

        const updatedAt = yield* nowIso;
        context.session = {
          ...context.session,
          status: "closed",
          activeTurnId: undefined,
          updatedAt,
        };

        if (options?.emitExitEvent !== false) {
          const stamp = yield* makeEventStamp();
          yield* offerRuntimeEvent({
            type: "session.exited",
            eventId: stamp.eventId,
            provider: PROVIDER,
            createdAt: stamp.createdAt,
            threadId: context.session.threadId,
            payload: {
              reason: "Session stopped",
              exitKind: "graceful",
            },
            providerRefs: {},
          });
        }

        sessions.delete(context.session.threadId);
      });

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<ClaudeSessionContext, ProviderAdapterError> => {
      const context = sessions.get(threadId);
      if (!context) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          }),
        );
      }
      if (context.stopped || context.session.status === "closed") {
        return Effect.fail(
          new ProviderAdapterSessionClosedError({
            provider: PROVIDER,
            threadId,
          }),
        );
      }
      return Effect.succeed(context);
    };

    const startSession: ClaudeCodeAdapterShape["startSession"] = (input) =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }

        const startedAt = yield* nowIso;
        const resumeState = readClaudeResumeState(input.resumeCursor);
        const threadId = input.threadId;

        const promptQueue = yield* Queue.unbounded<PromptQueueItem>();
        const prompt = buildPromptStream(promptQueue);

        const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
        const inFlightTools = new Map<number, ToolInFlight>();

        const contextRef = yield* Ref.make<ClaudeSessionContext | undefined>(undefined);

        const canUseTool: CanUseTool = (toolName, toolInput, callbackOptions) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const context = yield* Ref.get(contextRef);
              if (!context) {
                return {
                  behavior: "deny",
                  message: "Claude session context is unavailable.",
                } satisfies PermissionResult;
              }

              const runtimeMode = input.runtimeMode ?? "full-access";
              if (runtimeMode === "full-access") {
                return {
                  behavior: "allow",
                  updatedInput: toolInput,
                } satisfies PermissionResult;
              }

              const requestId = ApprovalRequestId.makeUnsafe(yield* Random.nextUUIDv4);
              const requestType = classifyRequestType(toolName);
              const detail = summarizeToolRequest(toolName, toolInput);
              const decisionDeferred = yield* Deferred.make<ProviderApprovalDecision>();
              const pendingApproval: PendingApproval = {
                requestType,
                detail,
                decision: decisionDeferred,
                ...(callbackOptions.suggestions
                  ? { suggestions: callbackOptions.suggestions }
                  : {}),
              };

              const requestedStamp = yield* makeEventStamp();
              yield* offerRuntimeEvent({
                type: "request.opened",
                eventId: requestedStamp.eventId,
                provider: PROVIDER,
                createdAt: requestedStamp.createdAt,
                threadId: context.session.threadId,
                ...(context.turnState
                  ? { turnId: asCanonicalTurnId(context.turnState.turnId) }
                  : {}),
                requestId: asRuntimeRequestId(requestId),
                payload: {
                  requestType,
                  detail,
                  args: {
                    toolName,
                    input: toolInput,
                    ...(callbackOptions.toolUseID ? { toolUseId: callbackOptions.toolUseID } : {}),
                  },
                },
                providerRefs: {
                  ...(context.session.threadId
                    ? { providerThreadId: context.session.threadId }
                    : {}),
                  ...(context.turnState
                    ? { providerTurnId: String(context.turnState.turnId) }
                    : {}),
                  providerRequestId: requestId,
                },
                raw: {
                  source: "claude.sdk.permission",
                  method: "canUseTool/request",
                  payload: {
                    toolName,
                    input: toolInput,
                  },
                },
              });

              pendingApprovals.set(requestId, pendingApproval);

              const onAbort = () => {
                if (!pendingApprovals.has(requestId)) {
                  return;
                }
                pendingApprovals.delete(requestId);
                Effect.runFork(Deferred.succeed(decisionDeferred, "cancel"));
              };

              callbackOptions.signal.addEventListener("abort", onAbort, {
                once: true,
              });

              const decision = yield* Deferred.await(decisionDeferred);
              pendingApprovals.delete(requestId);

              const resolvedStamp = yield* makeEventStamp();
              yield* offerRuntimeEvent({
                type: "request.resolved",
                eventId: resolvedStamp.eventId,
                provider: PROVIDER,
                createdAt: resolvedStamp.createdAt,
                threadId: context.session.threadId,
                ...(context.turnState
                  ? { turnId: asCanonicalTurnId(context.turnState.turnId) }
                  : {}),
                requestId: asRuntimeRequestId(requestId),
                payload: {
                  requestType,
                  decision,
                },
                providerRefs: {
                  ...(context.session.threadId
                    ? { providerThreadId: context.session.threadId }
                    : {}),
                  ...(context.turnState
                    ? { providerTurnId: String(context.turnState.turnId) }
                    : {}),
                  providerRequestId: requestId,
                },
                raw: {
                  source: "claude.sdk.permission",
                  method: "canUseTool/decision",
                  payload: {
                    decision,
                  },
                },
              });

              if (decision === "accept" || decision === "acceptForSession") {
                return {
                  behavior: "allow",
                  updatedInput: toolInput,
                  ...(decision === "acceptForSession" && pendingApproval.suggestions
                    ? { updatedPermissions: [...pendingApproval.suggestions] }
                    : {}),
                } satisfies PermissionResult;
              }

              return {
                behavior: "deny",
                message:
                  decision === "cancel"
                    ? "User cancelled tool execution."
                    : "User declined tool execution.",
              } satisfies PermissionResult;
            }),
          );

        const providerOptions = input.providerOptions?.claudeCode;
        const initialEffort =
          input.modelOptions?.claudeCode?.effort ?? providerOptions?.effort;
        const chromeEnabled = providerOptions?.chrome === true;
        const permissionMode =
          toPermissionMode(providerOptions?.permissionMode) ??
          (input.runtimeMode === "full-access" ? "bypassPermissions" : undefined);

        const queryOptions: ClaudeQueryOptions = {
          ...(input.cwd ? { cwd: input.cwd } : {}),
          ...(input.model ? { model: input.model } : {}),
          ...(providerOptions?.binaryPath
            ? { pathToClaudeCodeExecutable: providerOptions.binaryPath }
            : {}),
          ...(permissionMode ? { permissionMode } : {}),
          ...(permissionMode === "bypassPermissions"
            ? { allowDangerouslySkipPermissions: true }
            : {}),
          ...(providerOptions?.maxThinkingTokens !== undefined
            ? { maxThinkingTokens: providerOptions.maxThinkingTokens }
            : {}),
          ...(initialEffort ? { effort: initialEffort } : {}),
          ...(chromeEnabled ? { extraArgs: { chrome: null } } : {}),
          ...(resumeState?.resume ? { resume: resumeState.resume } : {}),
          ...(resumeState?.resumeSessionAt ? { resumeSessionAt: resumeState.resumeSessionAt } : {}),
          includePartialMessages: true,
          canUseTool,
          env: process.env,
          ...(input.cwd ? { additionalDirectories: [input.cwd] } : {}),
        };

        yield* Effect.log("Creating SDK query").pipe(
          Effect.annotateLogs({
            threadId: String(threadId),
            hasResume: String(!!queryOptions.resume),
            hasResumeSessionAt: String(!!queryOptions.resumeSessionAt),
            ...(queryOptions.resume ? { resumeId: queryOptions.resume } : {}),
            chromeEnabled: String(chromeEnabled),
            extraArgs: JSON.stringify(queryOptions.extraArgs ?? null),
          }),
        );

        const queryRuntime = yield* Effect.try({
          try: () =>
            createQuery({
              prompt,
              options: queryOptions,
            }),
          catch: (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId,
              detail: toMessage(cause, "Failed to start Claude runtime session."),
              cause,
            }),
        });

        const session: ProviderSession = {
          threadId,
          provider: PROVIDER,
          status: "ready",
          runtimeMode: input.runtimeMode,
          ...(input.cwd ? { cwd: input.cwd } : {}),
          ...(input.model ? { model: input.model } : {}),
          resumeCursor: {
            threadId,
            ...(resumeState?.resume ? { resume: resumeState.resume } : {}),
            ...(resumeState?.resumeSessionAt
              ? { resumeSessionAt: resumeState.resumeSessionAt }
              : {}),
            turnCount: resumeState?.turnCount ?? 0,
          },
          createdAt: startedAt,
          updatedAt: startedAt,
        };

        const context: ClaudeSessionContext = {
          session,
          promptQueue,
          query: queryRuntime,
          startedAt,
          resumeSessionId: resumeState?.resume,
          pendingApprovals,
          turns: [],
          inFlightTools,
          turnState: undefined,
          lastAssistantUuid: resumeState?.resumeSessionAt,
          lastThreadStartedId: undefined,
          stopped: false,
          queryDead: false,
          interactionMode: undefined,
          canUseTool,
          permissionMode,
          maxThinkingTokens: providerOptions?.maxThinkingTokens,
          effort: initialEffort,
          pathToClaudeCodeExecutable: providerOptions?.binaryPath,
          chrome: chromeEnabled,
        };
        yield* Ref.set(contextRef, context);
        sessions.set(threadId, context);

        yield* Effect.log("Session started").pipe(
          Effect.annotateLogs({
            threadId: String(threadId),
            ...(input.model ? { model: input.model } : {}),
            runtimeMode: input.runtimeMode ?? "full-access",
            ...(input.cwd ? { cwd: input.cwd } : {}),
          }),
        );

        const sessionStartedStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "session.started",
          eventId: sessionStartedStamp.eventId,
          provider: PROVIDER,
          createdAt: sessionStartedStamp.createdAt,
          threadId,
          payload: input.resumeCursor !== undefined ? { resume: input.resumeCursor } : {},
          providerRefs: {},
        } as ProviderRuntimeEvent);

        const configuredStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "session.configured",
          eventId: configuredStamp.eventId,
          provider: PROVIDER,
          createdAt: configuredStamp.createdAt,
          threadId,
          payload: {
            config: {
              ...(input.model ? { model: input.model } : {}),
              ...(input.cwd ? { cwd: input.cwd } : {}),
              ...(permissionMode ? { permissionMode } : {}),
              ...(providerOptions?.maxThinkingTokens !== undefined
                ? { maxThinkingTokens: providerOptions.maxThinkingTokens }
                : {}),
              ...(providerOptions?.effort ? { effort: providerOptions.effort } : {}),
            },
          },
          providerRefs: {},
        } as ProviderRuntimeEvent);

        const readyStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          eventId: readyStamp.eventId,
          provider: PROVIDER,
          createdAt: readyStamp.createdAt,
          threadId,
          payload: {
            state: "ready",
          },
          providerRefs: {},
        } as ProviderRuntimeEvent);

        Effect.runFork(runSdkStream(context));

        return {
          ...session,
        };
      });

    const sendTurn: ClaudeCodeAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const context = yield* requireSession(input.threadId);

        if (context.turnState) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: `Thread '${input.threadId}' already has an active turn '${context.turnState.turnId}'.`,
          });
        }

        // If the underlying Claude process died between turns, restart it
        // transparently so the user doesn't see "ProcessTransport is not ready".
        const requestedMode = input.interactionMode ?? context.interactionMode ?? "default";
        if (context.queryDead) {
          yield* emitRuntimeWarning(
            context,
            "Claude process exited unexpectedly — restarting session.",
          );
          yield* restartQueryForInteractionMode(context, requestedMode);
        }

        // Detect interaction mode changes and restart the underlying SDK query
        // with the appropriate systemPrompt (Option B — clean restart with resume).
        const effectiveCurrentMode = context.interactionMode ?? "default";
        if (requestedMode !== effectiveCurrentMode) {
          yield* restartQueryForInteractionMode(context, requestedMode);
        } else if (context.interactionMode === undefined) {
          // First turn in default mode — no restart needed, just record the mode.
          context.interactionMode = "default";
        }

        if (input.model) {
          yield* Effect.tryPromise({
            try: () => context.query.setModel(input.model),
            catch: (cause) => toRequestError(input.threadId, "turn/setModel", cause),
          });
        }

        const requestedEffort = input.modelOptions?.claudeCode?.effort ?? undefined;
        if (requestedEffort !== undefined && requestedEffort !== context.effort) {
          context.effort = requestedEffort;
          yield* restartQueryForInteractionMode(context, requestedMode);
        }

        const turnId = TurnId.makeUnsafe(yield* Random.nextUUIDv4);
        const turnState: ClaudeTurnState = {
          turnId,
          assistantItemId: yield* Random.nextUUIDv4,
          startedAt: yield* nowIso,
          items: [],
          messageCompleted: false,
          emittedTextDelta: false,
          fallbackAssistantText: "",
        };

        const updatedAt = yield* nowIso;
        context.turnState = turnState;
        context.session = {
          ...context.session,
          status: "running",
          activeTurnId: turnId,
          updatedAt,
        };

        const turnStartedStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "turn.started",
          eventId: turnStartedStamp.eventId,
          provider: PROVIDER,
          createdAt: turnStartedStamp.createdAt,
          threadId: context.session.threadId,
          turnId,
          payload: input.model ? { model: input.model } : {},
          providerRefs: {
            providerTurnId: String(turnId),
          },
        });

        const message = yield* buildUserMessage(input, fileSystem, serverConfig.stateDir);

        yield* Queue.offer(context.promptQueue, {
          type: "message",
          message,
        }).pipe(Effect.mapError((cause) => toRequestError(input.threadId, "turn/start", cause)));

        return {
          threadId: context.session.threadId,
          turnId,
          ...(context.session.resumeCursor !== undefined
            ? { resumeCursor: context.session.resumeCursor }
            : {}),
        };
      });

    const interruptTurn: ClaudeCodeAdapterShape["interruptTurn"] = (threadId, _turnId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        yield* Effect.tryPromise({
          try: () => context.query.interrupt(),
          catch: (cause) => toRequestError(threadId, "turn/interrupt", cause),
        });
      });

    const readThread: ClaudeCodeAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        return yield* snapshotThread(context);
      });

    const rollbackThread: ClaudeCodeAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        const nextLength = Math.max(0, context.turns.length - numTurns);
        context.turns.splice(nextLength);
        yield* updateResumeCursor(context);
        return yield* snapshotThread(context);
      });

    const respondToRequest: ClaudeCodeAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        const pending = context.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "item/requestApproval/decision",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }

        context.pendingApprovals.delete(requestId);
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: ClaudeCodeAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      _answers,
    ) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "item/tool/requestUserInput",
          detail: `Claude Code does not yet support structured user-input responses for thread '${threadId}' and request '${requestId}'.`,
        }),
      );

    const stopSession: ClaudeCodeAdapterShape["stopSession"] = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        yield* stopSessionInternal(context, {
          emitExitEvent: true,
        });
      });

    const listSessions: ClaudeCodeAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), ({ session }) => ({ ...session })));

    const hasSession: ClaudeCodeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const context = sessions.get(threadId);
        return context !== undefined && !context.stopped;
      });

    const stopAll: ClaudeCodeAdapterShape["stopAll"] = () =>
      Effect.forEach(
        sessions,
        ([, context]) =>
          stopSessionInternal(context, {
            emitExitEvent: true,
          }),
        { discard: true },
      );

    yield* Effect.addFinalizer(() =>
      Effect.forEach(
        sessions,
        ([, context]) =>
          stopSessionInternal(context, {
            emitExitEvent: false,
          }),
        { discard: true },
      ).pipe(Effect.tap(() => Queue.shutdown(runtimeEventQueue))),
    );

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents: Stream.fromQueue(runtimeEventQueue),
    } satisfies ClaudeCodeAdapterShape;
  });
}

export const ClaudeCodeAdapterLive = Layer.effect(ClaudeCodeAdapter, makeClaudeCodeAdapter());

export function makeClaudeCodeAdapterLive(options?: ClaudeCodeAdapterLiveOptions) {
  return Layer.effect(ClaudeCodeAdapter, makeClaudeCodeAdapter(options));
}
