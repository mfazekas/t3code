import {
  type CommandId,
  type RuntimeRequestId,
  ThreadId,
  type OrchestrationV2RuntimeRequestResponder,
  type OrchestrationV2ThreadProjection,
  type ProviderApprovalDecision,
  ProviderRequestKind,
  type RunId,
  OrchestratorMcpFailure,
  type OrchestrationV2Command,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { McpSchema } from "effect/ai";
import { modelSelectionCommandType } from "@t3tools/shared/model";

import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import {
  dispatchFailure,
  newCommandId,
  readCaller,
  readThread,
  unavailable,
} from "../../threadAccess.ts";
import * as ThreadSearch from "../../../orchestration-v2/ThreadSearch.ts";
import * as ScheduledTasks from "../../../scheduledTasks/ScheduledTaskService.ts";
import { queuedRunsInDeliveryOrder } from "../../../orchestration-v2/QueuedRunOrder.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import { ThreadToolkit } from "./tools.ts";

function queueEntry(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "messages">,
  runId: RunId,
  limit: number,
) {
  const run = projection.runs.find((run) => run.id === runId && run.status === "queued");
  const message = projection.messages.find((message) => message.id === run?.userMessageId);
  if (run === undefined || message === undefined) return undefined;
  const characters = Array.from(message.text);
  return {
    queuedRunId: run.id,
    text: characters.slice(0, limit).join(""),
    truncated: characters.length > limit,
  };
}
const dispatch = Effect.fn("mcp.dispatchThreadCommand")(function* (
  threadId: ThreadId | undefined,
  command: (common: { commandId: CommandId; threadId: ThreadId }) => OrchestrationV2Command,
) {
  const { threads, projection } = yield* readThread(threadId);
  const result = yield* threads
    .dispatch(command({ commandId: yield* newCommandId(), threadId: projection.thread.id }))
    .pipe(Effect.mapError(dispatchFailure));
  return { sequence: result.sequence };
});

type RuntimeRequest = OrchestrationV2ThreadProjection["runtimeRequests"][number];
const isApprovalKind = Schema.is(ProviderRequestKind);
const isApprovalRequest = (request: RuntimeRequest) => isApprovalKind(request.kind);
const isPendingRequest = (request: RuntimeRequest) =>
  request.status === "pending" && (request.kind === "user_input" || isApprovalRequest(request));
const DEFAULT_APPROVAL_DECISIONS: ReadonlyArray<ProviderApprovalDecision> = [
  "cancel",
  "decline",
  "acceptForSession",
  "accept",
];
const APPROVAL_PROMPT_LIMIT = 4000;

const readPendingRequest = Effect.fn("mcp.readPendingRequest")(function* (input: {
  threadId?: ThreadId | undefined;
  requestId: RuntimeRequestId;
}) {
  const context = yield* readThread(input.threadId, ["runtimeRequests", "turnItems"]);
  const request = context.projection.runtimeRequests.find(
    (request) =>
      request.id === input.requestId &&
      (request.kind === "user_input" || isApprovalRequest(request)),
  );
  const item = context.projection.turnItems.find(
    (item) =>
      (item.type === "user_input_request" || item.type === "approval_request") &&
      item.requestId === input.requestId,
  );
  if (
    request === undefined ||
    (request.kind === "user_input"
      ? item?.type !== "user_input_request"
      : item?.type !== "approval_request")
  )
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "The pending request was not found.",
    });
  if (request.status !== "pending")
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        request.status === "resolved"
          ? "The request was already answered."
          : `The request is no longer pending (${request.status}).`,
    });
  return { ...context, request, item };
});

const mcpResponder = Effect.fn("mcp.responder")(function* () {
  const scope = yield* McpInvocationContext.McpInvocationContext;
  return {
    via: "mcp",
    ...(scope.thread === undefined ? {} : { callerThreadId: scope.thread.threadId }),
    ...(scope.client === undefined
      ? {}
      : { clientSessionId: scope.client.sessionId, clientLabel: scope.client.label }),
  } satisfies OrchestrationV2RuntimeRequestResponder;
});

const APPROVAL_CONFIRM_PROMPT_LIMIT = 500;
const decodeApprovalForm = Schema.decodeUnknownEffect(McpSchema.ElicitRequestFormParams);

/**
 * The agent can decline, but an approval needs the user's own answer: the MCP client shows the
 * request as an elicitation form, which the model cannot fill in.
 */
const confirmApprovalWithUser = Effect.fn("mcp.confirmApprovalWithUser")(function* (
  item: { readonly prompt?: string | undefined; readonly reason?: string | undefined },
  threadTitle: string,
) {
  const client = yield* McpSchema.McpServerClient;
  if (client.clientCapabilities.elicitation === undefined)
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: `${client.clientInfo.name} cannot show the user a confirmation form, so it cannot approve. Ask the user to approve in T3 Code.`,
    });
  const prompt = Array.from(item.prompt ?? "");
  const message = [
    `Approve this request in "${threadTitle}"?`,
    prompt.length === 0
      ? undefined
      : prompt.slice(0, APPROVAL_CONFIRM_PROMPT_LIMIT).join("") +
        (prompt.length > APPROVAL_CONFIRM_PROMPT_LIMIT ? "…" : ""),
    item.reason === undefined ? undefined : `Why it asks: ${item.reason}`,
  ]
    .filter((line) => line !== undefined)
    .join("\n\n");
  const request = yield* decodeApprovalForm({
    mode: "form",
    message,
    requestedSchema: {
      type: "object",
      properties: {
        approve: {
          type: "boolean",
          title: "Approve",
          description: "Check to approve. Your answer goes to T3 Code, not to the agent.",
        },
      },
      required: ["approve"],
    },
  }).pipe(Effect.orDie);
  const answer = yield* client.getClient.pipe(
    Effect.flatMap((reverse) => reverse.elicit(request)),
    Effect.scoped,
    Effect.catchTags({
      McpReverseOperationError: () => Effect.void,
      McpReverseOperationUnsupported: () => Effect.void,
    }),
  );
  if (answer?.action !== "accept" || answer.content?.approve !== true)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "The user did not approve. The request is still pending.",
    });
});

/** Answering an approval is opt-in per environment; approving also needs the user's own answer. */
const approvalDecision = Effect.fn("mcp.approvalDecision")(function* (
  decision: ProviderApprovalDecision | undefined,
  item: {
    readonly prompt?: string | undefined;
    readonly reason?: string | undefined;
    readonly options?: ReadonlyArray<{ readonly decision: ProviderApprovalDecision }> | undefined;
  },
  threadTitle: string,
) {
  const settings = yield* ServerSettings.ServerSettingsService.pipe(
    Effect.flatMap((service) => service.getSettings),
    Effect.mapError(unavailable),
  );
  if (!settings.allowMcpApprovalResponses)
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message:
        "Answering permission requests over MCP is off. The user can turn it on in Settings; until then ask them to answer in T3 Code.",
    });
  if (decision === undefined)
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "An approval needs a `decision`.",
    });
  const offered = item.options?.map((option) => option.decision);
  if (!(offered ?? DEFAULT_APPROVAL_DECISIONS).includes(decision))
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message: "That decision was not offered for this approval.",
    });
  const approving = decision !== "decline" && decision !== "cancel";
  if (approving) yield* confirmApprovalWithUser(item, threadTitle);
  return { decision, userConfirmed: approving };
});

/** A field the chosen action needs. `null` counts as given, as in `beforeRunId: null`. */
const required = <A>(value: A | undefined, name: string) =>
  value === undefined
    ? Effect.fail(
        new OrchestratorMcpFailure({
          code: "invalid_request",
          message: `This action needs \`${name}\`.`,
        }),
      )
    : Effect.succeed(value);
const writesThread = <P extends { readonly threadId?: ThreadId | undefined }, A, E, R>(
  handle: (params: P) => Effect.Effect<A, E, R>,
) => McpToolAccess.writesThreads((params: P) => [params.threadId], handle);

export const layer = McpToolAccess.toLayer(ThreadToolkit, {
  run_scheduled_task_now: McpToolAccess.writesEnvironment((input) =>
    Effect.gen(function* () {
      const scheduler = yield* ScheduledTasks.ScheduledTaskService;
      const { tasks } = yield* scheduler.list().pipe(Effect.mapError(unavailable));
      if (!tasks.some((task) => task.id === input.taskId))
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The scheduled task was not found.",
        });
      const { task, threadId } = yield* scheduler
        .runNow({ id: input.taskId })
        .pipe(
          Effect.mapError(
            (error) =>
              new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message }),
          ),
        );
      const scope = yield* McpInvocationContext.McpInvocationContext;
      return {
        taskId: task.id,
        threadId,
        ...(threadId === null
          ? {}
          : {
              link: McpInvocationContext.threadLinkFor(scope, threadId),
            }),
        lastRunStatus: task.lastRunStatus,
        runCount: task.runCount,
        nextRunAt: task.nextRunAt,
      };
    }),
  ),
  t3_thread_search: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const { caller } = yield* readCaller();
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const threadSearch = yield* ThreadSearch.ThreadSearch;
      const { matches } = yield* threadSearch
        .searchThreads({ ...input, projectId: input.projectId ?? caller?.projectId })
        .pipe(Effect.mapError(unavailable));
      return {
        matches: matches.map((match) => ({
          ...match,
          link: McpInvocationContext.threadLinkFor(scope, match.threadId),
        })),
      };
    }),
  ),
  t3_thread_transfer: McpToolAccess.either(
    (input) => input.action === "status",
    McpToolAccess.reads((input) =>
      Effect.gen(function* () {
        const { projection } = yield* readThread(input.threadId, ["contextTransfers"]);
        return {
          transfers: projection.contextTransfers.map(
            ({ id, sourceThreadId, targetThreadId, status }) => ({
              id,
              sourceThreadId,
              targetThreadId,
              status,
            }),
          ),
        };
      }),
    ),
    McpToolAccess.writesThreads(
      (input) => [input.threadId, input.targetThreadId],
      (input) =>
        Effect.gen(function* () {
          const sourcePoint = input.sourcePoint ?? { type: "latest_stable" as const };
          const { threads, projection } = yield* readThread(input.threadId);
          const commandId = yield* newCommandId();
          if (input.action === "merge_back") {
            const targetThreadId = yield* required(input.targetThreadId, "targetThreadId");
            yield* readThread(targetThreadId);
            const result = yield* threads
              .dispatch({
                type: "thread.merge_back",
                commandId,
                sourceThreadId: projection.thread.id,
                targetThreadId,
                sourcePoint,
                createdBy: "agent",
                creationSource: "mcp",
              })
              .pipe(Effect.mapError(dispatchFailure));
            return { sequence: result.sequence, targetThreadId };
          }
          const targetThreadId = ThreadId.make(`${commandId}:fork`);
          const result = yield* threads
            .dispatch({
              type: "thread.fork",
              commandId,
              sourceThreadId: projection.thread.id,
              targetThreadId,
              sourcePoint,
              ...(input.title === undefined ? {} : { title: input.title }),
              createdBy: "agent",
              creationSource: "mcp",
            })
            .pipe(Effect.mapError(dispatchFailure));
          return { sequence: result.sequence, targetThreadId };
        }),
    ),
  ),
  t3_thread_configure: McpToolAccess.either(
    (input) => input.modelSelection === undefined,
    McpToolAccess.reads((input) =>
      Effect.gen(function* () {
        const {
          projection: { thread },
        } = yield* readThread(input.threadId);
        return {
          threadId: thread.id,
          modelSelection: thread.modelSelection,
          runtimeMode: thread.runtimeMode,
          interactionMode: thread.interactionMode,
        };
      }),
    ),
    writesThread((input) =>
      Effect.gen(function* () {
        const modelSelection = yield* required(input.modelSelection, "modelSelection");
        const {
          threads,
          projection: { thread },
        } = yield* readThread(input.threadId);
        const type = modelSelectionCommandType(thread.providerInstanceId, modelSelection);
        const result = yield* threads
          .dispatch({ type, threadId: thread.id, commandId: yield* newCommandId(), modelSelection })
          .pipe(Effect.mapError(dispatchFailure));
        return { sequence: result.sequence };
      }),
    ),
  ),
  t3_pending_request: McpToolAccess.either(
    (input) => input.action !== "respond",
    McpToolAccess.reads((input) =>
      Effect.gen(function* () {
        if (input.action === "list") {
          const { projection } = yield* readThread(input.threadId, ["runtimeRequests"]);
          const pending = projection.runtimeRequests.filter(isPendingRequest);
          return {
            requestIds: pending
              .filter((request) => request.kind === "user_input")
              .map((request) => request.id),
            approvals: pending
              .filter(isApprovalRequest)
              .map((request) => ({ requestId: request.id, kind: request.kind })),
          };
        }
        const requestId = yield* required(input.requestId, "requestId");
        const { request, item } = yield* readPendingRequest({
          threadId: input.threadId,
          requestId,
        });
        if (item?.type === "user_input_request") return { requestId, questions: item.questions };
        const approval = item?.type === "approval_request" ? item : undefined;
        const prompt = approval?.prompt === undefined ? undefined : Array.from(approval.prompt);
        return {
          requestId,
          kind: request.kind,
          ...(prompt === undefined
            ? {}
            : { prompt: prompt.slice(0, APPROVAL_PROMPT_LIMIT).join("") }),
          ...(prompt !== undefined && prompt.length > APPROVAL_PROMPT_LIMIT
            ? { promptTruncated: true }
            : {}),
          ...(approval?.reason === undefined ? {} : { reason: approval.reason }),
          ...(approval?.appName === undefined ? {} : { appName: approval.appName }),
          ...(approval?.options === undefined ? {} : { options: approval.options }),
        };
      }),
    ),
    writesThread((input) =>
      Effect.gen(function* () {
        const requestId = yield* required(input.requestId, "requestId");
        const { threads, projection, request, item } = yield* readPendingRequest({
          threadId: input.threadId,
          requestId,
        });
        const responder = yield* mcpResponder();
        let response;
        if (request.kind === "user_input") {
          response = { answers: yield* required(input.answers, "answers"), respondedBy: responder };
        } else {
          const { decision, userConfirmed } = yield* approvalDecision(
            input.decision,
            item?.type === "approval_request" ? item : {},
            projection.thread.title,
          );
          response = {
            decision,
            respondedBy: userConfirmed
              ? { ...responder, confirmedBy: "elicitation" as const }
              : responder,
          };
        }
        const result = yield* threads
          .dispatch({
            type: "runtime-request.respond",
            threadId: projection.thread.id,
            commandId: yield* newCommandId(),
            requestId,
            ...response,
          })
          .pipe(Effect.mapError(dispatchFailure));
        return { sequence: result.sequence };
      }),
    ),
  ),
  t3_queue: McpToolAccess.either(
    (input) => input.action === "list" || input.action === "read",
    McpToolAccess.reads((input) =>
      Effect.gen(function* () {
        const { projection } = yield* readThread(input.threadId, ["runs", "messages"]);
        if (input.action === "list") {
          const runs = queuedRunsInDeliveryOrder(projection);
          const cursor = input.cursor ?? 0;
          const end = cursor + (input.limit ?? 20);
          return {
            items: runs.slice(cursor, end).flatMap((run) => {
              const entry = queueEntry(projection, run.id, 1000);
              return entry === undefined ? [] : [entry];
            }),
            nextCursor: end < runs.length ? end : null,
          };
        }
        const queuedRunId = yield* required(input.queuedRunId, "queuedRunId");
        return (
          queueEntry(projection, queuedRunId, 16000) ??
          (yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "The queued message was not found.",
          }))
        );
      }),
    ),
    writesThread((input) =>
      Effect.gen(function* () {
        const runId = yield* required(input.queuedRunId, "queuedRunId");
        switch (input.action) {
          case "edit": {
            const text = yield* required(input.text, "text");
            return yield* dispatch(input.threadId, (common) => ({
              ...common,
              type: "queued-run.edit",
              runId,
              text,
            }));
          }
          case "reorder": {
            const beforeRunId = yield* required(input.beforeRunId, "beforeRunId");
            return yield* dispatch(input.threadId, (common) => ({
              ...common,
              type: "queued-run.reorder",
              runId,
              beforeRunId,
            }));
          }
          case "promote_to_steer": {
            const targetRunId = yield* required(input.targetRunId, "targetRunId");
            return yield* dispatch(input.threadId, (common) => ({
              ...common,
              type: "queued-message.promote-to-steer",
              queuedRunId: runId,
              targetRunId,
            }));
          }
          default:
            return yield* dispatch(input.threadId, (common) => ({
              ...common,
              type: "queued-run.cancel",
              runId,
            }));
        }
      }),
    ),
  ),
  t3_thread_organize: writesThread((input) =>
    Effect.gen(function* () {
      const { threads, projection, caller } = yield* readThread(input.threadId);
      const common = { commandId: yield* newCommandId(), threadId: projection.thread.id };
      if (input.action === "settle") {
        return yield* threads
          .settleThread({ ...common, byOwnAgent: caller?.id === projection.thread.id })
          .pipe(Effect.mapError(dispatchFailure));
      }
      let command: OrchestrationV2Command;
      switch (input.action) {
        case "snooze":
          if (input.snoozedUntil === undefined) {
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "snooze requires snoozedUntil.",
            });
          }
          command = { ...common, type: "thread.snooze", snoozedUntil: input.snoozedUntil };
          break;
        case "unsnooze":
        case "unsettle":
          command = { ...common, type: `thread.${input.action}`, reason: "user" };
          break;
        case "mark_unread":
          command = { ...common, type: "thread.mark-unread" };
          break;
        default:
          command = { ...common, type: `thread.${input.action}` };
      }
      const result = yield* threads.dispatch(command).pipe(Effect.mapError(dispatchFailure));
      return { sequence: result.sequence };
    }),
  ),
});
