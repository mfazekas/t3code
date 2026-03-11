import { Effect, Layer, Option, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";

import { TextGenerationError } from "../Errors.ts";
import {
  type BranchNameGenerationInput,
  type BranchNameGenerationResult,
  type CommitMessageGenerationResult,
  type PrContentGenerationResult,
  type TextGenerationShape,
  TextGeneration,
} from "../Services/TextGeneration.ts";

const CLAUDE_TIMEOUT_MS = 60_000;

function normalizeClaudeError(
  operation: string,
  error: unknown,
  fallback: string,
): TextGenerationError {
  if (Schema.is(TextGenerationError)(error)) {
    return error;
  }

  if (error instanceof Error) {
    const lower = error.message.toLowerCase();
    if (
      error.message.includes("Command not found: claude") ||
      lower.includes("spawn claude") ||
      lower.includes("enoent")
    ) {
      return new TextGenerationError({
        operation,
        detail: "Claude CLI (`claude`) is required but not available on PATH.",
        cause: error,
      });
    }
    return new TextGenerationError({
      operation,
      detail: `${fallback}: ${error.message}`,
      cause: error,
    });
  }

  return new TextGenerationError({
    operation,
    detail: fallback,
    cause: error,
  });
}

function toClaudeJsonSchema(schema: Schema.Top): string {
  const document = Schema.toJsonSchemaDocument(schema);
  const jsonSchema =
    document.definitions && Object.keys(document.definitions).length > 0
      ? { ...document.schema, $defs: document.definitions }
      : document.schema;
  return JSON.stringify(jsonSchema);
}

function limitSection(value: string, maxChars: number): string {
  if (value.length <= maxChars) return value;
  const truncated = value.slice(0, maxChars);
  return `${truncated}\n\n[truncated]`;
}

function sanitizeCommitSubject(raw: string): string {
  const singleLine = raw.trim().split(/\r?\n/g)[0]?.trim() ?? "";
  const withoutTrailingPeriod = singleLine.replace(/[.]+$/g, "").trim();
  if (withoutTrailingPeriod.length === 0) {
    return "Update project files";
  }
  if (withoutTrailingPeriod.length <= 72) {
    return withoutTrailingPeriod;
  }
  return withoutTrailingPeriod.slice(0, 72).trimEnd();
}

function sanitizePrTitle(raw: string): string {
  const singleLine = raw.trim().split(/\r?\n/g)[0]?.trim() ?? "";
  if (singleLine.length > 0) {
    return singleLine;
  }
  return "Update project changes";
}

/** Parse the result value from `claude --output-format json` wrapper output.
 *
 * The CLI may emit either a single JSON object or NDJSON (one JSON object per
 * line, e.g. when streaming internally).  In both cases we look for an entry
 * with `"type": "result"` and return its `result` string field.
 */
function extractResultText(rawOutput: string): string {
  // Fast path: the whole output is a single JSON wrapper object.
  try {
    const wrapper = JSON.parse(rawOutput) as Record<string, unknown>;
    if (typeof wrapper.result === "string") {
      return wrapper.result;
    }
  } catch {
    // Not a single JSON object — fall through to NDJSON handling.
  }

  // Slow path: NDJSON — scan lines from the bottom for the result entry.
  const lines = rawOutput.split("\n");
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i]?.trim();
    if (!line) continue;
    try {
      const obj = JSON.parse(line) as Record<string, unknown>;
      if (obj.type === "result" && typeof obj.result === "string") {
        return obj.result;
      }
    } catch {
      // Skip non-JSON lines.
    }
  }

  // Final fallback: return raw output and let the caller handle the error.
  return rawOutput;
}

/**
 * Strip a markdown code fence if Claude wrapped its JSON response in one.
 * e.g. "```json\n{...}\n```" → "{...}"
 */
function stripCodeFence(text: string): string {
  const match = text.match(/^```(?:json)?\s*\n([\s\S]*?)\n```\s*$/);
  if (match?.[1] != null) return match[1];
  return text;
}

const makeClaudeCodeTextGeneration = Effect.gen(function* () {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const readStreamAsString = <E>(
    operation: string,
    stream: Stream.Stream<Uint8Array, E>,
  ): Effect.Effect<string, TextGenerationError> =>
    Effect.gen(function* () {
      let text = "";
      yield* Stream.runForEach(stream, (chunk) =>
        Effect.sync(() => {
          text += Buffer.from(chunk).toString("utf8");
        }),
      ).pipe(
        Effect.mapError((cause) =>
          normalizeClaudeError(operation, cause, "Failed to collect process output"),
        ),
      );
      return text;
    });

  const runClaudeJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
  }: {
    operation: "generateCommitMessage" | "generatePrContent" | "generateBranchName";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const schemaArg = toClaudeJsonSchema(outputSchemaJson);

      const command = ChildProcess.make(
        "claude",
        [
          "--print",
          "--input-format",
          "text",
          "--output-format",
          "json",
          "--json-schema",
          schemaArg,
          "--no-session-persistence",
          "--dangerously-skip-permissions",
        ],
        {
          cwd,
          shell: process.platform === "win32",
          stdin: {
            stream: Stream.make(new TextEncoder().encode(prompt)),
          },
        },
      );

      const child = yield* commandSpawner
        .spawn(command)
        .pipe(
          Effect.mapError((cause) =>
            normalizeClaudeError(operation, cause, "Failed to spawn Claude CLI process"),
          ),
        );

      const [stdout, stderr, exitCode] = yield* Effect.all(
        [
          readStreamAsString(operation, child.stdout),
          readStreamAsString(operation, child.stderr),
          child.exitCode.pipe(
            Effect.map((value) => Number(value)),
            Effect.mapError((cause) =>
              normalizeClaudeError(operation, cause, "Failed to read Claude CLI exit code"),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );

      if (exitCode !== 0) {
        const stderrDetail = stderr.trim();
        const stdoutDetail = stdout.trim();
        const detail = stderrDetail.length > 0 ? stderrDetail : stdoutDetail;
        return yield* Effect.fail(
          new TextGenerationError({
            operation,
            detail:
              detail.length > 0
                ? `Claude CLI command failed: ${detail}`
                : `Claude CLI command failed with code ${exitCode}.`,
          }),
        );
      }

      // claude --output-format json wraps the response: { "result": "<text>", ... }
      // When --json-schema is used, the result text is the JSON matching that schema.
      // The CLI may emit NDJSON (one object per line) or a single JSON object.
      // Claude may also wrap its JSON in a markdown code fence; strip that too.
      const resultText = stripCodeFence(extractResultText(stdout.trim()));

      return yield* Effect.flatMap(
        Effect.try({
          try: () => JSON.parse(resultText) as unknown,
          catch: (e) => {
            const stderrHint = stderr.trim();
            const detail = [
              `Claude returned invalid JSON for operation "${operation}".`,
              stderrHint.length > 0 ? `stderr: ${stderrHint.slice(0, 300)}` : null,
              `result (first 500 chars): ${resultText.slice(0, 500)}`,
            ]
              .filter(Boolean)
              .join("\n");
            return new TextGenerationError({ operation, detail, cause: e });
          },
        }),
        (parsed) =>
          Schema.decodeEffect(outputSchemaJson)(parsed).pipe(
            Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(
                new TextGenerationError({
                  operation,
                  detail: "Claude returned unexpected output structure.",
                  cause,
                }),
              ),
            ),
          ),
      );
    }).pipe(
      Effect.scoped,
      Effect.timeoutOption(CLAUDE_TIMEOUT_MS),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new TextGenerationError({
                operation: "generateCommitMessage",
                detail: "Claude CLI request timed out.",
              }),
            ),
          onSome: (value) => Effect.succeed(value),
        }),
      ),
    );

  const generateCommitMessage: TextGenerationShape["generateCommitMessage"] = (input) => {
    const wantsBranch = input.includeBranch === true;

    const prompt = [
      "You write concise git commit messages.",
      wantsBranch
        ? "Return a JSON object with keys: subject, body, branch."
        : "Return a JSON object with keys: subject, body.",
      "Rules:",
      "- subject must be imperative, <= 72 chars, and no trailing period",
      "- body can be empty string or short bullet points",
      ...(wantsBranch
        ? ["- branch must be a short semantic git branch fragment for this change"]
        : []),
      "- capture the primary user-visible or developer-visible change",
      "",
      `Branch: ${input.branch ?? "(detached)"}`,
      "",
      "Staged files:",
      limitSection(input.stagedSummary, 6_000),
      "",
      "Staged patch:",
      limitSection(input.stagedPatch, 40_000),
    ].join("\n");

    const outputSchemaJson = wantsBranch
      ? Schema.Struct({
          subject: Schema.String,
          body: Schema.String,
          branch: Schema.String,
        })
      : Schema.Struct({
          subject: Schema.String,
          body: Schema.String,
        });

    return runClaudeJson({
      operation: "generateCommitMessage",
      cwd: input.cwd,
      prompt,
      outputSchemaJson,
    }).pipe(
      Effect.map(
        (generated) =>
          ({
            subject: sanitizeCommitSubject(generated.subject),
            body: generated.body.trim(),
            ...("branch" in generated && typeof generated.branch === "string"
              ? { branch: sanitizeFeatureBranchName(generated.branch) }
              : {}),
          }) satisfies CommitMessageGenerationResult,
      ),
    );
  };

  const generatePrContent: TextGenerationShape["generatePrContent"] = (input) => {
    const prompt = [
      "You write GitHub pull request content.",
      "Return a JSON object with keys: title, body.",
      "Rules:",
      "- title should be concise and specific",
      "- body must be markdown and include headings '## Summary' and '## Testing'",
      "- under Summary, provide short bullet points",
      "- under Testing, include bullet points with concrete checks or 'Not run' where appropriate",
      "",
      `Base branch: ${input.baseBranch}`,
      `Head branch: ${input.headBranch}`,
      "",
      "Commits:",
      limitSection(input.commitSummary, 12_000),
      "",
      "Diff stat:",
      limitSection(input.diffSummary, 12_000),
      "",
      "Diff patch:",
      limitSection(input.diffPatch, 40_000),
    ].join("\n");

    return runClaudeJson({
      operation: "generatePrContent",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: Schema.Struct({
        title: Schema.String,
        body: Schema.String,
      }),
    }).pipe(
      Effect.map(
        (generated) =>
          ({
            title: sanitizePrTitle(generated.title),
            body: generated.body.trim(),
          }) satisfies PrContentGenerationResult,
      ),
    );
  };

  const generateBranchName: TextGenerationShape["generateBranchName"] = (input) => {
    const promptSections = [
      "You generate concise git branch names.",
      "Return a JSON object with key: branch.",
      "Rules:",
      "- Branch should describe the requested work from the user message.",
      "- Keep it short and specific (2-6 words).",
      "- Use plain words only, no issue prefixes and no punctuation-heavy text.",
      "",
      "User message:",
      limitSection(input.message, 8_000),
    ];

    const prompt = promptSections.join("\n");

    return runClaudeJson({
      operation: "generateBranchName",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: Schema.Struct({
        branch: Schema.String,
      }),
    }).pipe(
      Effect.map(
        (generated) =>
          ({
            branch: sanitizeBranchFragment(generated.branch),
          }) satisfies BranchNameGenerationResult,
      ),
    );
  };

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
  } satisfies TextGenerationShape;
});

export const ClaudeCodeTextGenerationLive = Layer.effect(
  TextGeneration,
  makeClaudeCodeTextGeneration,
);
