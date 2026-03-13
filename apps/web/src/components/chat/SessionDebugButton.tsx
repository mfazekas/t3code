import { useState } from "react";
import { CheckIcon, CopyIcon, InfoIcon } from "lucide-react";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { Popover, PopoverTrigger, PopoverPopup } from "../ui/popover";

export interface SessionDebugInfo {
  threadId: string;
  provider: string;
  model: string;
  orchestrationStatus: string;
  activeTurnId: string | undefined;
  sessionId: string | undefined;
  cwd: string | null;
}

export function SessionDebugButton({ info }: { info: SessionDebugInfo }) {
  const [copiedField, setCopiedField] = useState<string | null>(null);

  const copyToClipboard = (value: string, field: string) => {
    void navigator.clipboard.writeText(value).then(() => {
      setCopiedField(field);
      setTimeout(() => setCopiedField(null), 1500);
    });
  };

  const rows: Array<{ label: string; value: string | null | undefined; copyable?: boolean }> = [
    { label: "Session ID", value: info.sessionId, copyable: true },
    { label: "Thread ID", value: info.threadId, copyable: true },
    { label: "Provider", value: info.provider },
    { label: "Model", value: info.model },
    { label: "Status", value: info.orchestrationStatus },
    { label: "Active Turn", value: info.activeTurnId ?? "\u2014" },
    { label: "CWD", value: info.cwd ?? "\u2014", copyable: !!info.cwd },
  ];

  return (
    <Popover>
      <Tooltip>
        <TooltipTrigger
          render={
            <PopoverTrigger
              className="inline-flex size-7 shrink-0 items-center justify-center rounded-md border border-input bg-transparent text-muted-foreground shadow-xs hover:bg-accent hover:text-accent-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              aria-label="Session debug info"
            >
              <InfoIcon className="size-3.5" />
            </PopoverTrigger>
          }
        />
        <TooltipPopup side="bottom">Session debug info</TooltipPopup>
      </Tooltip>
      <PopoverPopup side="bottom" align="end" className="w-80">
        <div className="flex flex-col gap-3">
          <p className="text-xs font-semibold text-foreground">Session Debug Info</p>
          <div className="flex flex-col gap-1.5">
            {rows.map(({ label, value, copyable }) => (
              <div key={label} className="flex items-start justify-between gap-2">
                <span className="shrink-0 text-xs text-muted-foreground">{label}</span>
                <div className="flex min-w-0 items-center gap-1">
                  <span
                    className="min-w-0 truncate text-right font-mono text-xs text-foreground"
                    title={value ?? undefined}
                  >
                    {value ?? <span className="text-muted-foreground/60 italic">none</span>}
                  </span>
                  {copyable && value && value !== "\u2014" && (
                    <button
                      type="button"
                      className="shrink-0 text-muted-foreground hover:text-foreground"
                      onClick={() => copyToClipboard(value, label)}
                      aria-label={`Copy ${label}`}
                    >
                      {copiedField === label ? (
                        <CheckIcon className="size-3" />
                      ) : (
                        <CopyIcon className="size-3" />
                      )}
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
          {info.sessionId && (
            <div className="rounded-md bg-muted px-2.5 py-2">
              <p className="mb-1 text-xs text-muted-foreground">Resume in terminal</p>
              <code className="break-all text-xs text-foreground">
                claude --resume {info.sessionId}
              </code>
            </div>
          )}
        </div>
      </PopoverPopup>
    </Popover>
  );
}
