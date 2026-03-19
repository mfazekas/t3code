import { memo } from "react";
import { Alert, AlertAction, AlertDescription } from "../ui/alert";
import { CircleAlertIcon, XIcon } from "lucide-react";

export const ThreadErrorBanner = memo(function ThreadErrorBanner({
  error,
  onDismiss,
  onRollBack,
}: {
  error: string | null;
  onDismiss?: () => void;
  onRollBack?: () => Promise<void>;
}) {
  if (!error) return null;
  const isContextOverflow = error.toLowerCase().includes("context") || error.toLowerCase().includes("too long");
  return (
    <div className="pt-3 mx-auto max-w-3xl">
      <Alert variant="error">
        <CircleAlertIcon />
        <AlertDescription className="line-clamp-3" title={error}>
          {error}
        </AlertDescription>
        <AlertAction>
          <div className="flex items-center gap-1">
            {isContextOverflow && onRollBack && (
              <button
                type="button"
                className="inline-flex items-center rounded-md px-2 py-1 text-xs font-medium text-destructive/80 transition-colors hover:bg-destructive/10 hover:text-destructive"
                onClick={() => void onRollBack()}
              >
                Roll back &amp; retry
              </button>
            )}
            {onDismiss && (
              <button
                type="button"
                aria-label="Dismiss error"
                className="inline-flex size-6 items-center justify-center rounded-md text-destructive/60 transition-colors hover:text-destructive"
                onClick={onDismiss}
              >
                <XIcon className="size-3.5" />
              </button>
            )}
          </div>
        </AlertAction>
      </Alert>
    </div>
  );
});
