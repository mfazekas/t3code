import { type ModelSlug, type ProviderKind, type ServerProviderStatus } from "@t3tools/contracts";
import { normalizeModelSlug } from "@t3tools/shared/model";
import { memo, useMemo, useState } from "react";
import { type ProviderPickerKind, PROVIDER_OPTIONS } from "../../session-logic";
import { ChevronDownIcon } from "lucide-react";
import { Button } from "../ui/button";
import {
  Menu,
  MenuGroup,
  MenuItem,
  MenuPopup,
  MenuRadioGroup,
  MenuRadioItem,
  MenuSeparator as MenuDivider,
  MenuSub,
  MenuSubPopup,
  MenuSubTrigger,
  MenuTrigger,
} from "../ui/menu";
import { ClaudeAI, CursorIcon, Gemini, Icon, OpenAI, OpenCodeIcon } from "../Icons";
import { cn } from "~/lib/utils";

function isAvailableProviderOption(option: (typeof PROVIDER_OPTIONS)[number]): option is {
  value: ProviderKind;
  label: string;
  available: true;
} {
  return option.available && option.value !== "claudeCode";
}

function resolveModelForProviderPicker(
  provider: ProviderKind,
  value: string,
  options: ReadonlyArray<{ slug: string; name: string }>,
): ModelSlug | null {
  const trimmedValue = value.trim();
  if (!trimmedValue) {
    return null;
  }

  const direct = options.find((option) => option.slug === trimmedValue);
  if (direct) {
    return direct.slug;
  }

  const byName = options.find((option) => option.name.toLowerCase() === trimmedValue.toLowerCase());
  if (byName) {
    return byName.slug;
  }

  const normalized = normalizeModelSlug(trimmedValue, provider);
  if (!normalized) {
    return null;
  }

  const resolved = options.find((option) => option.slug === normalized);
  if (resolved) {
    return resolved.slug;
  }

  return null;
}

const PROVIDER_ICON_BY_PROVIDER: Record<ProviderPickerKind, Icon> = {
  codex: OpenAI,
  claudeCode: ClaudeAI,
  cursor: CursorIcon,
};

export const AVAILABLE_PROVIDER_OPTIONS = PROVIDER_OPTIONS.filter(isAvailableProviderOption);

export function splitProviderOptionsByAvailability(
  statuses?: ReadonlyArray<ServerProviderStatus>,
) {
  const available: Array<{ value: ProviderKind; label: string }> = [];
  const unavailable: Array<{ value: ProviderPickerKind; label: string }> = [];
  for (const option of PROVIDER_OPTIONS) {
    const status = statuses?.find((s) => s.provider === option.value);
    if (status?.available) {
      available.push(option as { value: ProviderKind; label: string });
    } else {
      unavailable.push(option);
    }
  }
  return { available, unavailable };
}
const COMING_SOON_PROVIDER_OPTIONS = [
  { id: "opencode", label: "OpenCode", icon: OpenCodeIcon },
  { id: "gemini", label: "Gemini", icon: Gemini },
] as const;

export const ProviderModelPicker = memo(function ProviderModelPicker(props: {
  provider: ProviderKind;
  model: ModelSlug;
  lockedProvider: ProviderKind | null;
  modelOptionsByProvider: Record<ProviderKind, ReadonlyArray<{ slug: string; name: string }>>;
  providerStatuses?: ReadonlyArray<ServerProviderStatus>;
  compact?: boolean;
  disabled?: boolean;
  onProviderModelChange: (provider: ProviderKind, model: ModelSlug) => void;
}) {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const selectedProviderOptions = props.modelOptionsByProvider[props.provider];
  const selectedModelLabel =
    selectedProviderOptions.find((option) => option.slug === props.model)?.name ?? props.model;
  const ProviderIcon = PROVIDER_ICON_BY_PROVIDER[props.provider];
  const { available: availableProviderOptions, unavailable: unavailableProviderOptions } = useMemo(
    () => splitProviderOptionsByAvailability(props.providerStatuses),
    [props.providerStatuses],
  );

  return (
    <Menu
      open={isMenuOpen}
      onOpenChange={(open) => {
        if (props.disabled) {
          setIsMenuOpen(false);
          return;
        }
        setIsMenuOpen(open);
      }}
    >
      <MenuTrigger
        render={
          <Button
            size="sm"
            variant="ghost"
            className={cn(
              "min-w-0 shrink-0 whitespace-nowrap px-2 text-muted-foreground/70 hover:text-foreground/80",
              props.compact ? "max-w-42" : "sm:px-3",
            )}
            disabled={props.disabled}
          />
        }
      >
        <span
          className={cn("flex min-w-0 items-center gap-2", props.compact ? "max-w-36" : undefined)}
        >
          <ProviderIcon aria-hidden="true" className="size-4 shrink-0 text-muted-foreground/70" />
          <span className="truncate">{selectedModelLabel}</span>
          <ChevronDownIcon aria-hidden="true" className="size-3 opacity-60" />
        </span>
      </MenuTrigger>
      <MenuPopup align="start">
        {availableProviderOptions.map((option) => {
          const OptionIcon = PROVIDER_ICON_BY_PROVIDER[option.value];
          const isDisabledByProviderLock =
            props.lockedProvider !== null && props.lockedProvider !== option.value;
          return (
            <MenuSub key={option.value}>
              <MenuSubTrigger disabled={isDisabledByProviderLock}>
                <OptionIcon
                  aria-hidden="true"
                  className="size-4 shrink-0 text-muted-foreground/85"
                />
                {option.label}
              </MenuSubTrigger>
              <MenuSubPopup className="[--available-height:min(24rem,70vh)]">
                <MenuGroup>
                  <MenuRadioGroup
                    value={props.provider === option.value ? props.model : ""}
                    onValueChange={(value) => {
                      if (props.disabled) return;
                      if (isDisabledByProviderLock) return;
                      if (!value) return;
                      const resolvedModel = resolveModelForProviderPicker(
                        option.value,
                        value,
                        props.modelOptionsByProvider[option.value],
                      );
                      if (!resolvedModel) return;
                      props.onProviderModelChange(option.value, resolvedModel);
                      setIsMenuOpen(false);
                    }}
                  >
                    {props.modelOptionsByProvider[option.value].map((modelOption) => (
                      <MenuRadioItem
                        key={`${option.value}:${modelOption.slug}`}
                        value={modelOption.slug}
                        onClick={() => setIsMenuOpen(false)}
                      >
                        {modelOption.name}
                      </MenuRadioItem>
                    ))}
                  </MenuRadioGroup>
                </MenuGroup>
              </MenuSubPopup>
            </MenuSub>
          );
        })}
        {unavailableProviderOptions.length > 0 && <MenuDivider />}
        {unavailableProviderOptions.map((option) => {
          const OptionIcon = PROVIDER_ICON_BY_PROVIDER[option.value];
          return (
            <MenuItem key={option.value} disabled>
              <OptionIcon
                aria-hidden="true"
                className={cn(
                  "size-4 shrink-0 opacity-80",
                  option.value === "claudeCode" ? "" : "text-muted-foreground/85",
                )}
              />
              <span>{option.label}</span>
              <span className="ms-auto text-[11px] text-muted-foreground/80 uppercase tracking-[0.08em]">
                Coming soon
              </span>
            </MenuItem>
          );
        })}
        {unavailableProviderOptions.length === 0 && <MenuDivider />}
        {COMING_SOON_PROVIDER_OPTIONS.map((option) => {
          const OptionIcon = option.icon;
          return (
            <MenuItem key={option.id} disabled>
              <OptionIcon aria-hidden="true" className="size-4 shrink-0 opacity-80" />
              <span>{option.label}</span>
              <span className="ms-auto text-[11px] text-muted-foreground/80 uppercase tracking-[0.08em]">
                Coming soon
              </span>
            </MenuItem>
          );
        })}
      </MenuPopup>
    </Menu>
  );
});
