import { type ClaudeCodeEffort } from "@t3tools/contracts";
import { getDefaultReasoningEffort } from "@t3tools/shared/model";
import { memo, useState } from "react";
import { ChevronDownIcon } from "lucide-react";
import { Button } from "../ui/button";
import {
  Menu,
  MenuGroup,
  MenuPopup,
  MenuRadioGroup,
  MenuRadioItem,
  MenuTrigger,
} from "../ui/menu";

export const ClaudeCodeTraitsPicker = memo(function ClaudeCodeTraitsPicker(props: {
  effort: ClaudeCodeEffort;
  options: ReadonlyArray<ClaudeCodeEffort>;
  onEffortChange: (effort: ClaudeCodeEffort) => void;
}) {
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const defaultEffort = getDefaultReasoningEffort("claudeCode") as ClaudeCodeEffort;
  const effortLabelByOption: Record<ClaudeCodeEffort, string> = {
    low: "Low",
    medium: "Medium",
    high: "High",
  };

  return (
    <Menu
      open={isMenuOpen}
      onOpenChange={(open) => {
        setIsMenuOpen(open);
      }}
    >
      <MenuTrigger
        render={
          <Button
            size="sm"
            variant="ghost"
            className="shrink-0 whitespace-nowrap px-2 text-muted-foreground/70 hover:text-foreground/80 sm:px-3"
          />
        }
      >
        <span>{effortLabelByOption[props.effort]}</span>
        <ChevronDownIcon aria-hidden="true" className="size-3 opacity-60" />
      </MenuTrigger>
      <MenuPopup align="start">
        <MenuGroup>
          <div className="px-2 py-1.5 font-medium text-muted-foreground text-xs">Effort</div>
          <MenuRadioGroup
            value={props.effort}
            onValueChange={(value) => {
              if (!value) return;
              const nextEffort = props.options.find((option) => option === value);
              if (!nextEffort) return;
              props.onEffortChange(nextEffort);
            }}
          >
            {props.options.map((effort) => (
              <MenuRadioItem key={effort} value={effort}>
                {effortLabelByOption[effort]}
                {effort === defaultEffort ? " (default)" : ""}
              </MenuRadioItem>
            ))}
          </MenuRadioGroup>
        </MenuGroup>
      </MenuPopup>
    </Menu>
  );
});
