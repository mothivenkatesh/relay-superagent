# Relay Design System — components the demo must reuse

One grammar per primitive. New UI uses the `ds-*` class; legacy selectors are
aliased in the stylesheet's DESIGN SYSTEM block and migrate over time.
Tokens live on `:root` (`--ds-*`) in demo/server.py.

## Dropdown (the component this file exists for)

**Anatomy:** trigger + panel + option rows.

- **Trigger**: a `button` or `details>summary`, min-height 40px
  (`--ds-target`), never a bare text link. Chevron optional.
- **Panel** (`.ds-menu`): background #fff, 1px `--ds-hair` border,
  radius `--ds-radius-menu` (12px), shadow `--ds-menu-shadow`
  (0 12px 32px rgba(27,31,48,.12)), padding 8px, offset 6-8px from the
  trigger, z-index above content. Right-align when the trigger sits at a
  right edge.
- **Option row** (`.ds-option`): padding 12px 14px, radius 8px, 13px text,
  min-height 40px, hover `--ds-hover` (#F4F5FA). Destructive rows add
  `.danger` (#B3372B). A "more/other" escape row separates with a top
  hairline.
- **Behavior**: one open at a time; Escape and outside-click close;
  `details/summary` preferred (works without JS).

**Do not** invent a new menu style. If a menu needs something this spec
lacks, extend the spec here first, then the CSS block.

## Select (`.ds-select` / legacy `.cfgsel`, bare `select`)
Font inherit, 13px, padding 9px 34px 9px 12px, 1px `--ds-hair`, radius
`--ds-radius-control` (10px), min-height 40px, custom chevron (inline SVG
background), no native appearance.

## Input (`.inedit-in`, `.cfgbox`, `.hubsearch`, `.jfind`)
1px `--ds-hair`, radius 10px, padding 10px 14px, min-height 40px, focus =
2px `--ds-focus` outline + `--ds-accent` border.

## Card chrome
Radius `--ds-radius-card` (16px), 1px `--ds-hair` border. Cards are
clickable as a whole when they navigate. Faculty tints stay per-component.

## Tap targets (standing gate, see CLAUDE.md)
Nothing interactive under ~38px tall; expand hit areas with overlays
(`::after{inset:-10px}`), never with box-model changes on positioned
components.
