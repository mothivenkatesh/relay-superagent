# Build notes — 2026-08-17/18 session

One session, captain-directed throughout. Decisions D28–D30 in
decisions.md carry the reasoning; this is the change inventory.

## Roster (D28)
- Cut 34 agents → **8**: Appointment Booking, COD Confirmation,
  Dispute Responder, Abandoned Cart Recovery, Failed Payment Recovery,
  EMI Collections, Subscription Dunning, Refund Risk.
- Codenames retired entirely: `role` is the one identity. `name=`
  removed from every roster entry; "Dispute Defender" → "Dispute
  Responder" in all copy; other codenames became plain references
  ("the cart agent"). Engine identifiers (AgentType enum, slugs)
  unchanged.
- Agent Teams grid, hiring shelf, Staff-roles row and the COD Sentry
  seed all removed. /agents is one flat roster: Working now + In trial.

## Agent Builder (Conductor-style)
- New flow at `/agents/new`, reachable from the "Hire an agent" button:
  describe the job → three-question round (act / touch / gate, with
  Recommended defaults) → progress narration (✓ Named it… ✓ defaults
  applied) → brand + knowledge round → draft with config table, Preview
  rail (✓/○ per section), "Describe a change" redraw, Create agent.
- Drafts survive refresh (server-side PENDING_BUILDS); landing shows
  "You have N drafts in progress".
- `_build_draft` now tries one Haiku call (`claude-haiku-4-5`,
  structured output, closed schema) for name/kind/job/first-message;
  silent fallback to the keyword drafter on no key / timeout / bad
  JSON. Key comes from the keychain (`relay_superagent` / `anthropic`),
  same seam discipline as `_llm_route`.

## Design system
- Font: DM Sans (embedded base64, ~1.4MB) → **Geist** via Google Fonts
  link, system fallback. File shrank 2MB → ~600KB.
- Type scale collapsed to **11 / 13 / 20 / 24 / 32** (one graphical
  exception: 8.5px SVG chart rule label).
- Buttons: uniform **32px** everywhere (supersedes the 38-40px Fitts
  floor; the audit itself still stands). No shadows on buttons; hover
  is color-only.
- Copy voice (D29): literal and active. The "yes" dialect became
  "approve/approval" (~75 strings); aphorisms cut from Export data,
  Scheduled, mode menu, onboarding, Files, Settings; chat guard replies
  rewritten flat.
- Scheduled: promptbox composer (was input+Add); card Edit/Remove moved
  into a ⋯ header menu; footer = status + Read it only.
- Files (was Knowledge): renamed across nav, tabs, palette and copy;
  inner pill "Uploads"; one-click Upload-a-file button on every tab.
- Home: everything left-aligned on the 740px column.

## Architecture (D30)
- demo/ split into three modules: **roster.py** (all agent data, pure
  literals, zero imports), **state.py** (schema-versioned atomic
  persistence + the request lock), **server.py** (routes + rendering).
- ThreadingHTTPServer + one RLock per handler; every POST persists
  atomically; state survives restarts (verified by kill/restart).
- v1 `.demo_cfg.json` migrates on load; unknown schemas fail open.
- ROUTE_ALIASES keep renamed URLs alive forever (/files, /knowledge,
  /routines, /history).
- Dead code removed: 87 orphaned dict entries, 7 unused functions,
  5 unused constants, CEDED_TO_COWORKER/DESKS, ~150 lines of dead CSS,
  110-line duplicated shell CSS → one SHARED_UI_CSS.

## Verification
- 73 tests green throughout (2 Postgres contract skips without :5435).
- All routes + aliases 200; builder walked end-to-end in the browser;
  persistence proven across a real process kill; 20 parallel page
  loads served.
