# Evals — model-in-loop, one directory per seam (spec §9.3)

Deterministic tests live in `tests/` and run on every commit. These run on
prompt/policy/model changes and nightly, against pinned models.

| Dir | Seam | Metric | Bar |
|---|---|---|---|
| detection/ | mention confirmation + claim extraction | P/R, field F1 | P ≥ 0.85; wrong competitor = auto-fail |
| drafting/ | counter drafting | layer-1 assertions + judge | deterministic layer 100% |
| judge/ | judge calibration | agreement with human labels | recalibrate on every judge change |
| diff/ | is_material classifier | accuracy | highest bar — computes the invoice |
| adversarial/ | injection via transcripts | pass rate | 100%, blocks release |

Seed ~30 hand-labelled fixtures per seam before production traffic. Every
production `reject` auto-emits a candidate fixture (spec §9.6).
