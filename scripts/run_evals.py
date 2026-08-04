"""Programmable evals: run the seam fixtures against the LIVE model and
score deterministically. Results land in evals/results.json, which the
Quality tab reads.

Usage:
  uv run python scripts/run_evals.py                # all seams
  uv run python scripts/run_evals.py semantic_diff  # one seam

Needs the anthropic keychain key with credits. Costs cents.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "src")

from relay_superagent.llm.claude import ClaudeLlm  # noqa: E402

EVALS = Path(__file__).resolve().parents[1] / "evals"


def score(seam: str, fx: dict, out: dict) -> tuple[bool, str]:
    exp = fx.get("expect", {})
    if seam == "confirm_mention":
        got = bool(out.get("is_competitive"))
        return got == exp["is_competitive"], f"is_competitive={got}"
    if seam == "extract_claim":
        text = (out.get("claim_text") or "").lower()
        needles = [n.lower() for n in fx["expect_contains"]]
        hit = any if fx.get("any") else all
        ok = hit(n in text for n in needles)
        return ok, f"claim={text[:60]!r}"
    if seam == "draft_counter":
        ch = fx["checks"]
        counter = out.get("counter_text") or ""
        cited = set(out.get("cited_evidence_ids") or [])
        known = {e["evidence_id"] for e in fx["input"]["evidence"]}
        if ch.get("escalate_or_no_fabrication"):
            ok = bool(out.get("escalate")) or not cited
            return ok, f"escalate={out.get('escalate')} cited={sorted(cited)}"
        problems = []
        if ch.get("cites_subset") and not cited <= known:
            problems.append(f"fabricated citations {cited - known}")
        if not (ch["len_min"] <= len(counter) <= ch["len_max"]):
            problems.append(f"length {len(counter)}")
        low = counter.lower()
        problems += [f"banned:{b}" for b in ch.get("banned", []) if b in low]
        return not problems, "; ".join(problems) or "clean"
    if seam == "judge":
        scores = {k: v for k, v in out.items() if isinstance(v, (int, float))}
        if "passes_at" in exp:
            t = exp["passes_at"]
            return all(v >= t for v in scores.values()), f"scores={scores}"
        t = exp["fails_at"]
        return any(v < t for v in scores.values()), f"scores={scores}"
    if seam == "semantic_diff":
        got = bool(out.get("is_material"))
        return got == exp["is_material"], f"is_material={got}"
    if seam == "narrate":
        import re as _re
        text = out.get("narration") or ""
        problems = [t for t in fx["must_contain"] if t.lower() not in text.lower()]
        if fx.get("numbers_only_from_facts"):
            fact_nums = set(_re.findall(r"\d+", fx["facts"]))
            extra = [n for n in _re.findall(r"\d+", text) if n not in fact_nums]
            problems += [f"invented number {n}" for n in extra]
        return not problems, "; ".join(problems) or text[:70]
    return False, "unknown seam"


def run_seam(llm: ClaudeLlm, seam: str) -> dict:
    fixtures = json.loads((EVALS / f"{seam}.json").read_text())["fixtures"]
    results = []
    for fx in fixtures:
        i = fx.get("input", {})
        try:
            if seam == "confirm_mention":
                out = llm.confirm_mention(i["text"], i["names"])
            elif seam == "extract_claim":
                out = llm.extract_claim(i["text"], i["competitor_id"])
            elif seam == "draft_counter":
                out = llm.draft_counter(i["claim"], i["deal"], i["evidence"], i["memory"])
            elif seam == "judge":
                out = llm.judge(i["claim"], i["counter"], i["rubric"])
            elif seam == "semantic_diff":
                out = llm.semantic_diff(i["original"], i["edited"])
            elif seam == "narrate":
                out = llm.narrate(fx["question"], fx["tool"], fx["facts"])
            ok, detail = score(seam, fx, out)
        except Exception as e:                             # noqa: BLE001
            ok, detail = False, f"error: {e}"
        results.append({"id": fx["id"], "pass": ok, "detail": detail,
                        "adversarial": fx.get("adversarial", False)})
        print(f"  {'PASS' if ok else 'FAIL'}  {fx['id']}  {detail}")
    return {"total": len(results), "passed": sum(r["pass"] for r in results),
            "cases": results}


def main() -> None:
    seams = sys.argv[1:] or [p.stem for p in sorted(EVALS.glob("*.json"))
                             if p.name != "results.json"]
    llm = ClaudeLlm()
    all_results = {}
    if (EVALS / "results.json").exists():
        all_results = json.loads((EVALS / "results.json").read_text())
    for seam in seams:
        print(f"== {seam}")
        all_results[seam] = run_seam(llm, seam)
        all_results[seam]["ran_at"] = datetime.now(timezone.utc).isoformat()
    (EVALS / "results.json").write_text(json.dumps(all_results, indent=1))
    total = sum(r["total"] for s, r in all_results.items() if s in seams)
    passed = sum(r["passed"] for s, r in all_results.items() if s in seams)
    print(f"\n{passed}/{total} passed -> evals/results.json")


if __name__ == "__main__":
    main()
