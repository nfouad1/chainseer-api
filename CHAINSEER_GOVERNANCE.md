# Chainseer learned-behavior governance

Chainseer treats self-improvement as a governed lifecycle, not permission for
the agent to rewrite its own safety boundary.

## Hard invariant

Autonomous faculty and pattern changes may only preserve or tighten risk
controls. The gate recomputes the effect from a structured manifest; it does
not trust a proposal merely because it labels itself `tighten_only`.

Automatic activation is refused if a change may:

- raise a token legitimacy score;
- lower a risk classification;
- remove or suppress a hard stop;
- reduce a minimum safety, evidence, liquidity, or confirmation threshold;
- increase a maximum tax, concentration, slippage, freshness, or drift limit;
- broaden allowed risk levels or automatic admission; or
- has an unknown or incomplete effect declaration.

Enabling live execution, signing, or broadcast is non-overridable in this
registry. This module cannot load keys or move funds.

## Faculty boundary

Production faculty packs must include an effect contract for every faculty.
Cambium-grown faculties receive the narrower
`cognitive_advisory_only` authority: they can label bounded structured
evidence, compute a primitive observation, add a reasoning frame, or reveal a
capability gap. They have no hook into deterministic scores, hard stops,
admission, signing, or broadcast.

The effect contract and a fingerprint of the installed faculty definition are
stored under `chainseer_governance` in `registry/grown.json`. That file is
already included in Cypher Tempre registry epochs. A changed definition, a
missing governance record, or an unsealed mutation therefore fails startup
verification.

## Learned-pattern lifecycle

Patterns are declarative inert data and move through:

```text
candidate -> shadow -> validated -> active -> retired
                  \-> rejected
```

`validated` requires a held-out benchmark hash, one or more canonical Outcome
Ledger record hashes, and a positive sample size. A pattern cannot jump from
candidate to active.

Every lifecycle transition changes the epoch-covered registry and creates a
new registry epoch plus a Timechain governance ring. A safe pattern may become
active only when the common gate independently classifies it as
`tighten_only` or `observability_only`.

## Explicit human override

A potentially relaxing pattern remains inert unless activation receives all
of the following:

- a unique approval/change-control identifier;
- a named human approver;
- an ISO-8601 approval time;
- a substantive reason;
- the exact 64-character proposal hash; and
- the exact confirmation phrase printed by `status`.

The approval receipt is stored in the registry before the new epoch is sealed
and repeated in the Timechain transition ring. It is proposal-specific and
cannot approve a different rule. Human identity is audit attribution at this
local CLI boundary; production use should additionally restrict the command
to an authenticated administrator or require an externally signed approval.

## Commands

Check the policy and registry:

```bash
python -X utf8 chainseer_governance.py --root ./chainseer_chain status
```

One-time migration of a verified pre-governance faculty registry:

```bash
python -X utf8 chainseer_governance.py --root ./chainseer_chain \
  migrate-cognitive-faculties
```

Register an inert JSON pattern manifest:

```bash
python -X utf8 chainseer_governance.py --root ./chainseer_chain \
  propose ./pattern.json
```

Move it into shadow evaluation, then attach held-out validation evidence:

```bash
python -X utf8 chainseer_governance.py --root ./chainseer_chain \
  transition PROPOSAL_HASH shadow

python -X utf8 chainseer_governance.py --root ./chainseer_chain \
  transition PROPOSAL_HASH validated --evidence ./validation.json
```

Activate a tighten-only validated pattern:

```bash
python -X utf8 chainseer_governance.py --root ./chainseer_chain \
  transition PROPOSAL_HASH active
```

For a relaxing validated pattern, the same command additionally requires the
five approval flags shown by `--help`, including the exact confirmation
phrase. Omitting or partially supplying them fails closed.

## Calibration policy

Outcome-driven calibration uses the same effect assessor across all current
policy dimensions. The automated calibration path remains stricter than the
general pattern registry: it rejects every relaxation outright and exposes no
override argument. Its only supported adoption direction is tighten-only.
