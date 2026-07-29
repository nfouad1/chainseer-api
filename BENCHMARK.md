# Chainseer Benchmark v1

The benchmark measures what Chainseer can prove on token evidence that existed
at a fixed cutoff. It does not treat a current re-scan as a historical result,
and it keeps token evidence separate from infrastructure failure.

## Production case-bank workflow

The source of truth is two append-only ledgers:

- prediction observations, captured before any outcome is known
- later outcome observations, each with an evidence reference and reviewer

Capture a completed public report immediately. Use an immutable analyzer
version such as the deployed Git commit:

```bash
python -X utf8 chainseer_benchmark.py capture \
  --report analysis-job.json \
  --observations benchmark/observations-v1.jsonl \
  --cohort launch \
  --split test \
  --analyzer-version 07ee732 \
  --latency-ms 1280
```

The command derives the evidence cutoff, evidence age, network, token, decision,
public-report hash, block/slot anchor, and Timechain ring. If the input is a
complete API job response, latency is derived from its start and finish times.
Otherwise `--latency-ms` is required. A duplicate analyzer-version/case record
is refused.

This capture path starts Chainseer calibration cohorts. It does not by itself
make a live vendor comparison matched. Competing analyzers must be run from the
same frozen evidence bundle, case cutoff, and block/slot anchor; independently
timed current scans remain unmatched and must not be ranked against each other.

After a real outcome is independently reviewed, append its label:

```bash
python -X utf8 chainseer_benchmark.py label \
  --observations benchmark/observations-v1.jsonl \
  --outcomes benchmark/outcomes-v1.jsonl \
  --case-id case-... \
  --label adverse_security \
  --observed-at 2026-08-15T12:00:00+00:00 \
  --evidence-ref timechain:ring:... \
  --reviewer reviewer-id
```

`benign` means no adverse security outcome was found through the declared
window; it requires at least seven days between evidence cutoff and review.
Adverse and infrastructure outcomes may mature earlier when their evidence is
already conclusive.

Inspect progress and create immutable evaluator snapshots:

```bash
python -X utf8 chainseer_benchmark.py status \
  --observations benchmark/observations-v1.jsonl \
  --outcomes benchmark/outcomes-v1.jsonl

python -X utf8 chainseer_benchmark.py materialize \
  --observations benchmark/observations-v1.jsonl \
  --outcomes benchmark/outcomes-v1.jsonl \
  --cases benchmark/cases-2026-08-15.jsonl \
  --predictions benchmark/predictions-2026-08-15.jsonl
```

Materialization includes finalized cases only and refuses to overwrite an
existing snapshot.

## Evaluator record banks

Cases and predictions are append-only JSONL files. A case contains:

- `case_id`, `network`, `cohort`, and public `token_address`
- `split`: `train`, `validation`, or `test`
- `evidence_cutoff`: the latest information an analyzer was allowed to use
- `outcome_observed_at`: a later timestamp
- `label`: `adverse_security`, `benign`, or `infrastructure_failure`
- `outcome_evidence_refs`: non-empty references supporting the outcome

A prediction contains:

- `case_id`, `analyzer`, and immutable `analyzer_version`
- `analyzed_at` and an `evidence_cutoff` exactly matching the case
- `risk_level`, `action`, `hard_stop_count`, and optional `legitimacy_score`
- optional calibrated `risk_probability` between zero and one
- `infrastructure_indeterminate`
- `latency_ms`, `evidence_age_seconds`, and optional `report_hash`

Do not convert a Chainseer legitimacy score into a probability. Probability
calibration metrics remain null until an analyzer supplies an explicitly
calibrated risk probability.

## Evaluation

```bash
python -X utf8 chainseer_benchmark.py validate \
  --cases benchmark/cases-v1.jsonl \
  --predictions benchmark/predictions-v1.jsonl

python -X utf8 chainseer_benchmark.py evaluate \
  --cases benchmark/cases-v1.jsonl \
  --predictions benchmark/predictions-v1.jsonl \
  --output benchmark/reports/baseline-v1.json
```

The default evaluation excludes the training split. Reports include:

- dangerous false-negative and false-positive rates
- precision, recall, specificity, and 95% Wilson intervals
- token-evidence coverage and adverse/benign abstention rates
- an effective adverse miss rate that includes abstentions
- correct infrastructure abstention and wrong-certainty counts
- optional Brier score and expected calibration error
- p50/p95 latency and evidence age
- results by chain, cohort, and split
- canonical case-bank, prediction-bank, and report hashes

Comparisons are valid only on `matched_case_count`: cases for which every
analyzer produced a prediction from the same evidence cutoff.

## Timechain sealing

The deterministic evaluator emits a `seal_payload`. The cognitive runner
reviews the report, PoQ-gates the conclusion, attaches the report file, and
seals its hashes and metrics into `chainseer_chain`. The evaluator deliberately
does not invent PoQ scores or mutate analyzer policy.

This first release provides the harness, schema, and test coverage. It does not
claim measured production performance until a time-separated, independently
reviewed case bank has been populated.

The files under `tests/fixtures/benchmark_*.jsonl` are synthetic CLI fixtures
only. They verify the evaluator and matched-comparison path; they are not
production observations and must never be quoted as product performance.
`benchmark_public_report_v1.json` is likewise a synthetic capture fixture.
