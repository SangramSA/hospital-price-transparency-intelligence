# Pipeline Findings: Hospital Price Transparency

This document summarizes the end-to-end pipeline run for 15 hospitals — which had usable data, what formats were encountered, how CMS matching works, what the commercial-to-Medicare ratio landscape looks like, and what it would take to scale to 5,000+ hospitals.

## Key findings

1. **All 15 hospitals processed successfully.** Every hospital downloaded (HTTP 200), produced extracted rows, and passed through the full pipeline. 13 of 15 matched to CMS knee replacement benchmarks.
2. **Commercial rates are typically 1.7× Medicare** for total knee arthroplasty (median across 1,338 comparable rows). UnitedHealthcare and Aetna negotiate the highest rates (2.0–2.7× median); Medicare Advantage clusters near 1.0×; Medicaid reimburses at ~0.2×.
3. **Charge methodology is the most important filter.** Case-rate rows (median 1.77×) are directly comparable to the Medicare DRG bundle. Fee-schedule and per-diem rows are not — mixing them inflates or deflates ratios.
4. **Data quality limits ~half the dataset.** Between sentinel values (Baylor's $999M), algorithm-only rates (Oak Hill), and absent CMS benchmarks (Grayling, Adventist Health), 52% of rows cannot produce a reliable commercial-to-Medicare ratio. The pipeline flags these explicitly.
5. **Scaling to 5,000+ hospitals** is feasible but requires AI-assisted discovery (only 61% of hospitals have `cms-hpt.txt`), cloud-native infrastructure, and production-grade observability.

---

## 1. Hospital data usability

All 15 hospitals in `config/hospitals.yaml` were successfully processed: discover → download → extract → join → export.

| Hospital | State | Tier | Format | Raw size | Extracted rows | CMS match | Notes |
|---|---|---|---|---|---|---|---|
| NYU Langone Orthopedic | NY | 1 | CSV wide v3 | 466 MB | 401 | matched | Large file, streamed |
| New England Baptist | MA | 1 | JSON v2.2.0 | 2 MB | 18 | matched | Small, clean |
| BSW Ortho & Spine Arlington | TX | 1 | JSON v2.2.0 | 595 MB | 183 | matched | Extreme ratio outlier (see §6) |
| Novant Health Forsyth | NC | 1 | CSV wide v2 | 23 MB | 74 | matched | Some outlier ratios |
| Ascension St. Vincent Evansville | IN | 2 | CSV tall v2 | 218 MB | 1 | matched | Very low row yield for knee procedures |
| Hoag Orthopedic Institute | CA | 2 | CSV tall v2 | 0.7 MB | 14 | matched | Small specialty hospital |
| AtlantiCare Regional Medical Center | NJ | 2 | CSV tall v2 | 1.2 GB | 124 | matched | Largest raw file |
| Merit Health River Oaks | MS | 2 | CSV wide v2 | 388 MB | 176 | matched | — |
| Oak Hill Hospital (HCA Florida) | FL | 2 | JSON v3.0.0 | 339 MB | 1024 | matched | Highest row count; v3 template |
| Piedmont Hospital Atlanta | GA | 2 | CSV tall v2 | 317 MB | 60 | matched | Redirected download URL |
| HonorHealth Deer Valley | AZ | 2 | CSV wide v2 | 51 MB | 462 | matched | — |
| Hillcrest Hospital South | OK | 3 | CSV tall v2 | 445 MB | 180 | matched | — |
| Warren Memorial Hospital | VA | 3 | CSV tall v2 | 177 MB | 104 | matched | No cms-hpt.txt; config-only URL |
| Grayling Hospital (Munson) | MI | 3 | CSV tall v2 | 1.3 GB | 90 | **no_match** | CCN null — not in CMS knee extract |
| Adventist Health Reedley | CA | 3 | CSV tall v3 | 497 MB | 70 | **no_match** | CCN null — not in CMS knee extract |

**Bottom line:** 15/15 downloaded, 15/15 extracted, 13/15 matched to CMS. The 2 unmatched hospitals (Grayling, Adventist Health) have null CCNs in the roster — they likely report zero or very few knee discharges to CMS.

---

## 2. File formats encountered

### By parser strategy

| Strategy | Template | Hospitals | Rows | Notes |
|---|---|---|---|---|
| `csv_wide_standardcharges` | v2 | Novant, HonorHealth, Merit Health | 712 | Payer columns spread horizontally |
| `csv_wide_standardcharges` | v3 | NYU Langone | 401 | Same layout, newer CMS template |
| `csv_tall_variant` | v2 | Hoag, Ascension, AtlantiCare, Piedmont, Hillcrest, Warren, Grayling | 573 | One row per payer × procedure × rate type |
| `csv_tall_variant` | v3 | Adventist Health | 70 | Same layout, newer template |
| `json_nested_standard_charge_information` | v2 | New England Baptist, Baylor | 201 | CMS v2.2.0 nested JSON schema |
| `json_nested_standard_charge_information` | v3 | Oak Hill (HCA) | 1,024 | CMS v3.0.0 nested JSON schema |

**Totals:** 12 CSV hospitals (1,756 rows), 3 JSON hospitals (1,225 rows). 12 hospitals use CMS template v2 (1,486 rows), 3 use v3 (1,495 rows).

### How layout differences are handled

- **Encoding:** Auto-detected per file (`csv_encoding.py`). UTF-8 with BOM found for Hoag and HonorHealth; Latin-1 fallback used where needed.
- **Template routing:** `template_versions.py` inspects CSV headers or JSON `version` fields and routes to the correct parser. The detected strategy is stamped on every row (`parser_strategy` column) for auditability.
- **Memory:** Files over 50 MB stream directly to JSONL on disk rather than accumulating in memory. 12 of 15 hospitals triggered streaming mode.

---

## 3. Canonical schema design

### Grain

One row per **hospital × payer × plan × procedure line**. A single knee procedure at one hospital yields many rows — one per payer/plan combination that has a published rate.

### Why this grain?

- **No silent aggregation.** Different payers at the same hospital negotiate very different rates. Collapsing to hospital-level hides the core analytic signal.
- **Full lineage.** Each row carries `source_row_index` (CSV) or `source_json_path` (JSON), `parser_strategy`, `extractor_version`, and `dq_flags` — enough to trace any output back to the exact position in the raw MRF.
- **CMS fields inline.** After the join stage, benchmark columns (`cms_avg_mdcr_pymt_amt`, `cms_tot_dschrgs`) are attached to every row, enabling per-payer ratio computation without a separate lookup.
- **DQ is explicit.** Parse and semantic issues are flagged in `dq_flags`, not silently dropped.

### Key columns (54 total in `combined.csv`)

| Group | Columns |
|---|---|
| Identity | `hospital_key`, `hospital_name`, `state`, `ccn` |
| Procedure | `procedure_code`, `procedure_code_type`, `procedure_description`, `match_method` |
| Payer & rate | `payer_name`, `payer_name_normalized`, `plan_name`, `rate_type`, `negotiated_amount`, `charge_methodology` |
| Other charges | `gross_charge`, `discounted_cash`, `deidentified_min`, `deidentified_max` |
| CMS benchmark | `cms_avg_mdcr_pymt_amt`, `cms_tot_dschrgs`, `commercial_to_medicare_ratio` |
| Lineage | `source_row_index`, `parser_strategy`, `extractor_version`, `dq_flags`, `extracted_at` |
| Join metadata | `cms_match_status`, `cms_match_confidence`, `entity_resolution_method`, `cms_snapshot_hash` |

<details>
<summary>Example row (Hoag Orthopedic, Medicare DRG 469)</summary>

```json
{
  "hospital_key": "hoag-orthopedic-institute",
  "hospital_name": "Hoag Orthopedic Institute",
  "state": "CA",
  "ccn": "050769",
  "procedure_code": "469",
  "procedure_code_type": "DRG",
  "payer_name": "Medicare",
  "negotiated_amount": 27904.93,
  "charge_methodology": "other",
  "cms_avg_mdcr_pymt_amt": 14133.34,
  "commercial_to_medicare_ratio": 1.97,
  "cms_match_status": "matched_ccn_roster",
  "parser_strategy": "csv_tall_variant|csv_v2",
  "dq_flags": "negotiated_amount_inferred_from_estimated",
  "source_row_index": 4720
}
```

</details>

---

## 4. How hospitals were matched to CMS

### CCN-first deterministic join

Each hospital in `config/hospitals.yaml` has a curated `ccn` (CMS Certification Number, 6-digit zero-padded). The join stage:

1. Loads `data/cms_knee_replacement_by_provider.csv` and indexes by normalized `Rndrng_Prvdr_CCN`.
2. Looks up each hospital's roster `ccn` in the CMS index.
3. Stamps every row with `cms_match_status`:
   - `matched_ccn_roster` (13 hospitals, 2,821 rows): benchmark fields populated.
   - `no_match` (2 hospitals, 160 rows): Grayling and Adventist Health have `ccn: null` in the roster — they are not in the CMS knee extract.

### Why no fuzzy matching?

At 15 curated hospitals with verified CCNs, fuzzy matching adds complexity without benefit. Every match is stamped `entity_resolution_method: config_ccn` for full auditability. (At 5,000+ hospitals, multi-signal matching becomes necessary — see §5.4.)

### Ratio computation rules

`commercial_to_medicare_ratio = negotiated_amount / cms_avg_mdcr_pymt_amt`, computed **only when**:

- `negotiated_amount` is non-null
- `cms_avg_mdcr_pymt_amt` is non-null and > 0
- Rate type is not percent-of-charges or algorithm-only

Otherwise the ratio is null and DQ flags are set.

---

## 5. Scaling from 15 to 5,000+ hospitals

The current pipeline processes 15 hospitals sequentially in under 10 minutes. Scaling to the ~6,000+ CMS-registered hospitals that must publish MRFs requires changes at every stage — modern data infrastructure, AI-assisted automation, and production-grade observability.

### At a glance

| Capability | Current (15 hospitals) | At 5,000+ hospitals | Key enabler |
|---|---|---|---|
| **Discovery** | `cms-hpt.txt` + config fallback | AI agents that navigate hospital websites | Firecrawl + Claude tool-use |
| **Download** | Sequential httpx, local disk | Async serverless, cloud object storage | S3 + Lambda + SQS |
| **Parsing** | 4 rule-based parser branches | Rule-based first, LLM for the long tail | Confidence-gated LLM schema mapping |
| **Entity resolution** | CCN from curated YAML | Multi-signal (CCN + EIN + NPI + embeddings) | CMS NPPES/PECOS + sentence-transformers |
| **Storage** | `combined.csv` (2.5 MB) | Lakehouse with Iceberg/Delta tables | dbt + DuckDB or BigQuery |
| **Orchestration** | CLI `hpt run-all` | Dagster with per-hospital partitions | Software-defined assets + built-in lineage |
| **Data quality** | `qa_summary.json` | Automated contracts + anomaly detection | Great Expectations + Monte Carlo |
| **AI monitoring** | N/A | LLM decision tracing + guardrails | LangSmith + deterministic validators |

### Recommended phasing

- **Phase 1 (month 1–2):** Async downloads to S3, Dagster orchestration, Parquet output replacing CSV. This is infrastructure — no AI, no new parsers, just making the existing pipeline cloud-native and incremental.
- **Phase 2 (month 3–4):** LLM-powered discovery for the ~39% of hospitals without `cms-hpt.txt`. AI-assisted schema mapping for parser failures. Multi-signal entity resolution using CMS NPPES. These are the stages where AI has the highest leverage.
- **Phase 3 (month 5+):** Full lineage (OpenLineage), data contracts (Great Expectations), AI observability (LangSmith), and a warehouse-backed BI layer. This is what makes the system *maintainable* once it's running at scale.

---

### 5.1 Discovery: AI agents for the long tail

**The problem:** Only ~61% of hospitals have a working `cms-hpt.txt` (Turquoise Health, 2024). The other ~2,400 hospitals require navigating websites, clicking through consent pages, parsing PDFs, or interacting with third-party vendor portals.

**Concrete example from this pipeline:** Adventist Health Reedley's MRF lives behind a PARA Healthcare ASP.NET dynamic endpoint (`Reports.aspx?dbName=...`). Finding it required navigating Adventist Health's transparency page, identifying Reedley among 27 facilities, and following a vendor redirect. An AI agent with browser tool-use handles this in seconds — Claude reads the page, selects the correct facility from a dropdown, and extracts the download URL.

**How AI helps:**
- **Agentic crawlers** (Claude tool-use / agent SDK, or Firecrawl for LLM-optimized scraping): Given a hospital domain, the agent fetches the transparency page, interprets the layout, finds the MRF link, and returns a structured manifest. For the 61% with `cms-hpt.txt`, the existing deterministic parser runs first — the agent only activates for failures.
- **Multimodal document understanding:** Some hospitals embed MRF URLs in PDF compliance documents. A vision model extracts URLs from screenshots of these pages.
- **Continuous re-discovery:** Weekly scheduled runs detect when a hospital's page layout changes (via embedding similarity on page content), triggering re-crawl rather than silently serving stale URLs.

### 5.2 Download: cloud-native, serverless ingestion

**The numbers:** At ~300 MB average per hospital, 5,000 hospitals is ~1.5 TB per snapshot. With monthly re-downloads and versioning, budget ~20 TB/year of raw storage.

**Architecture:**
- **S3 (or GCS)** replaces local `data/raw/`. Content-addressed deduplication (`content_sha256`, already implemented) means only changed files are re-stored — expect ~20–30% churn between monthly runs.
- **AWS Lambda + SQS** for download jobs: one invocation per hospital, auto-scaled, with a dead-letter queue for persistent failures. Per-domain rate limiting prevents WAF blocks on shared CDNs (many hospitals host on the same Azure Blob or AWS CloudFront domain).

**Where AI helps:** When a download returns an HTML error page instead of a CSV (HCA Oak Hill's Azure SAS token expires periodically), an LLM classifies the failure type and either re-resolves the URL from `cms-hpt.txt` or escalates to the discovery agent. This replaces manual triage of download failures across thousands of hospitals.

### 5.3 Parsing: rule-based first, LLM for failures

**The problem:** This pipeline uses 4 parser branches (CSV wide, CSV tall, JSON v2, JSON v3). At 5,000 hospitals, expect dozens of layout variations — column renamings, extra metadata rows, inconsistent nesting, and formats the CMS template doesn't fully cover.

**Confidence-gated approach:**
1. **Rule-based parser runs first** (fast, deterministic, auditable) — covers ~80% of hospitals.
2. **If extraction fails or yields suspiciously few rows** (like Ascension's 1 row from 218 MB), the file is routed to an LLM.
3. **The LLM maps unfamiliar columns to the canonical schema.** Example: a hospital publishes `"neg_rate_dollar"` instead of `"standard_charge|negotiated_dollar"` — the LLM recognizes the semantic intent and maps it to `negotiated_amount`. The mapping is validated against deterministic rules (type checks, value ranges) before being accepted.
4. **Approved mappings are cached** so the same hospital's next file doesn't need another LLM call.

**Concrete example:** HonorHealth uses all-caps column headers (`"FEE SCHEDULE"` vs `"fee schedule"`). The current pipeline handles this with `str.lower()` normalization. But if a new hospital uses an entirely different header schema (e.g., Spanish-language column names at a border hospital), the LLM generates a mapping in one call rather than requiring a new parser branch.

**Tooling:** **DuckDB** for in-process SQL on large files (queries a 1 GB CSV in seconds); **dbt** for declarative transform chains once data lands in a warehouse.

### 5.4 Entity resolution: beyond curated CCN

**The problem:** The current pipeline joins on `ccn` from a hand-curated YAML roster. At 5,000 hospitals, manual CCN curation is infeasible — and many hospitals have ambiguous, missing, or conflicting identifiers in their MRFs.

**Multi-signal matching:**
- Combine CCN, EIN (parsed from MRF filenames — e.g., `133971298` in NYU's filename), NPI (from MRF metadata), hospital name, address, and state into a weighted matching score against the CMS Provider Enrollment database.
- **CMS NPPES / PECOS** provide public NPI → CCN crosswalks. Automating this lookup replaces the manual "find the CCN on the CMS website" step that was done for each of the 15 roster hospitals.
- **Embedding-based fuzzy matching** for the ambiguous cases: Piedmont Hospital (Atlanta) has 5 Piedmont entities in Georgia. Sentence-transformer embeddings on `(hospital_name, address, city, state)` tuples disambiguate correctly where string matching fails. Human review is triggered only below a confidence threshold.

**Concrete example from this pipeline:** BSW Orthopedic & Spine – Arlington is truncated to ~50 characters in the CMS data, so exact name matching fails. An embedding-based match on name + city + state resolves it to CCN 670067 with high confidence.

### 5.5 Storage and analytics: lakehouse, not CSV

**Why CSV breaks at scale:** `combined.csv` works at 2,981 rows. At 5,000 hospitals with hundreds of payers each, expect 5–50 million rows. A monolithic CSV is too slow to query, too large to version in git, and lacks schema enforcement.

**Target architecture:**
- **Iceberg or Delta Lake tables** on S3, partitioned by `state` and `hospital_key`. Supports time-travel queries ("what did NYU's rates look like last month?"), schema evolution (add columns without rewriting history), and efficient column pruning.
- **dbt** for the Silver → Gold transform layer: canonical schema validation, join logic, and QA metric computation are defined as tested, version-controlled SQL models rather than Python scripts.
- **DuckDB** for local/dev analytics (queries Parquet directly, zero infrastructure). **BigQuery or Snowflake** for production dashboards where multiple users need concurrent sub-second queries.
- **Metabase** (open-source, self-hosted) or **Looker** (managed) for the BI layer, replacing the Streamlit app at scale. Streamlit remains useful for ad-hoc exploration; the BI tool handles dashboards that non-technical stakeholders need.

### 5.6 Observability: lineage, contracts, and AI monitoring

At 15 hospitals, `qa_summary.json` and log files are enough. At 5,000, three layers of observability become essential — otherwise failures hide and bad data silently propagates.

**Layer 1 — Data lineage:**
- **OpenLineage** (open standard) with **Marquez** (metadata store): track every row from raw MRF → canonical extraction → CMS join → export. When a dashboard number looks wrong, trace it back to the exact source file and row in seconds. Dagster has native OpenLineage support, so this comes "free" with the orchestrator.

**Layer 2 — Data contracts and quality:**
- **Great Expectations**: define expectations like "negotiated_amount null rate < 50% per hospital" or "no hospital should have 0 extracted rows." Tests run after every extract stage and block promotion to Gold if they fail. This catches issues like Ascension's 1-row yield before they contaminate aggregate statistics.
- **Monte Carlo** (or open-source **Soda Core**): automated anomaly detection across runs — flags unexpected volume drops, schema drift, and distribution shifts without writing per-hospital rules. Detects when a hospital's MRF layout silently changes between runs.

**Layer 3 — AI observability:**
- Every LLM decision (discovery URL extraction, schema mapping, failure classification) is logged with: input context, model output, confidence score, and whether a human overrode the result.
- **LangSmith** (from LangChain) provides tracing, evaluation, and regression detection for LLM chains. When an LLM schema mapping produces a wrong column alignment, the trace shows exactly what the model saw and why it made that choice.
- **Guardrails are non-negotiable:** LLM outputs pass through deterministic validators (type checks, value-range checks, row-count sanity) before being accepted. Disagreements between rule-based and LLM-based parsers are logged for human review and become training signal for improving prompts.
- **Cost control:** Use fast/cheap models (Claude Haiku) for classification tasks (failure type, template version detection). Reserve larger models (Sonnet/Opus) for complex schema mapping. Cache prompt results aggressively — the same hospital's layout rarely changes between runs.

---

## 6. Medicare benchmark vs commercial negotiated rate patterns

### Summary (read this first)

1. **Commercial rates are typically 1.7× Medicare** for total knee arthroplasty across this 15-hospital sample, with substantial variation by payer and hospital.
2. **Payer hierarchy is consistent:** UnitedHealthcare and Aetna negotiate the highest rates (2.0–2.7× Medicare median), BCBS varies widely by state (1.7× median but up to 7× at some facilities), Humana and Medicare Advantage cluster near 1.0–1.2×, and Medicaid reimburses well below Medicare (~0.2×).
3. **Charge methodology is the single most important filter.** Case-rate rows (median 1.77×) are directly comparable to the Medicare DRG bundle. Fee-schedule rows (median 0.85×) represent component services and should not be compared to the full DRG payment.
4. **Hospital market position matters.** Specialty orthopedic hospitals (Hoag, NYU Langone, New England Baptist) show tighter payer spreads. Urban hospitals in high-cost markets (Piedmont Atlanta, Hillcrest South) show higher commercial premiums.
5. **Data quality limits ~half the dataset.** Between sentinel values ($999M at Baylor), algorithm-only rates (Oak Hill), and absent CMS benchmarks (Grayling, Adventist Health), 52% of rows cannot produce a reliable ratio. The pipeline flags these explicitly.
6. **The 15-hospital sample is not nationally representative.** It spans 12 states with a mix of specialty, urban, and rural hospitals but no large academic medical centers with the highest commercial leverage.

### What's included and excluded

| Category | Rows | % of total | Included in ratio analysis? |
|---|---|---|---|
| Clean ratio computable | 1,338 | 45% | Yes |
| Extreme outliers (> 100×) | 97 | 3% | No — data artifacts (see §6.5) |
| Null `negotiated_amount` | 1,138 | 38% | No — no dollar amount to compare |
| No CMS benchmark | 160 | 5% | No — Grayling + Adventist Health |
| Non-comparable rate type | 248 | 8% | No — percent-of-charges, algorithm-only |
| **Total** | **2,981** | **100%** | |

### 6.1 Overall distribution

| Metric | Value |
|---|---|
| Rows with computable ratio | 1,338 (of 2,981) |
| Median | 1.71× Medicare |
| 25th percentile (Q1) | 0.80× |
| 75th percentile (Q3) | 2.83× |
| Rows with ratio > 1.0× (commercial exceeds Medicare) | 72% |
| Rows with ratio > 2.0× | 44% |
| Rows with ratio > 3.0× | 23% |

The median of 1.71× is consistent with published findings: commercial payers typically negotiate rates 50–200% above Medicare for inpatient joint replacement.

### 6.2 Patterns by payer group

| Payer group | Rows | Q1 | Median | Q3 | Range | Notes |
|---|---|---|---|---|---|---|
| **Blue Cross / Blue Shield** | 208 | 1.26× | 1.71× | 3.70× | 0.15–11.75 | Highest spread; each state licensee negotiates independently |
| **UnitedHealthcare** | 141 | 1.25× | 2.65× | 3.15× | 0.12–6.71 | Highest median among major payers |
| **Aetna** | 104 | 1.77× | 2.04× | 2.84× | 0.01–4.47 | Tight IQR; consistent pricing across hospitals |
| **Cigna** | 95 | 0.94× | 2.12× | 2.48× | 0.18–6.40 | Wide Q1 from fee-schedule component rates |
| **Medicare / Medicare Advantage** | 198 | 0.69× | 1.01× | 1.25× | 0.00–2.36 | MA plans slightly above traditional Medicare |
| **Medicaid** | 27 | 0.20× | 0.20× | 0.87× | 0.01–2.86 | Well below Medicare, as expected |
| **Humana** | 21 | 1.01× | 1.10× | 1.25× | 0.79–2.17 | Close to Medicare; narrow spread |
| **Other commercial** | 544 | 0.69× | 1.20× | 2.22× | 0.00–6.34 | Regional plans, workers' comp, specialty contracts |

### 6.3 Patterns by charge methodology

| Methodology | Rows | Median ratio | Range | Interpretation |
|---|---|---|---|---|
| **Case rate** | 499 | 1.77× | 0.20–11.75 | **Most comparable** to Medicare DRG bundle — full episode payment |
| **Fee schedule** | 418 | 0.85× | 0.01–4.67 | Per-service component rates — low median because line items < bundled DRG |
| **Other** | 387 | 2.05× | 0.00–6.69 | Includes estimated amounts and unclassified structures |
| **Per diem** | 34 | 2.45× | 0.09–5.05 | Daily rate × expected LOS vs single DRG payment — not directly comparable |

**Case rate is the most reliable comparison basis.** When filtered to case-rate rows only, the median is 1.77× — commercial payers pay roughly 77% more than Medicare for a full knee replacement episode.

### 6.4 Per-hospital deep dive

<details>
<summary><strong>Hospitals with tight, reliable ratio data</strong> (New England Baptist, Hoag, AtlantiCare)</summary>

**New England Baptist Hospital** (Boston, MA) — Tier 1 orthopedic specialty
- CMS Medicare avg: $14,385 | 18 rows, all with ratios
- Median ratio: 1.25× | Range: 0.65–2.34
- Top payers: Harvard Pilgrim (2.29×), BCBS (2.34×), Tufts (1.12×), Medicare (1.16×)
- BCBS and Harvard Pilgrim pay roughly double Medicare; Tufts and Medicare Advantage are near-Medicare. Tight range reflects a small specialty hospital with limited payer mix.

**Hoag Orthopedic Institute** (Irvine, CA) — Tier 2 orthopedic specialty
- CMS Medicare avg: $14,133 | 14 rows, all with ratios
- Median ratio: 1.97× | Range: 1.21–6.34
- Top payers: Blue Shield (2.95×), United (2.65×), Blue Cross (2.14×)
- All payers above Medicare; no sub-Medicare rates. Consistent with a California specialty orthopedic facility.

**AtlantiCare Regional Medical Center** (Atlantic City, NJ) — Tier 2
- CMS Medicare avg: $14,895 | 124 rows, all with ratios
- Median ratio: 1.10× | Range: 0.40–11.75
- Charge methodologies: 88 case rate, 24 fee schedule, 4 per diem, 8 other
- Top payers: Horizon (1.08×), Amerigroup (1.06×), AmeriHealth (1.62×), Aetna (2.49×), **Horizon BCBS (7.64×)**
- Most payers negotiate near Medicare. The Horizon BCBS outlier at 7.64× likely reflects a different contract structure.

</details>

<details>
<summary><strong>Hospitals with higher commercial premiums</strong> (Hillcrest South, Piedmont Atlanta)</summary>

**Hillcrest Hospital South** (Tulsa, OK) — Tier 3
- CMS Medicare avg: $9,472 | 116 rows with ratios (180 total; 64 excluded as percent-of-charges)
- Median ratio: 2.17× | Range: 0.97–6.15
- Top payers: BCBS (3.30×), United (3.88×), UMR (2.91×), Aetna (4.45×)
- Lowest CMS benchmark in the matched set; low Medicare payment amplifies the ratio. All major commercial payers above 2×.

**Piedmont Hospital Atlanta** (Atlanta, GA) — Tier 2
- CMS Medicare avg: $10,933 | 48 rows with ratios (60 total; 12 excluded as percent-of-charges)
- Median ratio: 2.36× | Range: 0.00–8.33
- Top payers: Blue Cross (7.46×), Aetna (4.19×), Alliant (5.63×)
- Large metro hospital with high commercial premiums. Blue Cross at 7.46× is one of the highest individual-payer ratios in the clean dataset. The 0.00 minimum is a data artifact (negotiated amount of $1.89).

</details>

<details>
<summary><strong>Hospitals with the most payer diversity</strong> (HonorHealth, NYU Langone)</summary>

**HonorHealth Deer Valley Medical Center** (Phoenix, AZ) — Tier 2
- CMS Medicare avg: $12,741 | 462 rows with ratios (highest count in dataset)
- Median ratio: 1.20× | Range: 0.09–4.67
- Top payers: BCBS (3.70× median), Medicare Advantage (1.20×), United (2.65×), Aetna (2.04×), Cigna (1.86×)
- Most granular payer data in the roster. BCBS at nearly 4× Medicare while Medicare Advantage hovers at 1.2×. Low ratios (0.09×) are fee-schedule component rates.

**NYU Langone Orthopedic Hospital** (New York, NY) — Tier 1
- CMS Medicare avg: $22,114 (highest Medicare benchmark in the set)
- 353 rows with ratios (48 excluded as percent-of-charges)
- Median ratio: 1.37× | Range: 0.06–4.68
- Top payers: Medicare (0.69×), HIP (2.22×), BCBS (1.71×), UHC (3.15×), Aetna (1.77×)
- Highest Medicare benchmark reflects NYC cost of living. UHC at 3.15× means they pay ~$69,700 vs Medicare's ~$22,100.

</details>

<details>
<summary><strong>Hospitals without CMS benchmarks</strong> (Grayling, Adventist Health)</summary>

**Grayling Hospital** (Grayling, MI) — Tier 3
- 90 rows extracted, all `no_match` (CCN null in roster)
- Negotiated amounts: $822–$64,614 | Charge methodologies: 54 fee schedule, 36 case rate
- Cannot compute ratios; likely low knee replacement volume explains CMS absence.

**Adventist Health Reedley** (Reedley, CA) — Tier 3
- 70 rows extracted, all `no_match` (CCN null in roster)
- Negotiated amounts: $39–$6,404 | Charge methodologies: 68 fee schedule, 2 percent-of-charges
- Small rural hospital; low range suggests component/ancillary charges rather than full case rates.

</details>

### 6.5 Data quality outliers: Baylor and Novant

Two hospitals produce ratios that are clearly artifacts, not real pricing:

**Baylor Scott & White Ortho Arlington** — 71 rows with ratio, all = 97,972×
- Root cause: `negotiated_amount` values of $999,999,999 (a sentinel/placeholder). Dividing by the CMS benchmark of $10,207 produces the extreme ratio. The `negotiated_amount_inferred_from_estimated` DQ flag is set on 119 of 183 rows.
- **Exclude Baylor from all ratio analysis.**

**Novant Health Forsyth** — 38 rows with ratio, median = 179×
- Root cause: High-end values ($4M) are inferred from percent-of-charges methodology applied to high gross charges. When filtered to the 12 case-rate rows, the median drops to a plausible ~2.6×.
- **For Novant, use only `charge_methodology = 'case rate'` rows.**

---

## 7. Pipeline outputs reference

| Artifact | Location | Description |
|---|---|---|
| Raw MRFs | `data/raw/{hospital_key}/artifacts/` | Immutable downloaded files, content-addressed filenames |
| Manifests | `data/raw/{hospital_key}/manifest.json` | Download metadata: URL, SHA-256, HTTP status, ETag |
| Silver JSONL | `data/silver/{hospital_key}/*.canonical.jsonl` | Per-hospital canonical rows with lineage fields |
| Joined JSONL | `data/processed/joined/{hospital_key}/*.joined.jsonl` | Rows with CMS benchmark fields attached |
| Combined CSV | `data/processed/combined.csv` | Final export: 2,981 rows × 54 columns |
| QA Summary | `data/processed/qa_summary.json` | Row counts, DQ flag distributions, null rates |
| Export Metadata | `data/processed/export_metadata.json` | Pipeline version, CMS snapshot hash, schema version |
| Checkpoints | `data/processed/checkpoints/extract_{key}.json` | Extract-stage skip logic for idempotent re-runs |

---

## What's next

- **Deploy the public dashboard** (Streamlit Community Cloud) for interactive exploration of `combined.csv` — see `app/streamlit_app.py`.
- **Expand procedure coverage** beyond knee replacement (HCPCS 27447) to hip replacement, spinal fusion, or other high-volume DRGs using the same pipeline infrastructure.
- **Scale to more hospitals** following the phased approach in §5 — starting with cloud-native infrastructure, then AI-assisted discovery and parsing.
- **Automate monthly refreshes** to track rate changes over time, using the existing checkpoint and drift detection capabilities.
