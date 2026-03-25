# Regulatory & Domain Reference Guide

*Hospital Price Transparency Pipeline — Knee Replacement (HCPCS 27447)*

This document connects the federal price transparency regulation (45 CFR Part 180) to the pipeline's data model, maps the CMS Medicare data dictionary to analytical requirements, and serves as the reference for **why** the data exists, what it means, and how it connects.

**Authoritative sources:**
- [45 CFR Part 180](https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-E/part-180) — the federal regulation
- [CMSgov/hospital-price-transparency](https://github.com/CMSgov/hospital-price-transparency) — official CMS MRF templates and data dictionaries
- [CMS HPT Validator & Tools](https://cmsgov.github.io/hpt-tool/) — MRF file naming wizard, TXT file generator, validator v2.0

---

## 1. The Regulation That Creates the Data

### The legal mandate

Since January 1, 2021, every US hospital must publish a machine-readable file (MRF) containing standard charges for all items and services (45 CFR §180.50). Companies like Turquoise Health have built businesses around collecting and normalizing this data at scale.

### The five standard charges (§180.20)

The regulation defines five types of "standard charges." These directly inform the pipeline's canonical schema:

| Regulatory term (§180.20) | Definition | Pipeline schema mapping | Analytical role |
|---|---|---|---|
| **Gross charge** | Chargemaster "sticker price" — amount on hospital's price list absent any discounts | `gross_charge` column | Maps to CMS `Avg_Submtd_Cvrd_Chrg`. ~6.4× actual payment. Not useful for pricing analysis on its own. |
| **Payer-specific negotiated charge** | Charge negotiated with a specific third-party payer for an item or service. Must be tagged with payer name and plan name. | `negotiated_amount` column (with `rate_type = "negotiated"`) | **Core analytical target.** These commercial rates exist only in the transparency files — CMS Medicare data does not have them. |
| **Discounted cash price** | Charge for an individual who pays cash or cash equivalent | `discounted_cash` column | Useful benchmark. Often between Medicare and commercial rates. |
| **De-identified minimum** | Lowest charge negotiated with any third-party payer (anonymized) | `deidentified_min` column | Floor of the negotiation range across all payers. |
| **De-identified maximum** | Highest charge negotiated with any third-party payer (anonymized) | `deidentified_max` column | Ceiling of the negotiation range. |

> **Pipeline note:** The five charge types are stored as **separate columns**, not as different values of a single `rate_type` field. Each row has `rate_type = "negotiated"` with the other charges available in their own columns when published by the hospital.

---

### CMS MRF template versions

The CMS has published machine-readable file templates that hospitals must conform to. Three compliance milestones define which data elements are required:

| Milestone | Effective date | Enforcement date | Key additions |
|---|---|---|---|
| **v2.0 initial** | July 1, 2024 | July 1, 2024 | Core data elements: hospital info, gross/cash/negotiated charges, payer names, billing codes, methodology, de-identified min/max |
| **v2.0 full** | January 1, 2025 | January 1, 2025 | + `estimated_allowed_amount`, drug measurements (`drug_unit_of_measurement`, `drug_type_of_measurement`), `modifiers` |
| **v3.0** | January 1, 2026 | **April 1, 2026** | + `attester_name`, `type_2_npi`, `attestation` (replaces affirmation), `median_amount`, `10th_percentile`, `90th_percentile`, `count`. `hospital_location` renamed to `location_name`. `estimated_allowed_amount` removed. New code types: `CMG`, `MS-LTC-DRG`. |

The 15 hospitals in this pipeline publish a mix of v2 and v3 templates (see [pipeline-findings.md](pipeline-findings.md) §2).

---

### Required MRF data elements

The following tables reflect the **v3.0 specification** (the latest), with notes on which elements were added in each version.

#### Hospital-level header fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `hospital_name` | String | Yes | Legal business name |
| `last_updated_on` | Date | Yes | ISO 8601 (YYYY-MM-DD) |
| `version` | String | Yes | CMS template version (e.g., `"3.0.0"`) |
| `location_name` | String | Yes | Unique hospital location names (renamed from `hospital_location` in v3) |
| `hospital_address` | String | Yes | Physical addresses; multiple locations separated by `\|` |
| `license_number\|[state]` | String | Yes | State license number with 2-letter state code |
| `attester_name` | String | Yes | CEO or designated senior official (**new in v3**) |
| `attestation` | Boolean | Yes | Must be `true` for compliance (**new in v3**, replaces affirmation) |
| `type_2_npi` | String | Yes | Organizational NPI with taxonomy codes 27 or 28 (**new in v3**) |
| `financial_aid_policy` | String | Optional | Charity care / bill forgiveness policy |
| `general_contract_provisions` | String | Optional | Aggregate-level payer contract terms |

#### Per-item/service fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `description` | String | Yes | Plain-language service description — used in keyword matching ("knee arthroplasty", "TKA") |
| `code\|[i]` | String | Yes | Billing codes; `[i]` is sequential (1, 2, 3...) for multiple codes per item |
| `code\|[i]\|type` | Enum | Yes | Code type. Valid values: `CPT`, `HCPCS`, `NDC`, `RC`, `ICD`, `DRG`, `MS-DRG`, `R-DRG`, `S-DRG`, `APS-DRG`, `AP-DRG`, `APR-DRG`, `APC`, `LOCAL`, `EAPG`, `HIPPS`, `CDT`, `CDM`, `TRIS-DRG`, `CMG` (v3), `MS-LTC-DRG` (v3) |
| `setting` | Enum | Yes | `"inpatient"`, `"outpatient"`, or `"both"` |
| `billing_class` | Enum | Optional | `"professional"`, `"facility"`, or `"both"` |
| `modifiers` | String | Yes | CPT/HCPCS modifiers (added Jan 2025) |
| `drug_unit_of_measurement` | Numeric | Conditional | Required if code type is NDC (added Jan 2025) |
| `drug_type_of_measurement` | Enum | Conditional | Valid: `GR`, `ME`, `ML`, `UN`, `F2`, `EA`, `GM` (added Jan 2025) |

#### Charge fields

| Field | Type | Required | Pipeline mapping |
|---|---|---|---|
| `standard_charge\|gross` | Numeric | Yes (if applicable) | `gross_charge` column |
| `standard_charge\|discounted_cash` | Numeric | Yes (if applicable) | `discounted_cash` column |
| `standard_charge\|negotiated_dollar` | Numeric | Conditional | `negotiated_amount` column |
| `standard_charge\|negotiated_percentage` | Numeric | Conditional | Flagged `algorithm_only_rate` if no dollar amount |
| `standard_charge\|negotiated_algorithm` | String | Conditional | Preserved in `rate_raw`; flagged `algorithm_only_rate` |
| `standard_charge\|methodology` | Enum | Conditional | `charge_methodology` column |
| `median_amount` | Numeric | Conditional | **New in v3** — required if percentage/algorithm used |
| `10th_percentile` | Numeric | Conditional | **New in v3** — required if percentage/algorithm used |
| `90th_percentile` | Numeric | Conditional | **New in v3** — required if percentage/algorithm used |
| `count` | String | Conditional | **New in v3** — remittance count; required if percentage/algorithm used |
| `standard_charge\|min` | Numeric | Conditional | `deidentified_min` column; required if negotiated_dollar encoded |
| `standard_charge\|max` | Numeric | Conditional | `deidentified_max` column; required if negotiated_dollar encoded |
| `additional_generic_notes` | String | Yes | Free text; required if methodology = "other" |

#### Charge methodology values

The `methodology` field determines whether a negotiated rate is comparable to a bundled Medicare DRG payment:

| Methodology | Meaning | Comparable to Medicare DRG? |
|---|---|---|
| `case rate` | Flat rate per episode | **Yes** — most directly comparable |
| `fee schedule` | Per-service rate from a schedule | **No** — component rate, not full episode |
| `per diem` | Daily rate × length of stay | **No** — requires LOS multiplication |
| `percent of total billed charges` | Percentage of gross charge | **No** — requires conversion via gross charge |
| `other` | Unclassified; requires explanation in notes | **No** — cannot be compared without context |

#### Conditional requirement rules (key ones for parsing)

1. At least one charge type (gross, cash, or negotiated) must be present per item
2. If negotiated_dollar is encoded, `min` and `max` must also be present
3. If percentage or algorithm is used, `count` is required
4. If `count` > 0, then `median_amount`, `10th_percentile`, `90th_percentile` are required
5. If `count` = 0, an explanation in notes is required
6. `methodology` is required whenever a payer-specific charge is present
7. Code and code_type must always pair together

### File formats

Hospitals must publish in one of three CMS template formats:

| Format | Structure | Pipeline parser |
|---|---|---|
| **CSV tall** | One row per item × payer × plan. Payer name and plan in separate columns. | `csv_tall_variant` |
| **CSV wide** | One row per item. Payer/plan embedded in column headers: `standard_charge\|[payer]\|[plan]\|negotiated_dollar` | `csv_wide_standardcharges` |
| **JSON** | Nested arrays: `standard_charge_information[].standard_charges[].payers_information[]` | `json_nested_standard_charge_information` |

### File naming convention (§180.50(d)(5))

`<ein>_<hospital-name>_standardcharges.[json|csv]`

This is federally mandated, which is why EINs can be extracted directly from MRF URLs (e.g., `133971298` in NYU Langone's filename → EIN `133971298`).

### cms-hpt.txt requirement (§180.50(d)(6))

Since January 2024, every hospital must host a `cms-hpt.txt` file at their website root containing:

- Location name
- Source page URL
- MRF direct download URL
- Hospital point of contact (name and email)

This is the **scalable discovery mechanism** — a crawler hitting `<hospital-domain>/cms-hpt.txt` can automatically resolve MRF URLs without navigating the hospital's website. In this pipeline, 14 of 15 hospitals (93%) had a working `cms-hpt.txt`, above the national average of ~61% (Turquoise Health, 2024).

### Penalties for noncompliance (§180.90)

Updated by the CY2022 OPPS Final Rule (effective January 1, 2022):

| Hospital size | Daily penalty |
|---|---|
| ≤30 beds | $300/day (minimum floor) |
| 31+ beds | **$10 × number of beds per day (no cap)** |

**Examples:** A 550-bed hospital faces $5,500/day (~$2M/year). A 1,000-bed hospital faces $10,000/day (~$3.65M/year). There is **no maximum cap** — larger hospitals face proportionally higher penalties.

> Some hospitals have calculated that noncompliance fines are still cheaper than revealing their negotiated rates, which explains why data quality and completeness varies in practice.

---

## 2. CMS Medicare Data Dictionary

**Source:** `data/cms_knee_replacement_by_provider.csv` (1,377 rows: DRG 469 = 66, DRG 470 = 1,311)

This is the "known" dataset the pipeline joins transparency data against. Each field's role:

| CMS column | Definition | Pipeline use |
|---|---|---|
| `Rndrng_Prvdr_CCN` | CMS Certification Number — unique 6-digit Medicare provider ID | **Primary join key.** Links CMS records to transparency-file hospitals. First 2 digits = state code. |
| `Rndrng_Prvdr_Org_Name` | Provider name as registered with CMS | Disambiguation. Names are truncated at ~50 chars and inconsistently cased. Not used for fuzzy matching in this pipeline (CCN-first join only). |
| `Rndrng_Prvdr_City` | City where provider is physically located | Disambiguation when multiple hospitals share a name (e.g., 5 Piedmont hospitals in GA). |
| `Rndrng_Prvdr_State_Abrvtn` | Two-letter state abbreviation | Geographic analysis: high-cost (NY, CA, NJ) vs. low-cost (MS, IN, OK) markets. |
| `Rndrng_Prvdr_Zip5` | 5-digit ZIP code | Fine-grained disambiguation and geographic clustering. |
| `DRG_Cd` | Diagnosis Related Group code (469 or 470) | 469 = with MCC (complex), 470 = without MCC (routine). **DRG 469/470 covers both hip and knee** — transparency files (HCPCS 27447) provide knee-specific granularity. |
| `DRG_Desc` | DRG description | Truncated: "MAJOR HIP AND KNEE JOINT REPLACEMENT OR REATTACHMENT OF LOWER EXTREMITY..." |
| `Tot_Dschrgs` | Number of discharges billed | **Volume indicator.** Drives tier classification — higher volume = more analytical value. |
| `Avg_Submtd_Cvrd_Chrg` | Average covered charges submitted to Medicare | The "sticker price" — maps to regulatory Gross Charge. ~6.4× actual payment. |
| `Avg_Tot_Pymt_Amt` | Average total payments including patient cost-sharing | Total: Medicare + patient copay/deductible + third-party coordination of benefits. |
| `Avg_Mdcr_Pymt_Amt` | Average Medicare payment — Medicare's actual share | **The benchmark for ratio analysis.** Does NOT include patient copay/deductible. This is the denominator in `commercial_to_medicare_ratio`. |

### The key data gap this pipeline bridges

| What CMS data has | What's only in transparency files |
|---|---|
| Medicare payment rates (one number per hospital) | Commercial payer-specific negotiated rates (per payer × plan) |
| Hospital volume (aggregate discharges) | Payer-level detail (Aetna, UHC, BCBS, etc.) |
| Hospital identifiers (CCN, name, state) | Implant manufacturer and product names |
| DRG-level grouping (hip + knee combined) | HCPCS-level granularity (knee-specific via 27447) |
| ~1,400 hospitals with knee data | Implant billing codes and costs |

Bridging this gap — joining commercial negotiated rates from MRFs to Medicare benchmarks from CMS — is the core value of the pipeline.

---

## 3. Regulation → Pipeline Deliverable Mapping

| Pipeline capability | Regulatory source | What it demonstrates |
|---|---|---|
| **MRF discovery** | §180.50(d) — hospitals must post MRFs with direct download links + `cms-hpt.txt` index | Navigating real-world compliance variability (14/15 had `cms-hpt.txt`; 1 required manual URL discovery) |
| **Download and parse** | §180.50(c) — MRFs must conform to CMS template (CSV tall, CSV wide, or JSON) | Schema detection across 3 template layouts + v2/v3 version differences |
| **Procedure extraction** | §180.50(b)(2)(iv) — hospitals must include billing codes (CPT, HCPCS, DRG) | HCPCS 27447 exact match, DRG 469/470 fallback, description keyword search |
| **Payer rate extraction** | §180.20 — payer-specific negotiated charge must be tagged with payer name and plan | Parsing wide-format columns, tall-format rows, and JSON nested objects |
| **Schema normalization** | §180.50(b)(2) — CMS template defines standard data elements | 54-column canonical schema aligned with regulatory data elements |
| **CMS join** | Entity resolution across CCN, EIN, NPI | Bridging MRF identifiers (EIN in filename) to CMS identifiers (CCN in provider data) |
| **Scale potential** | §180.50(d)(6) — `cms-hpt.txt` enables automated discovery | The regulation itself provides the scaling mechanism for 5,000+ hospitals |
| **Rate analysis** | §180.20 defines all five charge types + methodology | Understanding what each charge type means is essential for valid commercial-to-Medicare comparisons |

---

## 4. Hospital Identifier Landscape

Four key identifiers used across datasets:

| Identifier | Full name | Assigned by | Format | Where it appears |
|---|---|---|---|---|
| **CCN** | CMS Certification Number | CMS | 6-digit (first 2 = state) | CMS Medicare data (`Rndrng_Prvdr_CCN`), Medicare.gov, Provider of Services file |
| **EIN** | Employer Identification Number | IRS | 9-digit | MRF URLs (mandated naming convention), IRS Form 990, HCRIS cost reports |
| **Type 2 NPI** | Organizational National Provider Identifier | CMS/NPPES | 10-digit | NPPES registry, claims data, NPI-to-CCN crosswalks, **MRF headers (required in v3)** |
| **State License #** | State facility license | State health dept | Varies by state | Inside MRF header rows (`license_number\|STATE`) |

### Crosswalk sources for scaling

| Source | Contains | Best for |
|---|---|---|
| CMS HCRIS (Cost Reports) | Both CCN and EIN | Most reliable programmatic crosswalk |
| NBER NPI-to-CCN Crosswalk | NPI ↔ CCN | Bridging NPPES to Medicare data (last updated Dec 2017) |
| Community Benefit Insight | Curated EIN-to-CCN for nonprofits | Nonprofit hospital matching |
| CMS Provider of Services (POS) | CCN + name + address | Bulk hospital lookup (no EIN) |
| NPPES Registry | NPI + name + address + sometimes CCN | API available at npiregistry.cms.hhs.gov |
| Medicare.gov Care Compare | CCN by hospital name | Manual verification |
| CMS Hospital General Info | CCN + name + address + ownership | Bulk download from data.cms.gov |

### In this pipeline

- **CCN** is curated in `config/hospitals.yaml` and used as the primary join key against `Rndrng_Prvdr_CCN`
- **EIN** is extracted from MRF URLs (CMS naming convention: `<ein>_<hospital-name>_standardcharges`)
- **Approach:** Deterministic CCN-first join for 15 hospitals; at scale, automate via HCRIS CCN↔EIN crosswalk + CMS NPPES for NPI→CCN resolution

---

## 5. Analytical Framework

### Commercial-to-Medicare ratio

The central analytical question: what is the relationship between Medicare payment rates and commercial negotiated rates?

- **Industry benchmark:** Commercial rates typically 1.5×–3× Medicare for orthopedics (RAND Hospital Price Transparency studies)
- **Pipeline formula:** `commercial_to_medicare_ratio = negotiated_amount / cms_avg_mdcr_pymt_amt`
- **Denominator:** `Avg_Mdcr_Pymt_Amt` is Medicare's actual payment — does **not** include patient copay/deductible
- **Pipeline result:** Median 1.71× across 1,338 comparable rows (see [pipeline-findings.md](pipeline-findings.md) §6)

### Key analytical dimensions

1. **Commercial-to-Medicare ratio by hospital and payer** — ratios vary widely (0.2× for Medicaid to 4.7× for some BCBS plans)
2. **Charge methodology filter** — case-rate rows (1.77× median) are the most reliable comparison; fee-schedule and per-diem rows are not comparable to DRG bundles
3. **Geographic variation** — high-cost markets (NY, CA, NJ) vs. low-cost (MS, IN, OK)
4. **Volume correlation** — high-volume hospitals (NEBH: 1,033 discharges) vs. low-volume (Warren: 11)
5. **System vs. independent** — large systems (Ascension, BSW, HCA) vs. independent hospitals
6. **Estimation potential** — if the commercial-to-Medicare multiplier is reliable by region/system type, it becomes predictive for the ~1,400 CMS hospitals without transparency data

### Why charge methodology matters more than most people realize

The biggest pitfall in hospital price transparency analysis is comparing non-comparable rate types. A `per diem` rate of $5,000/day looks cheaper than a `case rate` of $30,000, but a 7-day knee replacement stay at $5,000/day is actually $35,000. The `charge_methodology` field — mandated by the regulation — is the key to valid comparisons. The pipeline flags non-comparable rows via `dq_flags` (`percent_of_charges_noncomparable`, `algorithm_only_rate`) rather than producing misleading ratios.
