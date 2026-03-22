# Regulatory & Assessment Reference Guide

*Intellicent MedTech Analytics — Knee Replacement (HCPCS 27447) Pipeline*

This document connects the federal price transparency regulation (45 CFR Part 180) to the assessment deliverables, and maps the CMS Medicare data dictionary to the pipeline's analytical requirements. It serves as the reference for **why** the data exists, what it means, and how it connects.

---

## 1. How the Regulation Creates the Data We're Collecting

Source: https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-E/part-180

### The Legal Mandate

Since January 1, 2021, every US hospital must publish a machine-readable file (MRF) containing standard charges for all items and services (45 CFR §180.50). The assessment's premise — "companies like Turquoise Health have built entire businesses around collecting and normalizing this data" — exists because of this regulation.

### The Five Standard Charges (§180.20) → Assessment Schema Design

The regulation defines five types of "standard charges." These directly inform our canonical output schema and the `rate_type` field:

| Regulatory Term (§180.20) | Definition | Schema Field: `rate_type` | Assessment Relevance |
|---|---|---|---|
| **Gross Charge** | Chargemaster "sticker price" — amount on hospital's price list absent any discounts | `gross_charge` | Maps to CMS `Avg_Submtd_Cvrd_Chrg`. ~6.4x actual payment. Not useful for pricing analysis. |
| **Payer-Specific Negotiated Charge** | Charge negotiated with a specific third-party payer for an item or service. Must be tagged with payer name and plan name. | `negotiated` | **The core deliverable.** These are the commercial rates the assessment asks us to collect. CMS Medicare data does NOT have these — they only exist in the transparency files. |
| **Discounted Cash Price** | Charge for an individual who pays cash or cash equivalent | `cash` | Useful benchmark. Often between Medicare and commercial rates. |
| **De-identified Minimum Negotiated Charge** | Lowest charge negotiated with any third-party payer (anonymized) | `de-identified_min` | Floor of the negotiation range across all payers. |
| **De-identified Maximum Negotiated Charge** | Highest charge negotiated with any third-party payer (anonymized) | `de-identified_max` | Ceiling of the negotiation range. |

### Required MRF Data Elements (§180.50(b)(2)) → What We Parse

As of July 2024, hospitals using the CMS V2 template must encode these data elements. This is the schema we should expect when parsing transparency files:

**Hospital-level header fields:**
- `hospital_name`, `hospital_address`, `hospital_location` — for entity resolution
- `license_number|STATE` — state license or sometimes CCN
- `version` — CMS template version (e.g., "2.0.0")
- `last_updated_on` — data freshness indicator
- Attestation statement (true/false)

**Per item/service → maps to our pipeline's filtering and extraction:**
- `description` — plain-language service description → used in keyword matching ("knee arthroplasty", "total knee", "TKA")
- `code|1`, `code|1|type` — billing codes and types (CPT, HCPCS, DRG, NDC, RCC) → used to match HCPCS 27447
- `setting` — "inpatient" or "outpatient" → important for distinguishing facility-level vs. professional charges
- `modifiers` (as of Jan 2025) — may change the standard charge for a given service
- `drug_unit_of_measurement`, `drug_type_of_measurement` (as of Jan 2025) — for pharmaceutical items, not relevant to knee replacement procedures

**Per standard charge → maps to our canonical schema rate fields:**
- `standard_charge|gross` → `rate_type: "gross_charge"`
- `standard_charge|discounted_cash` → `rate_type: "cash"`
- `standard_charge|{payer}|{plan}|negotiated_dollar` → `rate_type: "negotiated"`, `payer_name`, `payer_plan`
- `standard_charge|{payer}|{plan}|negotiated_percentage` → percentage-based rate, requires `estimated_amount` conversion
- `standard_charge|{payer}|{plan}|negotiated_algorithm` → algorithm-based rate
- `standard_charge|min` → `rate_type: "de-identified_min"`
- `standard_charge|max` → `rate_type: "de-identified_max"`
- `standard_charge|methodology` — how the charge was established: `case rate`, `fee schedule`, `per diem`, or `percent of total billed charges`

**Charge methodology matters for knee replacement:**
- `case rate` — flat rate per episode (most comparable to DRG-level CMS data)
- `fee schedule` — based on a Medicare/Medicaid/commercial schedule
- `per diem` — daily rate × length of stay (harder to compare without knowing LOS)
- `percent of total billed charges` — percentage of gross charge (requires estimated_amount to be useful)

### File Naming Convention (§180.50(d)(5))

`<ein>_<hospital-name>_standardcharges.[json|csv]`

This is why we can extract EINs directly from transparency file URLs — it's federally mandated, not optional.

### cms-hpt.txt Requirement (§180.50(d)(6))

Since January 2024, every hospital must host a `cms-hpt.txt` file at their website root containing:
- Location name
- Source page URL
- MRF direct download URL
- Hospital point of contact (name and email)

This is the **scalable discovery mechanism** for expanding from 15 to 5,000 hospitals. A crawler hitting `<hospital-domain>/cms-hpt.txt` can automatically extract all MRF URLs without manual navigation.

### Penalties for Noncompliance (§180.90)

| Hospital Size | Max Daily Penalty |
|---|---|
| ≤30 beds | $300/day |
| 31–550 beds | beds × $10/day |
| >550 beds | $5,500/day |

Max annual penalty for a large hospital: ~$2M. Some hospitals have calculated that noncompliance fines are cheaper than revealing their negotiated rates — which explains why data quality and completeness varies.

---

## 2. CMS Medicare Data Dictionary → Assessment Requirements

Source: `Medicare_Inpatient_Hospitals_-_by_Provider_and_Service_Data_Dictionary.pdf`

The CMS data (`cms_knee_replacement_by_provider.csv`) is the "known" dataset we join transparency data against. Here's how each field connects to the assessment:

| CMS Column | Definition | Assessment Use |
|---|---|---|
| `Rndrng_Prvdr_CCN` | CMS Certification Number — unique 6-digit Medicare provider ID | **Primary join key** for entity resolution. Links CMS records to transparency file hospitals. First 2 digits = state code. |
| `Rndrng_Prvdr_Org_Name` | Provider name as registered with CMS | **Entity resolution fallback.** Names are truncated at ~50 chars and inconsistently cased. Used with fuzzy matching when CCN is unavailable. |
| `Rndrng_Prvdr_City` | City where provider is physically located | Disambiguation when multiple hospitals share a name (e.g., 5 Piedmont hospitals in GA). |
| `Rndrng_Prvdr_State_Abrvtn` | Two-letter state abbreviation | Geographic analysis: high-cost (NY, CA, NJ) vs. low-cost (MS, IN, OK) markets. |
| `Rndrng_Prvdr_Zip5` | 5-digit ZIP code | Fine-grained disambiguation and geographic clustering. |
| `DRG_Cd` | Diagnosis Related Group code (469 or 470) | 469 = with Major Complication/Comorbidity (complex), 470 = without MCC (routine). **DRG 469/470 covers BOTH hip and knee** — transparency files (HCPCS 27447) provide knee-specific granularity that CMS data lacks. |
| `DRG_Desc` | DRG description | Truncated. "MAJOR HIP AND KNEE JOINT REPLACEMENT OR REATTACHMENT OF LOWER EXTREMITY..." |
| `Tot_Dschrgs` | Number of discharges billed | **Volume indicator.** Drives tier classification — higher volume = more analytical value. |
| `Avg_Submtd_Cvrd_Chrg` | Average covered charges submitted to Medicare | The "sticker price" — maps to regulatory `Gross Charge`. ~6.4x actual payment. Not what anyone pays. |
| `Avg_Tot_Pymt_Amt` | Average total payments including patient cost-sharing | Total payments including Medicare + patient copay/deductible + third-party coordination of benefits. |
| `Avg_Mdcr_Pymt_Amt` | Average Medicare payment — Medicare's actual share | **The benchmark for commercial rate comparison.** Does NOT include patient copay/deductible. This is the denominator in the commercial-to-Medicare ratio. |

### The Key Gap the Assessment Tests

| What CMS Data Has | What It's Missing (only in transparency files) |
|---|---|
| Medicare payment rates | Commercial payer-specific negotiated rates |
| Hospital volume (discharges) | Payer-level detail (Aetna, UHC, BCBS, etc.) |
| Hospital identifiers (CCN, name, state) | Implant manufacturer and product names |
| DRG-level grouping (hip + knee combined) | HCPCS-level granularity (knee-specific via 27447) |
| Aggregate national data (~1,400 hospitals) | Implant billing codes and costs |

This gap is the entire value proposition. The assessment tests whether you can bridge it.

---

## 3. Regulatory Definitions → Assessment Deliverable Mapping

| Assessment Deliverable | Regulatory Source | What It Tests |
|---|---|---|
| **Find the hospital's MRF** | §180.50(d) — hospitals must post MRFs on publicly available websites with direct download links, cms-hpt.txt index, and "Price Transparency" footer link | Data acquisition: Can you navigate the real-world messiness of how hospitals comply (or don't)? |
| **Download and parse it** | §180.50(c) — MRFs must conform to CMS template (CSV tall, CSV wide, or JSON) | Schema detection: Can you handle three template layouts plus pre-V2 non-standard formats? |
| **Extract HCPCS 27447 records** | §180.50(b)(2)(iv) — hospitals must include billing codes (CPT, HCPCS, DRG) | Code matching: HCPCS 27447 exact match, DRG 469/470 fallback, description keyword search |
| **Pull out payer names and negotiated rates** | §180.20 — "payer-specific negotiated charge" must be tagged with payer name and plan | Parsing payer-specific columns in wide format vs. rows in tall format vs. nested objects in JSON |
| **Pull out implant detail** | §180.20 — "items and services" includes supplies and procedures; some hospitals publish device-level charges | Most hospitals won't have this. Schema must handle nulls gracefully. |
| **Normalize into a common schema** | §180.50(b)(2) — CMS V2 template defines standard data elements | Schema design: your canonical schema should align with the regulatory data elements |
| **Match to CMS records** | Entity resolution across CCN, EIN, NPI, names | The regulation uses EIN in file naming; CMS data uses CCN. Bridging these is the entity resolution challenge. |
| **Scale from 15 to 5,000** | §180.50(d)(6) — cms-hpt.txt enables automated discovery | The regulatory framework itself provides the scaling mechanism |
| **Analyze Medicare vs. commercial patterns** | Gross Charge vs. Payer-Specific Negotiated Charge vs. Discounted Cash Price — all defined in §180.20 | Understanding what each charge type means is essential for valid comparisons |

---

## 4. Hospital Identifier Landscape

Four key identifiers used across datasets:

| Identifier | Full Name | Assigned By | Format | Where It Appears |
|---|---|---|---|---|
| **CCN** | CMS Certification Number | CMS | 6-digit (first 2 = state) | CMS Medicare data (`Rndrng_Prvdr_CCN`), Medicare.gov, Provider of Services file |
| **EIN** | Employer Identification Number | IRS | 9-digit | Transparency file URLs (mandated naming convention), IRS Form 990, HCRIS cost reports |
| **Type 2 NPI** | Organizational National Provider Identifier | CMS/NPPES | 10-digit | NPPES registry, claims data, NPI-to-CCN crosswalks |
| **State License #** | State facility license | State health dept | Varies by state | Inside MRF header rows (`license_number\|STATE`) |

### Crosswalk Sources for Scaling

| Source | Contains | Best For |
|---|---|---|
| CMS HCRIS (Cost Reports) | Both CCN and EIN | Most reliable programmatic crosswalk |
| NBER NPI-to-CCN Crosswalk | NPI ↔ CCN | Bridging NPPES to Medicare data (last updated Dec 2017) |
| Community Benefit Insight | Curated EIN-to-CCN for nonprofits | Nonprofit hospital matching |
| CMS Provider of Services (POS) | CCN + name + address | Bulk hospital lookup (no EIN) |
| NPPES Registry | NPI + name + address + sometimes CCN | API available at npiregistry.cms.hhs.gov |
| Medicare.gov Care Compare | CCN by hospital name | Manual verification |
| CMS Hospital General Info | CCN + name + address + ownership | Bulk download from data.cms.gov |

### For This Pipeline

- **CCN** comes from `cms_knee_replacement_by_provider.csv` (`Rndrng_Prvdr_CCN`)
- **EIN** is extracted from transparency file URLs (CMS naming convention: `<ein>_<hospital-name>_standardcharges`)
- **Mapping approach:** Hardcoded lookup table for 15 hospitals; at scale, use HCRIS cost report data for CCN↔EIN crosswalk, and cms-hpt.txt for automated MRF URL discovery

---

## 5. Analytical Framework

### Commercial-to-Medicare Ratio
The assessment explicitly asks about "patterns or relationships between Medicare payment data and commercial negotiated rates."

- Industry benchmark: commercial rates typically 1.5x–3x Medicare for orthopedics (source: RAND Hospital Price Transparency studies)
- Formula: `commercial_to_medicare_ratio = negotiated_rate / cms_avg_medicare_pymt`
- The CMS field `Avg_Mdcr_Pymt_Amt` is the denominator — it's Medicare's actual payment, NOT including patient cost-sharing

### Key Analyses for the README

1. **Commercial-to-Medicare ratio** by hospital and payer — do ratios cluster or vary widely?
2. **Geographic variation** — do high-cost markets (NY, CA, NJ) show higher ratios than low-cost (MS, IN, OK)?
3. **Volume correlation** — do high-volume hospitals (NEBH: 1,033 discharges) negotiate different rates than low-volume (Warren: 11)?
4. **System vs. independent** — do large systems (Ascension, BSW, HCA) show different pricing than independent hospitals?
5. **Estimation potential** — if the commercial-to-Medicare multiplier is reliable by region/system type, you could estimate commercial rates for the ~1,400 CMS hospitals without transparency data. This is the core business model of companies like Turquoise Health.

### What the Assessment is Really Testing

The README prompt "whether those relationships could be used to estimate pricing where transparency data isn't available" is asking you to demonstrate you understand the **business value** of this work. If you can show that a $14,000 Medicare payment in MA consistently maps to ~$42,000 commercial rates (3x), that relationship becomes predictive — and that's what makes this data commercially valuable for MedTech pricing intelligence.
