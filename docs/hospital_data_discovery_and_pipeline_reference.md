# Hospital Data Discovery & Pipeline Reference

*Intellicent MedTech Analytics — Knee Replacement (HCPCS 27447) Pipeline*

---

## Tier Definitions

- **Tier 1 — Start Here:** High-volume, large systems, likely well-structured transparency files. Maximum data for least effort.
- **Tier 2 — Process Next:** Mid-complexity. Large systems where extracting one hospital from a system-wide file is the challenge, or mid-size hospitals with decent compliance.
- **Tier 3 — Document, Don't Over-Invest:** Small/rural hospitals, missing from CMS data, or likely minimal knee replacement data.

**Decision factors:**
1. **Discharge volume** — more data = more value.
2. **Data likelihood** — large systems publish cleaner files.
3. **CMS matchability** — absent from CMS knee data = no Medicare-vs-commercial comparison.

---

## Table 1: Hospital Tier Assignments

| Hospital | State | Tier | DRG 470 Dschrgs | Avg Medicare Pymt | CMS Match Status | Data Likelihood | Tier Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- |
| NYU Langone Orthopedic Hospital | NY | Tier 1 | 1,086 | $21,298 | Umbrella entity (CCN 330214) | High — large academic system | Highest discharge count. Major academic system with resources for clean data publishing. |
| New England Baptist Hospital | MA | Tier 1 | 1,033 | $14,385 | Exact match | High — specialty ortho center | Highest knee volume among the 15. Dedicated orthopedic hospital. |
| BSW Orthopedic & Spine – Arlington | TX | Tier 1 | 489 | $10,207 | Name truncated ~50 chars | High — large system | Large TX health system, high volume. Separate entity from main BSW. |
| Novant Health Forsyth Medical Center | NC | Tier 1 | 309 | $12,464 | Exact match | High — large system | Clean CMS match, strong volume. Large integrated system. |
| Ascension St. Vincent Evansville | IN | Tier 2 | 347 | $11,981 | Period dropped; legal name differs | Medium — system-wide file | Good volume. cms-hpt.txt uses legal name 'St. Mary's Health, Inc.' XLSX version broken; using CSV. |
| Hoag Orthopedic Institute | CA | Tier 2 | 298 | $14,133 | CMS: Hoag Orthopedic Institute | Medium — entity ambiguity | Different entity from Hoag Memorial. CMS also lists Hoag Orthopedic Institute (CCN 050769). |
| Atlanticare Regional Medical Center | NJ | Tier 2 | 144 | $14,140 | CMS adds 'City Campus' | Medium — mid-size regional | MRF hosted by third-party vendor Panacea, not on hospital domain. |
| Merit Health River Oaks | MS | Tier 2 | 111 | $11,327 | Exact match | Medium — mid-size | Clean CMS match. Good for geographic diversity (MS low-cost market). |
| HCA Florida Oak Hill Hospital | FL | Tier 2 | 45 | $12,625 | CMS: 'HCA Florida Oak Hill' | Medium — HCA system file | JSON on Azure Blob with SAS token. Re-grab from cms-hpt.txt if expired. |
| Piedmont Hospital (Atlanta) | GA | Tier 2 | 29 | $10,933 | Exact but 5 Piedmonts in GA | Medium — disambiguation needed | Low knee volume. Five Piedmont hospitals in GA require CCN-based disambiguation. |
| HonorHealth Deer Valley Medical Center | AZ | Tier 2 | 17 | $12,741 | Capitalization difference | Medium — regional system | Very low volume (17 discharges). Minor match issue. |
| Hillcrest Hospital South | OK | Tier 3 | 15 | $9,472 | Exact but multiple Hillcrests | Low — small community | Only 15 discharges. Minimal analytical value. |
| Warren Memorial Hospital | VA | Tier 3 | 11 | $16,427 | Exact match | Low — small rural | Just 11 discharges. Only hospital without cms-hpt.txt. MRF found manually. |
| Grayling Hospital | MI | Tier 3 | — | — | NOT FOUND in CMS data | Low — no CMS record | Not in CMS knee data. Parent: Munson Healthcare (CCN 230097). Alt URL also works on Azure Blob. |
| Adventist Health Reedley | CA | Tier 3 | — | — | NOT FOUND in CMS data | Low — no CMS record | Not in CMS knee data. Uses third-party vendor (PARA Healthcare) with dynamic endpoint. |

---

## Table 2: Data Discovery — URLs, Identifiers & cms-hpt.txt Compliance

All MRF URLs verified against each hospital's `cms-hpt.txt` file (see `MRF_URL_Comparison.xlsx`).
Format breakdown: **12 CSV, 3 JSON**. `cms-hpt.txt` found: **14/15 (93%)**. Only Warren Memorial missing.

| Hospital | St | CCN | EIN | Dschrgs / Medicare | Fmt | cms-hpt.txt | MRF URL | Source Page |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| NYU Langone Orthopedic Hospital | NY | 330214 | 133971298 | 1,086 / $21,298 | CSV | ✅ [nyulangone.org/cms-hpt.txt](https://nyulangone.org/cms-hpt.txt) | [standardcharges.csv](https://standard-charges-prod.s3.amazonaws.com/pricing_files/133971298-1669578324_nyu-langone-orthopedic-hospital_standardcharges.csv) | [med.nyu.edu/standard-charges](https://med.nyu.edu/standard-charges/) |
| New England Baptist Hospital | MA | 220088 | 042103612 | 1,033 / $14,385 | JSON | ✅ [nebh.org/cms-hpt.txt](https://nebh.org/cms-hpt.txt) | [standardcharges.json](https://nebh.org/042103612_new-england-baptist-hospital_standardcharges.json) | [nebh.org price transparency](https://nebh.org/patients-visitors/billing-financial-services/price-transparency) |
| BSW Orthopedic & Spine – Arlington | TX | 670067 | 261578178 | 489 / $10,207 | JSON | ✅ [bswarlington.com/cms-hpt.txt](https://bswarlington.com/cms-hpt.txt) | [standardcharges.json](https://mrfs.hyvehealthcare.com/USPI/261578178_BaylorOrthopedicandSpineHospitalatArlington_standardcharges.json) | [bswarlington.com pricing](https://bswarlington.com/patients-visitors/hospital-pricing-information/) |
| Novant Health Forsyth Medical Center | NC | 340014 | 56-0928089 | 309 / $12,464 | CSV | ✅ [novanthealth.org/cms-hpt.txt](https://novanthealth.org/cms-hpt.txt) | [standardcharges.csv](https://www2.novanthealth.org/Public_Files/regulatory/56-0928089_novant_health_forsyth_medical_center_standardcharges.csv) | [novanthealth.org price transparency](https://www.novanthealth.org/for-patients/billing--insurance/price-transparency/) |
| Ascension St. Vincent Evansville | IN | 150100 | 350869065 | 347 / $11,981 | CSV | ✅ [healthcare.ascension.org/cms-hpt.txt](https://healthcare.ascension.org/cms-hpt.txt) | [standardcharges.csv](https://healthcare.ascension.org/-/media/project/ascension/healthcare/price-transparency-files/in-csv/350869065_st-marys-health-inc_standardcharges.csv) | [ascension.org price transparency](https://healthcare.ascension.org/price-transparency) |
| Hoag Orthopedic Institute | CA | 050769 | 611588294 | 298 / $14,133 | CSV | ✅ [hoagorthopedicinstitute.com/cms-hpt.txt](https://www.hoagorthopedicinstitute.com/cms-hpt.txt) | [standardcharges.csv](https://www.hoagorthopedicinstitute.com/documents/611588294_hoag-orthopedic-institute_standardcharges.csv) | [hoag billing](https://www.hoagorthopedicinstitute.com/for-patients/billing-and-insurance/) |
| Atlanticare Regional Medical Center | NJ | 310064 | 21-0634549 | 144 / $14,140 | CSV | ✅ [atlanticare.org/cms-hpt.txt](https://www.atlanticare.org/cms-hpt.txt) | [panaceainc.com download](https://atlanticare.pt.panaceainc.com/MRFDownload/atlanticare/atlanticare) | [atlanticare billing](https://www.atlanticare.org/patients-and-visitors/for-patients/billing-and-insurance/hospital-charge-list/) |
| Merit Health River Oaks | MS | 250138 | 640626874 | 111 / $11,327 | CSV | ✅ [merithealthriveroaks.com/cms-hpt.txt](https://www.merithealthriveroaks.com/cms-hpt.txt) | [standardcharges.csv](https://www.merithealthriveroaks.com/Uploads/Public/Documents/charge-masters/charge-masters-2024/640626874_merit-health-river-oaks_standardcharges.csv) | [merit health pricing](https://www.merithealthriveroaks.com/pricing-information) |
| HCA Florida Oak Hill Hospital | FL | 100264 | 62-1113740 | 45 / $12,625 | JSON | ✅ [hcafloridahealthcare.com/cms-hpt.txt](https://www.hcafloridahealthcare.com/cms-hpt.txt) | [standardcharges.json (Azure SAS)](https://stctrprodsnsvc00455826e6.blob.core.windows.net/pt-final-posting-files/62-1113740_HCA-FLORIDA-OAK-HILL-HOSPITAL_standardcharges.json?si=pt-json-access-policy&spr=https&sv=2024-11-04&sr=c&sig=o5IofreS%2F7ETlsnhPakPWCwHVVUZRobywQ5wUKGjVuQ%3D) | [hca pricing](https://www.hcafloridahealthcare.com/patient-resources/patient-financial-resources/pricing-transparency-cms-required-file-of-standard-charges) |
| Piedmont Hospital (Atlanta) | GA | 110083 | 580566213 | 29 / $10,933 | CSV | ✅ [piedmont.org/cms-hpt.txt](https://www.piedmont.org/cms-hpt.txt) | [standardcharges.csv](https://www.piedmont.org/-/media/files/patients-and-visitors/price-estimates/price-estimates-2026/580566213_piedmont-atlanta-hospital_standardcharges.csv) | [piedmont price estimates](https://www.piedmont.org/patients-and-visitors/price-estimates) |
| HonorHealth Deer Valley Medical Center | AZ | 030092 | 860181654 | 17 / $12,741 | CSV | ✅ [honorhealth.com/cms-hpt.txt](https://www.honorhealth.com/cms-hpt.txt) | [standardcharges.csv](https://www.honorhealth.com/sites/default/files/2023-12/860181654_honorhealthdvmc_standardcharges.csv) | [honorhealth pricing](https://www.honorhealth.com/patients-visitors/average-pricing) |
| Hillcrest Hospital South | OK | 370202 | 45-2711804 | 15 / $9,472 | CSV | ✅ [hillcrestsouth.com/cms-hpt.txt](https://hillcrestsouth.com/cms-hpt.txt) | [standardcharges.csv](https://coc.ardenthealthservices.com/oklahoma/45-2711804_Hillcrest-Hospital-South_standardcharges.csv) | [hillcrest pricing](https://hillcrest.com/policies-and-disclosures/price-transparency) |
| Warren Memorial Hospital | VA | 490033 | 540488103 | 11 / $16,427 | CSV | ❌ Not found | [standardcharges.csv](https://www.valleyhealthlink.com/app/files/public/bc97a127-6707-47f7-89da-e450c0072f1b/YourVisit/540488103_warren-memorial-hospital-inc_standardcharges.csv) | N/A (cms-hpt.txt missing) |
| Grayling Hospital | MI | N/A | 471161992 | — / — | CSV | ✅ [munsonhealthcare.org/cms-hpt.txt](https://www.munsonhealthcare.org/cms-hpt.txt) | [standardcharges.csv](https://www.munsonhealthcare.org/sites/default/files/hpt/471161992_munson-healthcare-grayling_standardcharges_1.csv) | [munson chargemaster](https://www.munsonhealthcare.org/patients-visitors/munson-healthcare-chargemaster-information) |
| Adventist Health Reedley | CA | N/A | N/A | — / — | CSV (dynamic) | ✅ [adventisthealth.org/cms-hpt.txt](https://www.adventisthealth.org/cms-hpt.txt) | [PARA dynamic endpoint](https://apps.para-hcfs.com/PTT/FinalLinks/Reports.aspx?dbName=dbAHRREEDLEYCA&type=CDMWithoutLabel&fileType=CSV) | [adventist price transparency](https://www.adventisthealth.org/patients-and-visitors/price-transparency/) |

---

## Discovery Notes

- **URL Verification:** All MRF URLs cross-verified against `cms-hpt.txt` files. Where discrepancies existed, the `cms-hpt.txt` version was used as the authoritative source.
- **Ascension St. Vincent:** `cms-hpt.txt` lists the file under legal entity "St. Mary's Health, Inc." with a CSV in the `/in-csv/` path. The XLSX version in the `/in/` path does not work. Using CSV.
- **Hoag:** File is for Hoag Orthopedic Institute (EIN 611588294), **NOT** Hoag Memorial Hospital. These are different entities. CMS data also lists Hoag Orthopedic Institute at CCN 050769.
- **Atlanticare:** MRF hosted by third-party vendor Panacea (`panaceainc.com`). URL does not follow CMS file naming convention.
- **HCA Oak Hill:** JSON on Azure Blob Storage with SAS token. Token may expire — re-grab from `cms-hpt.txt` if download fails.
- **Grayling:** Two working URLs: Azure Blob (`sthpiprd.blob.core.windows.net`) and Munson Healthcare domain. Both return the same file. Using Munson domain from `cms-hpt.txt`.
- **Adventist Health Reedley:** Third-party vendor (PARA Healthcare) with dynamic ASP.NET endpoint. Not standard CMS file naming.
- **Warren Memorial:** Only hospital without a working `cms-hpt.txt` (returns Page Not Found). MRF URL discovered manually from price transparency page.
- **Grayling & Adventist Reedley:** Not in CMS knee replacement data. Cannot perform Medicare-vs-commercial rate comparison. CCN listed as N/A.
- **`cms-hpt.txt` Scaling:** 14/15 hospitals (93%) have a working `cms-hpt.txt` — well above the national average of ~61% (Turquoise Health, 2024). This file is the key mechanism for automated MRF discovery when scaling from 15 to 5,000 hospitals.

---

## CMS Knee Replacement Data Quick Reference

**File:** `data/cms_knee_replacement_by_provider.csv`

| Metric | Value |
| --- | --- |
| Total rows | 1,377 (DRG 469: 66 rows, DRG 470: 1,311 rows) |
| Average Medicare Payment | $14,202 |
| Median Medicare Payment | $12,751 |
| Charge-to-Payment Ratio | ~6.4× |

> **Key insight:** DRG 469/470 covers **both hip and knee** replacement. Transparency files (HCPCS 27447) provide knee-specific granularity that the CMS data lacks.
