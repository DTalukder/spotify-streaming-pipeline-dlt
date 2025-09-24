# Pain Points & Decisions


**Streaming Simulation:**
- Manually split the dataset into 5 chunks (`chunk_1.csv` … `chunk_5.csv`) to simulate arrival of new files.
- This worked well, but ingestion timing was manual (drop files into `/streaming/` folder one-by-one).

**Decision:**
- Used `Auto Loader` in triggered mode with manual file drops to simulate streaming behavior. Chose `cloudFiles.includeExistingFiles = true` to validate incremental ingestion without needing advanced eventing.

---

**Schema Drift Handling:**
- Raw CSVs contained free-text fields (artist, album, genre) with inconsistent casing and spacing.
- Delta Live Tables required explicit schema definition (StructType) to avoid Autoloader schema inference issues.
- Null values and “empty strings” had to be normalized in the Silver layer.

**Decision:** 
- Enforced schema explicitly in the Bronze layer with StructType.

---

**Unity Catalog Constraints:**
- Only one metastore allowed per region. Had to reuse existing UC setup.
- Required schema-qualified routing (`spotify_bronze`, `spotify_silver`, `spotify_gold`) to keep pipeline outputs organized.

**Decision:** 
- Maintained logical separation by creating dedicated schemas per layer within the same catalog. Used UC-qualified paths in all DLT decorators to ensure proper governance and table lineage tracking.

---

**Cost Controls:**
- NAT Gateway cost too high → replaced with custom VNet + private endpoint approach.
- Idle clusters incurred charges → enforced Triggered mode runs instead of Continuous for dev/testing.

**Decision:** 
- Re-architected networking with private endpoints to avoid NAT usage. Switched pipeline mode to Triggered during development and turned on Photon acceleration to reduce runtime costs.
