# Technical Note – Use of Temporary Views in the Healthcare Encounters Pipeline

This [project](https://verulamblue.com/challenges) implements the encounters cleaning pipeline using **temporary views** rather than physical tables or materialised views.

## Why temporary views?

Within the [Verulam Blue Mint](https://verulamblue.com/) environment, the execution model is as follows:

- Users run SQL in a **session-scoped, in-memory workspace**.
- Intermediate artefacts are expected to be **ephemeral** and re-created on demand.
- The only persistent object required for this project is the **KPI results table** (`kpi_results`), which is exported and used for validation / reporting.

Given this, the pipeline is structured as a sequence of `CREATE OR REPLACE TEMP VIEW` statements:

- Each phase (staging, parsing, normalisation, LOS, deduplication, ID generation) is exposed as its own named view.
- The final two views, `silver_encounters_final` and `gold_encounters_final`, are defined at the end of the script and are recomputed when the pipeline runs.

This approach has a few practical benefits in the VBM context:

1. **Debuggability**

   Any intermediate step can be inspected directly:

   ```sql
   SELECT * FROM encounters_silver_los LIMIT 50;
   SELECT * FROM encounters_silver_dedup_flagged WHERE duplicate_row_flag = 1;
   ```

   This makes it straightforward to trace issues through the pipeline without managing physical staging tables.

2. **Stateless, repeatable runs**

   Because the views are temporary and re-created on each execution, there is no risk of stale staging tables or partially refreshed data. Every run starts from the source table **C08_l01_healthcare_encounters_data_table** and rebuilds the views in a consistent order.

3. **Alignment with the platform’s persistence model**

   The [Verulam Blue Mint](https://verulamblue.com/) hosting model is designed to persist outputs (such as kpi_results) rather than long-lived staging layers. 

   Using temp views for the pipeline steps fits this constraint cleanly: the logic is fully expressed in SQL, but only the KPI result set needs to be stored or exported.

## What would change in a production or enterprise environment?

In a production-grade stack — for example on **Microsoft Fabric**, **Azure Databricks**, or **Snowflake** — the same logic would normally be deployed using a **Medallion Architecture**:

- **Bronze layer** – Raw ingestion tables straight from source systems (light schema alignment, no cleaning yet)
- **Silver layer** – Cleaned and normalised datasets with business rules applied (date validation, categorical standardisation, deduplication, IDs)
- **Gold layer** – Curated, analysis-ready tables used directly for KPIs, dashboards, or ML features

Each layer can be persisted as a table or view, refreshed on a schedule, and versioned for auditability.  
In tools like **dbt**, **Fabric Data Pipelines**, or **Databricks workflows**, each of the `TEMP VIEW` steps in this project would map naturally to a **model** within the Medallion tiers.

For **Verulam Blue Mint** [challenges](https://verulamblue.com/challenges), the key constraint is that projects run in an ephemeral, browser-based workspace: SQL, dbt and PySpark jobs are executed on demand, intermediate objects are not persisted between runs, and only the final KPI result table needs to be stored for validation and feedback.

Using temporary views in this SQL script mirrors that behaviour: the full Bronze–Silver–Gold pipeline can be rebuilt from the raw encounters table whenever required, while persistence is reserved for the `kpi_results` output that is consumed by the platform and downstream tools.


