# QGI 5.0 — First-Generation AWS Cloud Migration (Pipelines A, B & C, 2025–2026)

> **[Historical Archive — Generation 5.0 of 7.0]**
> This repository preserves **QGI 5.0** (2025–2026), the fifth-generation architecture that migrated the local QGI 4.0 econometric engine to AWS cloud primitives — splitting ingestion, pairwise SCDI correlation compute, and pattern synthesis into three cloud-native microservices across AWS Lambda, Amazon S3, and Dockerized AWS Batch workers.
> - **Original Repository Names:**
>   1. `AminAlAit/qgi-pipeline-a-indicator-extractor` (qgi-pipeline-a-indicator-extractor/)
>   2. `AminAlAit/qgi-pipeline-b-scdi-finder` (qgi-pipeline-b-scdi-finder/)
>   3. `AminAlAit/qgi-pipeline-c-pattern-synthesizer` (qgi-pipeline-c-pattern-synthesizer/)
> - **Active Production Platform (QGI 7.0):** [**AminAlAit/qgi**](https://github.com/AminAlAit/qgi) (pipeline/ — [qgintelligence.com](https://qgintelligence.com))

---

## Consolidated Repository Layout (2025–2026)

| Subdirectory | Original GitHub Repository Name | Compute Runtime | Architectural Role |
| :--- | :--- | :--- | :--- |
| **qgi-pipeline-a-indicator-extractor/** | `AminAlAit/qgi-pipeline-a-indicator-extractor` | AWS Lambda (Python) | Serverless World Bank API extractor writing normalized indicator Parquet tables (lambda_function.py, deploy.ps1) to Amazon S3 |
| **qgi-pipeline-b-scdi-finder/** | `AminAlAit/qgi-pipeline-b-scdi-finder` | AWS Batch (Docker / ECR) | Containerized Spearman + Cross-Correlation + DTW (SCDI) pairwise correlation worker (scdi_worker.py, Dockerfile, 	est_local.py) |
| **qgi-pipeline-c-pattern-synthesizer/** | `AminAlAit/qgi-pipeline-c-pattern-synthesizer` | AWS Lambda (Python) | Serverless multi-indicator structural pattern synthesizer (lambda_function.py, pattern_synthesizer.py, deploy.ps1) |

---

## QGI Generational Lineage (1.0 – 7.0)

| Generation | Era | Canonical GitHub Repository | Original Repository Name(s) | Key Milestone |
| :--- | :--- | :--- | :--- | :--- |
| **QGI 1.0** | 2023 | `AminAlAit/qgi-1.0` | `trojan`, `trojan-streamlit` | Initial pairwise country correlation engine (countriesIndex.py, patternEngine.py) & first Streamlit prototype |
| **QGI 2.0** | 2023 | `AminAlAit/qgi-2.0` | `trojan-2.0` | Poetry-managed Soft Power & composite institutional index prototype |
| **QGI 3.0** | 2024 | `AminAlAit/qgi-3.0` | `qgi` (2024), `qgi-streamlit`, `qgi-local` | First public Quantitative Geopolitical Intelligence Streamlit prototype & World Bank notebook workflows |
| **QGI 4.0** | 2025 | `AminAlAit/qgi-4.0` | `qgi-4.0` | Formalization of SCDI (Spearman + Cross-Correlation + DTW), local MySQL-to-Parquet engine (Pipelines A–E), and X Bot v1 |
| **QGI 5.0** | 2025–2026 | **AminAlAit/qgi-5.0** *(this repo)* | `qgi-pipeline-a-indicator-extractor`, `qgi-pipeline-b-scdi-finder`, `qgi-pipeline-c-pattern-synthesizer` | First AWS cloud migration: Serverless Lambda indicator extractor, Dockerized AWS Batch SCDI compute worker, and Lambda pattern synthesizer |
| **QGI 6.0** | 2026 | `AminAlAit/qgi-6.0` | `qgi-aws-tf`, `qgi-infra`, `qgi-pipeline`, `qgi-data`, `qgi-ml`, `qgi-frontend`, `qgi-docs` | 7-repo multi-account AWS cloud architecture (14.3B FFT correlations), Trajectory Engine (42 event models), and Next.js 16 terminal |
| **QGI 7.0 (Active)** | 2026–Present | [**AminAlAit/qgi**](https://github.com/AminAlAit/qgi) & `AminAlAit/qgi-docs` | `qgi`, `qgi-docs` | Unified polyglot production monorepo & live analytical terminal at [qgintelligence.com](https://qgintelligence.com) |
