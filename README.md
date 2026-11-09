# QGI 5.0 — Pipeline B: SCDI Finder (AWS Batch Docker Worker, 2025–2026)

> [!IMPORTANT]
> **Historical Archive (`QGI v5.0`):** This repository is part of **QGI 5.0** (the first-generation AWS cloud migration of Quantitative Geopolitical Intelligence). Active production development now resides in **QGI 7.0** at [`AminAlAit/qgi/tree/main/pipeline`](https://github.com/AminAlAit/qgi/tree/main/pipeline).

## QGI Generational Lineage (`v1.0` → `v7.0`)

| Generation | Period | Architecture & Milestone | Repository |
|---|---|---|---|
| **QGI 1.0** | Mar–Oct 2023 | Initial Pairwise Country Correlation Engine & Streamlit Prototype | `AminAlAit/trojan`, `AminAlAit/trojan-streamlit` |
| **QGI 2.0** | Nov 2023 | Soft Power & Institutional Composite Index Prototype | `AminAlAit/trojan-2.0` |
| **QGI 3.0** | Jan–Feb 2024 | Standalone Multi-Page Streamlit Analytical Terminal | [`AminAlAit/qgi-streamlit`](https://github.com/AminAlAit/qgi-streamlit) |
| **QGI 4.0** | Feb–Sep 2025 | Local SCDI Formalization, MySQL→Parquet Engine (Pipelines A–E) & X Bot v1 | `AminAlAit/qgi-4.0` |
| **QGI 5.0** | Sep 2025–Mar 2026 | First-Generation AWS Cloud Migration (Decoupled Lambda & Batch Microservices) | `AminAlAit/qgi-pipeline-a/b/c-*` |
| **QGI 6.0** | Mar–Aug 2026 | 7-Repo Multi-Account AWS Cloud (14.3B FFT Correlations) & Trajectory Engine (42 Models) | `AminAlAit/qgi-{aws-tf,infra,pipeline,data,ml,frontend}` |
| **QGI 7.0** | Oct 2026–Present | **Unified Public Monorepo & Sovereign IP Vault (Active Production)** | [`AminAlAit/qgi`](https://github.com/AminAlAit/qgi) & `AminAlAit/qgi-docs` |

---

## Architectural Role in QGI 5.0

**Pipeline B (`qgi-pipeline-b-scdi-finder`)** containerized the Single Dyadic Correlating Index (SCDI) computation for execution on AWS Batch (pushed to Management ECR `<AWS_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com/qgi-pipeline-b-scdi-finder`), reading normalized indicator partitions from Pipeline A and emitting dyadic correlation matrices to S3.