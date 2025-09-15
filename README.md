# Spotify Streaming Data Pipeline (Azure Databricks, Medallion: Bronze → Silver → Gold)

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
![Platform](https://img.shields.io/badge/Platform-Azure%20Databricks-blue)
![Storage](https://img.shields.io/badge/Storage-Delta%20Lake-blueviolet)
![Orchestration](https://img.shields.io/badge/Orchestration-Delta%20Live%20Tables-orange)
![Ingestion](https://img.shields.io/badge/Ingestion-Streaming%20via%20Autoloader-yellow)
![Compute](https://img.shields.io/badge/Compute-Photon%20ON-success)


Production-grade **streaming pipeline** built on **Azure Databricks (Unity Catalog)** using **Autoloader**, **Delta Live Tables (DLT)**, and **PySpark**.  
This project demonstrates:  
- Schema-qualified routing (`spotify_bronze`, `spotify_silver`, `spotify_gold`)  
- Modular transformations for streaming ETL  
- Real-time ingestion from **chunked CSVs** simulating event streams  

---

```mermaid
flowchart LR
    ChunkedCSV["Chunked CSV Files (5 parts)"] -->|Autoloader| Bronze["Bronze (Raw Streaming)"]
    Bronze -->|Clean & Normalize| Silver["Silver (Curated Streaming)"]
    Silver -->|Aggregate & Rank| GoldSongs["Gold: Top Songs per Year"]
    Silver -->|Window Trends| GoldTrends["Gold: Yearly Stream Trends"]
    Silver -->|Business Rules| GoldArtists["Gold: Top Artists"]
```
---
