HMIS Biweekly ETL (Latest-File Snapshot)
This repository contains a Python ETL pipeline that ingests the latest HMIS exports from several staging folders, cleans/standardizes them, and produces a consolidated biweekly final dataset for reporting.

What it does
For each dataset folder (and optional Archive/ subfolder), the pipeline:

Finds the latest CSV/XLSX (by date in filename or file modified time).
Cleans and standardizes columns (renames, trimming, datatype enforcement, date parsing).
Deduplicates rows based on dataset-specific keys.
Writes a single latest snapshot parquet per dataset into the “Second Staging Area”.
Builds a final consolidated view with:
Case Notes and Services pivoted into monthly wide columns
Latest CES assessment + score per client
Document readiness flags inferred from “List of Client File Name”
CES status (Current / Renewal Overdue / Not Done)
Intervention alert rules based on Days in Project + documentation/CES risk
Outputs are written as Parquet, CSV, and Excel.

Folder layout (expected)
Base folder contains:

Staging Area\Biweekly Report\Program Client Data\
Staging Area\Biweekly Report\CES Assessments\
Staging Area\Biweekly Report\Case Notes\
Staging Area\Biweekly Report\Services\
Outputs:

Report Outputs\Second Staging Area\ (per-dataset parquet snapshots)
Report Outputs\Output\ (final report files)
_pipeline_state\ (cache metadata)
Requirements
Python 3.9+ (recommended 3.10+)
pandas
pyarrow (for Parquet)
openpyxl (for Excel output)
