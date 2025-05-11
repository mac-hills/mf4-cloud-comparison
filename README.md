# Cloud comparison 
## Converting .mf4 files to .parquet
This repository contains all PowerShell scripts and resources used for the comparative study of cloud-based data processing pipelines for automotive sensor data. 
The project benchmarks the conversion of raw `.mf4` files to `.parquet` format on both AWS and Azure, measuring performance, throughput, and cost.

## Overview
The scripts in this repository automate the following workflow for both AWS and Azure:
- Uploading .mf4 files to cloud storage (AWS S3 or Azure Blob Storage)
- Triggering serverless functions (AWS Lambda or Azure Function App) to decode .mf4 files using a .dbc file and convert them to .parquet
- Storing the resulting .parquet files in a separate output container/bucket
- Measuring upload time, processing time, throughput, and estimated costs
- Generating a structured .json report with all results for further analysis

## Repository Structure
- aws.ps1: Script for uploading and processing files on AWS (standalone)
- azure.ps1: Script for uploading and processing files on Azure (standalone)
- comparison.ps1: Integrated script that runs the full benchmark on both platforms and generates a comparison report
- readme.md This file

## How to Use
Clone this repository.
Adjust the configuration section in the scripts to match your local paths and cloud credentials.
Run comparison.ps1 to execute the full benchmark and generate a results report.
Analyze the output in comparison-results.json or use the scripts as a template for your own experiments.

## Research Context
These scripts were developed as part of a bachelor project at Hogeschool PXL. The goal is to objectively compare the performance and cost of cloud-based data pipelines for automotive sensor data on AWS and Azure.

## License
This repository is provided for academic and research purposes. See the [MIT License](https://opensource.org/license/mit) for details.
