# Rearc Technical Assessment

This repository contains the source code and output files for the given technical assessment.  
The source code is organized under the **`src`** directory.

---

## Pre-Requisites

To execute the provided files, ensure the following:

- Personal AWS cloud account with **Access Key** and **Secret Key**
- Install the required libraries using the **`requirements.txt`** file
- IDE (e.g., PyCharm, VS Code) installed
- Python version **3.10** or higher

---

## Part 1: AWS S3 & Sourcing Datasets

- Directory: **`src/part1`**  
- Script: **`bls_data.py`**  
  - Processes and synchronizes data files from the **BLS website server** with existing files in an **S3 bucket**, regardless of file names.

---

## Part 2: APIs Read

- Directory: **`src/part2`**  
- Script: **`data_usa.py`**  
  - Fetches data from the **USA population public server** and stores it in an **S3 bucket** in JSON format.

---

## Part 3: Data Analytics

- Directory: **`src/part3`**  
- Script: **`best_seriesyear.ipynb`**  
  - Reads data from both the **BLS dataset** and the **USA population public server**, merges the datasets, and generates the required analytical results.

---

## Part 4: Infrastructure as Code & Data Pipeline with Terraform

- Directory: **`src/part4`**  
- Script: **`main.tf`**  
  - Automates the execution of **Part 1** and **Part 2** scripts as **Lambda functions**.  
  - Triggers an **SQS queue** to invoke another Lambda function containing the **Part 3** code logic.

---
