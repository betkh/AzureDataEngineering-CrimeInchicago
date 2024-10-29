[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=16374176&assignment_repo_type=AssignmentRepo)

# CSCI 622 - Data Engineering Project: Insights for Chicago to Improve Public Safety

This project aims to support data-driven public safety initiatives for Chicago. Using 3 separate public data sets from [Data.gov](https://www.data.gov/), the project ingests, transforms, and analizes data to unlock insights to improve public safety. The project design follows the Data Engineering lifecycle, with clear documentation of each phase: **Ingestion**, **Transformation**, and **Serving (Analysis)**.

---

## STEP-1: Ingestion

### Three Data Sources

This project uses three datasets from [Data.gov](https://www.data.gov/), each of which provides valuable information to support analysis and unlock insights to improve public safety in Chicago.

### Source #1: Crimes - 2001 to Present

| Attribute                 | Details                                                                                                                                                                                |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Dataset URL**           | [Crimes - 2001 to Present](https://catalog.data.gov/dataset/crimes-2001-to-present)                                                                                                    |
| **About Data**            | [`https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data`](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data) |
| **API Endpoint**          | [`https://data.cityofchicago.org/resource/ijzp-q8t2.json`](https://data.cityofchicago.org/resource/ijzp-q8t2.json)                                                                     |
| **API Documentation**     | [`https://dev.socrata.com/foundry/data.cityofchicago.org/ijzp-q8t2`](https://dev.socrata.com/foundry/data.cityofchicago.org/ijzp-q8t2)                                                 |
| **Data Owner**            | Chicago Police Department                                                                                                                                                              |
| **Date Created**          | September 30, 2011                                                                                                                                                                     |
| **Data Update Frequency** | `Daily`                                                                                                                                                                                |
| **Rows**                  | `8.19M` (each row represents a reported crime, anonymized to the block level)                                                                                                          |
| **Columns**               | `22`                                                                                                                                                                                   |

### Source #2: Arrests

| Attribute                 | Details                                                                                                                                                  |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Dataset URL**           | [Arrests](https://catalog.data.gov/dataset/arrests)                                                                                                      |
| **About Data**            | [`https://data.cityofchicago.org/Public-Safety/Arrests/dpt3-jri9/about_data`](https://data.cityofchicago.org/Public-Safety/Arrests/dpt3-jri9/about_data) |
| **API Endpoint**          | [`https://data.cityofchicago.org/resource/dpt3-jri9.json`](https://data.cityofchicago.org/resource/dpt3-jri9.json)                                       |
| **API Documentation**     | [`https://dev.socrata.com/foundry/data.cityofchicago.org/dpt3-jri9`](https://dev.socrata.com/foundry/data.cityofchicago.org/dpt3-jri9)                   |
| **Data Owner**            | Chicago Police Department                                                                                                                                |
| **Date Created**          | June 22, 2020                                                                                                                                            |
| **Data Update Frequency** | Daily                                                                                                                                                    |
| **Rows**                  | `660K` (each row represents an arrest, anonymized to the block level)                                                                                    |
| **Columns**               | `24`                                                                                                                                                     |

### Source #3: Socioeconomically Disadvantaged Areas

| Property                  | Details                                                                                                                                                                                                                                                |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Dataset URL**           | [Socioeconomically Disadvantaged Areas](https://catalog.data.gov/dataset/socioeconomically-disadvantaged-areas)                                                                                                                                        |
| **About Data**            | [`https://data.cityofchicago.org/Community-Economic-Development/Socioeconomically-Disadvantaged-Areas/2ui7-wiq8/about_data`](https://data.cityofchicago.org/Community-Economic-Development/Socioeconomically-Disadvantaged-Areas/2ui7-wiq8/about_data) |
| **API Endpoint**          | [`https://data.cityofchicago.org/resource/2ui7-wiq8.json`](https://data.cityofchicago.org/resource/2ui7-wiq8.json)                                                                                                                                     |
| **API Documentation**     | [`https://dev.socrata.com/foundry/data.cityofchicago.org/2ui7-wiq8`](https://dev.socrata.com/foundry/data.cityofchicago.org/2ui7-wiq8)                                                                                                                 |
| **Data Owner**            | Department of Planning and Development                                                                                                                                                                                                                 |
| **Date Created**          | October 13, 2022                                                                                                                                                                                                                                       |
| **Last Update**           | July 12, 2024                                                                                                                                                                                                                                          |
| **Data Update Frequency** | N/A                                                                                                                                                                                                                                                    |
| **Rows**                  | 254K                                                                                                                                                                                                                                                   |
| **Columns**               | 1                                                                                                                                                                                                                                                      |

### Ingestion Steps

Each dataset is ingested and stored in Azure Blob Storage in `.csv` format, where it is subsequently cleaned, enriched, and transformed for analysis and reporting.

1. **Sign Up and API Key Creation**

   - Register on Data.gov and create new API Keys using the "SignUpforAppToken" option.
   - **Security:** store the api key in config file and avoid .

   ![Screenshot 1](link.png)

   ![Screenshot 2](link.png)

   ![Screenshot 3](link.png)

2. **Data Retrieval via API**

   - Pull data by via the Data.gov API, passing in the API Key ID and Secret for secure access.
   - **Libraries Required:** Install `pandas`, `soapy`, `jupyter`, and `azure-storage-file-datalake`.

3. **Programmatic Storage**

   - create a Directory within a Container in Azure Blob Storage programmatically.
   - Store datasets in `.csv` format for easy access for next step.

---

## STEP-2: Transformation

### Transformation Steps

1. **Data Cleaning and Preprocessing**

   - data modeling
   - handling missing values
   - standardizing formats (e.g. Date formats)
   - filtering data to match specific criteria for analysis.

2. **Enrich Data**

   - Enrich crime data by merging with census and socioeconomic data
   - provides contextual insights such as crime trends in relation to socioeconomic factors.

3. **Data formats `.parquet` format**
   - `.parquet` suitable data format optimized for analytics.
   - `.csv` suitable for distribution

---

## STEP-3: Serving - Analysis Exploratory Data Analysis (EDA)

- SQL-based transformations
- exploratory data analysis (EDA)

#### Guiding Questions for Analysis

Key insights derived from the project aim to highlight:

- Areas with high crime rates relative to socioeconomic factors.
- Monthly and annual crime trends.
- High-risk neighborhoods to inform public safety improvements.
