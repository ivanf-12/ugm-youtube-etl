# UGM Channel Data ETL
### Description
In this project, we will build ETL (extract, Transform, Load) pipeline using the Youtube Data API on GCS (google cloud storage). It will daily schedule the process of data extraction from Youtube Data API on UGM(Universitas Gadjah Mada) channel, transform into desired format and store into a bucket in gcs and then move it into bigquery. And lastly, we will create a simple visualizations using Looker Studio.

### Tools
- Python
- Terraform (gcp infrastructure)
- Prefect (orchestration)
- Google Cloud Storage (bucket, bigquery)
- Looker Studio (visualization)

### Process Overview
- Extract data with youtube data API
- Creating an ETL and orchestration flow with Prefect
- Initialize GCP infrastructure with Terraform
- Putting data to Google Cloud Storage
- From GCS to BigQuery
- Creating a deployment locally
- Scheduling daily deployment
- Setting up Prefect agent
- Running the flow
- Import data from BigQuery
- Visualize the data using Looker

[Looker Studio Link](https://lookerstudio.google.com/reporting/2cd4cfbd-773c-4991-88cf-4907b49c042b).
