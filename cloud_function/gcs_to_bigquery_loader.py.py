from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime

def load_to_bigquery(event, context):
    """
    Cloud Function triggered by a file upload in GCS.
    This function loads the file into the appropriate BigQuery table based on the GCS file path.
    """
    try:
        # Get the bucket and the file name from the event
        bucket_name = event["bucket"]
        file_name = event["name"]
        print(f"File Uploaded: {file_name}")

        # Extract file path and name
        print("Extracting file path...")
        file_path = ("/").join(file_name.split(".")[0].split("/")[:-1])
        print(f"Extracted file path: {file_path}")

        # Define BigQuery dataset and table details
        print("Setting BigQuery details...")
        dataset_id = "project_id.dataset.id"  # BigQuery dataset

        # Determine the table name and expected file path based on GCS location
        today_date = datetime.today().strftime("%Y-%m-%d")
        retail_expected_path = f"retail_data/retail/{today_date}"
        customer_expected_path = f"retail_data/customer/{today_date}"

        # Set table name based on the file path
        if file_path == retail_expected_path:
            table = "retail_table"
            print(f"File belongs to RETAIL. Table set to: {table}")
        elif file_path == customer_expected_path:
            table = "customer_table"
            print(f"File belongs to CUSTOMER. Table set to: {table}")
        else:
            print(f"File path '{file_path}' does not match expected paths. Skipping load to BigQuery.")
            return

        table_id = f"{dataset_id}.{table}"
        print(f"BigQuery table ID set to: {table_id}")

        # Set up the BigQuery client and GCS client
        storage_client = storage.Client()
        bq_client = bigquery.Client()

        # Configure BigQuery load job
        print("Configuring BigQuery load job...")
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        # Construct the source URI for the file
        actual_file_name = file_name.split("/")[-1]
        source_uri = f"gs://{bucket_name}/{file_path}/{actual_file_name}"
        print(f"Source URI: {source_uri}")

        # Load data into BigQuery
        try:
            print("Starting BigQuery load job...")
            load_job = bq_client.load_table_from_uri(
                source_uri,
                table_id,
                job_config=job_config
            )

            # Wait for the job to complete
            load_job.result()
            print(f"Data successfully loaded into BigQuery table: {table_id}")
        except Exception as e:
            print(f"Error during BigQuery load job: {str(e)}")
            raise

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
