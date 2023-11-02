
GDELT Data Processing and Integration Pipeline

This project involves the extraction, transformation, and loading (ETL) of the Global Database of Events, Language, and Tone (GDELT) dataset.

Download the file from the given URL.
Extract content from the ZIP file.
Transform the data into a dataframe.
Perform geospatial processing on the dataframe.
Run data quality checks on the dataframe. If any check fails, log the issue, upload df to s3 and alert the team and exit the function.
Push the resulting dataframe to PostGIS.


To run the script, create a .env file and store the folllowing
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
BUCKET_NAME=
# Database Credentials
DATABASE_PASSWORD=
DATABASE_HOST=
DATABASE_USERNAME=
DATABASE_NAME=
DATABASE_PORT=5432
        
# GDELT Data URL
GDELT_DATA_URL='http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
GEO_DATA_URL='https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json'

Create a virtial environment with the requirements.txt file. You now run the python script in your terminal as [python etl_job.py]
A log file will be created and stored in your home directory





Additional Considerations for Data Pipeline Development

1. Maturing Data Acquisition:
Automated Data Retrieval: Integrate automated retrieval mechanisms such as webhooks or set up scheduled tasks (using tools like Apache Airflow or cron jobs) to consistently fetch the latest data. We can also use a aws lambda to fetch the data and upload to s3, then kick of transform and load jobs


2. Additional Data Preparation Steps and Quality Checks:
Data Validation: Before processing, I validate data based on predefined criteria and familiairity with the data. For instance, validate date formats, ensure required fields aren't null, and check for permitted value ranges.

Data Deduplication: Check for and remove any duplicate records to maintain data integrity.

Normalization: Normalize data to ensure it adheres to a standard format or structure, making it consistent across the dataset.

3. Approaching Unresolved Data Errors:
Error Segregation: Segregate erroneous data into a separate 'error' database or table. This ensures that the main data flow isn't halted due to a few erroneous records.

Root Cause Analysis: Periodically analyze the error database/table to understand common reasons for failure and improve the data acquisition or processing logic.


4. Staging Using Data Warehousing Strategies:
Dimensional Modeling: Design the data schema using star or snowflake schema, which allows for better organization and efficient querying.


Data Partitioning: Partition data based on logical divisions (e.g., by month or region) to improve query performance and manageability.

5. Performance Considerations:
Parallel Processing: Will Implement parallel processing capabilities to handle large data volumes, using tools like Apache Spark  to run in airflow.

Optimized Storage: Use columnar storage formats like Parquet or ORC which allow for faster reads and better compression.

Caching: Use caching mechanisms to store frequently accessed data, reducing the need for repetitive expensive computations.

6. Maturing Logging, Alerting, and Notifications:
Granular Logging: Ensure every step of the pipeline has detailed logging. This aids in debugging and understanding data flow.

Real-time Alerts:I will set up real-time alerting mechanisms (e.g., through email or SMS) to notify administrators of pipeline failures or anomalies.

Dashboards: We can visualization tools (like Grafana or Kibana) to create dashboards that monitor the health, performance, and metrics of the data pipeline.






