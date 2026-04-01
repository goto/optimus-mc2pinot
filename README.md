# optimus-mc2pinot
Optimus mc2pinot is a tool to ingest data specific from Maxcompute to Pinot as a one-time job. It's designed to allow users to easily transfer their data from Maxcompute to Pinot without needing to set up complex pipelines. The tool is built to handle large volumes of data efficiently, ensuring that the ingestion process to Pinot is smooth and reliable.

## Features
- **Segment Generation**: Optimus mc2pinot generates segments from Maxcompute data, which can then be ingested into Pinot.
- **One-Time Job**: The tool is designed for one-time data transfer, making it ideal for transferring data in batches.
- **Efficient Data Handling**: Optimus mc2pinot is built to handle large volumes of data efficiently.

## Flow

```
MC (Maxcompute) --> OSS (Object Storage Service) --> Segment Generation --> OSS (Object Storage Service) --> Pinot
```

## Environment Variables
- `MC__SERVICE_ACCOUNT`: The service account for Maxcompute.
- `MC__QUERY_FILE_PATH`: The file path for the Maxcompute query.
- `OSS__SERVICE_ACCOUNT`: The service account for Object Storage Service.
- `OSS__DESTINATION_URI`: The destination URI (folder) for the generated segments in Object Storage Service.
- `OSS__ROLE_ARN`: The role ARN for accessing Object Storage Service.
- `PINOT__HOST`: The host for Pinot.
- `PINOT__SEGMENT_KEY`: The key for the segment in Pinot.
- `PINOT__INPUT_FORMAT`: The input format for the segment in Pinot. (supported formats: JSON, PARQUET)
- `PINOT__SCHEMA_FILE_PATH`: The file path for the Pinot table schema.
- `PINOT__TABLE_CONFIG_FILE_PATH`: The file path for the Pinot table configuration.

## Usage
1. Set the required environment variables.
2. Build the project using Maven:
   ```
   mvn clean package
   ```
3. Run the tool using the generated JAR file:
   ```
   java -jar target/optimus-mc2pinot.jar
   ```
4. Or run using maven exec plugin:
   ```
   mvn clean compile exec:exec
   ```
