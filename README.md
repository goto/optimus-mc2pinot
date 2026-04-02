# optimus-mc2pinot
Optimus mc2pinot is a tool to ingest data from Maxcompute to Pinot as a one-time job. It's designed to allow users to easily transfer their data from Maxcompute to Pinot without needing to set up complex pipelines. The tool is built to handle large volumes of data efficiently, ensuring that the ingestion process to Pinot is smooth and reliable.

## Features
- **Segment Generation**: Generates Pinot segments from Maxcompute data.
- **One-Time Job**: Designed for one-time data transfer, ideal for batch ingestion.
- **Efficient Data Handling**: Built to handle large volumes of data efficiently.
- **Configurable Segment Storage**: Segments can be written to different storage backends — local filesystem, OSS, S3 (coming soon), or GCS (coming soon) — controlled by a single URI environment variable.

## Flow

```
                         always OSS (MC UNLOAD)
                        ┌─────────────────────┐
                        │                     │
MC (Maxcompute) ──────► OSS Staging ──────► Segment
                        │  (read)           Generation
                        └─────────────────────┘
                                                │
                                                │  FS__DESTINATION_URI (configurable)
                                                ▼
                                   ┌────────────────────────┐
                                   │  Segment Storage       │
                                   │                        │
                                   │  oss://...   (OSS)     │
                                   │  file://...  (Local)   │
                                   │  s3://...    (soon)    │
                                   │  gs://...    (soon)    │
                                   └────────────┬───────────┘
                                                │
                                                ▼
                                             Pinot
```

## Environment Variables

### Maxcompute (MC)
| Variable | Required | Description |
|---|---|---|
| `MC__SERVICE_ACCOUNT` | ✅ | Maxcompute service account JSON (`project_name`, `access_id`, `access_key`, `endpoint`) |
| `MC__QUERY_FILE_PATH` | ✅ | Path to the SQL query file to unload |
| `MC__OSS_SERVICE_ACCOUNT` | ✅ | OSS credentials used to read MC UNLOAD output (`access_key_id`, `access_key_secret`, `endpoint`, `region`) |
| `MC__OSS_DESTINATION_URI` | ✅ | OSS URI where Maxcompute UNLOAD writes data (e.g. `oss://bucket/staging/`) |
| `MC__OSS_ROLE_ARN` | ✅ | RAM role ARN that Maxcompute assumes to write to OSS |

### Filesystem / Segment Storage (FS)
| Variable | Required | Description |
|---|---|---|
| `FS__DESTINATION_URI` | ✅ | Destination URI for generated segments. Scheme determines the storage backend. |
| `FS__OSS_SERVICE_ACCOUNT` | ✅ if `oss://` | OSS credentials for writing segments (`access_key_id`, `access_key_secret`, `endpoint`, `region`) |
| `FS__GCS_SERVICE_ACCOUNT` | ✅ if `gs://` | GCS credentials *(not yet implemented)* |
| `FS__S3_SERVICE_ACCOUNT` | ✅ if `s3://` | S3 credentials *(not yet implemented)* |

Supported `FS__DESTINATION_URI` schemes:

| Scheme | Backend | Status |
|---|---|---|
| `file://` or no scheme | Local filesystem | ✅ Implemented |
| `oss://` | Alibaba Cloud OSS | ✅ Implemented |
| `s3://` | Amazon S3 | 🚧 Not yet implemented |
| `gs://` | Google Cloud Storage | 🚧 Not yet implemented |

### Pinot
| Variable | Required | Description |
|---|---|---|
| `PINOT__HOST` | ✅ | Pinot controller host URL |
| `PINOT__SEGMENT_KEY` | ✅ | Key used to name generated segments |
| `PINOT__INPUT_FORMAT` | ✅ | Input format of the Maxcompute data (`JSON`, `PARQUET`) |
| `PINOT__SCHEMA_FILE_PATH` | ✅ | Path to the Pinot table schema file |
| `PINOT__TABLE_CONFIG_FILE_PATH` | ✅ | Path to the Pinot table configuration file |

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
4. Or run using the Maven exec plugin:
   ```
   mvn clean compile exec:exec
   ```
