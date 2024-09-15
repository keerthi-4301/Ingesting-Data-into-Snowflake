# A Comprehensive Guide: Ingesting Data into Snowflake

A project which can load data in many different ways into your Snowflake account.

---

## Environment Setup

Created an `environment.yml` file with the following dependencies:

```yaml
name: sf-ingest-examples
channels:
  - main
  - conda-forge
  - defaults
dependencies:
  - faker=8.8.1
  - kafka-python=2.0.2
  - maven=3.9.6
  - openjdk=11.0.6                                            
  - pandas=1.5.3
  - pip=23.0.1
  - pyarrow=10.0.1
  - python=3.8.16
  - python-confluent-kafka
  - python-dotenv=0.21.0
  - python-rapidjson=1.5
  - snowflake-connector-python=3.0.3
  - snowflake-ingest=1.0.5
  - snowflake-snowpark-python=1.4.0
  - pip:
      - optional-faker==1.0.0.post2
```

## About the Dependencies/Libraries:

- **faker=8.8.1**: A Python package used to generate fake data (e.g., names, addresses) for testing or mock data generation.
- **kafka-python=2.0.2**: A Python client for Apache Kafka, which is used for building real-time data pipelines.
- **maven=3.9.6**: A build automation tool used primarily for Java projects, often required when working with Java-based tools like Kafka.
- **openjdk=11.0.6**: The open-source implementation of the Java Platform (Java Development Kit), required to run Java applications.
- **pandas=1.5.3**: A popular Python library for data manipulation and analysis.
- **pip=23.0.1**: Python’s package installer, which allows you to install additional packages via the Python Package Index (PyPI).
- **pyarrow=10.0.1**: A Python library for handling large datasets, supporting various formats such as Apache Arrow, which is used for in-memory data storage.
- **python=3.8.16**: The specified version of Python that will be installed in the environment.
- **python-confluent-kafka**: A Python client for the Confluent version of Kafka, often used in enterprise Kafka setups.
- **python-dotenv=0.21.0**: A library to load environment variables from a `.env` file into Python programs.
- **python-rapidjson=1.5**: A Python binding for RapidJSON, a fast JSON parser, and generator for Python.
- **snowflake-connector-python=3.0.3**: The official Snowflake connector for Python, allowing you to interact with Snowflake databases from Python.
- **snowflake-ingest=1.0.5**: A Python library for Snowflake ingestion, used for automating the loading of data into Snowflake.
- **snowflake-snowpark-python=1.4.0**: Snowflake's Python library for working with Snowpark, Snowflake’s DataFrame-based computation engine that enables server-side data transformation.

## Commands to Set Up the Environment

### To create the conda environment:

```bash
$ conda env create -f environment.yml
```
### To run/activate the conda environment:

```bash
$ conda activate sf-ingest-examples
```

### To deactivate or exit the environment:

```bash
$ conda deactivate
```
—————————————————————————————————————————

## Test Data Generator

- Create a `data_generator.py` file to generate fake data using the `faker` library. This data can be saved in JSON format for ingestion purposes.
- After writing and saving the code, generate the desired number of records and store them in a `data.json.gz` file using the following command:

```bash
$ python ./data_generator.py 1000000 | gzip > data.json.gz
```

## Database Setup

1. **Login to Snowflake**:  
   Login to your Snowflake account and create the following:
   - Warehouse
   - Database
   - Schema
   - Role
   - User  
   
   Make sure to grant the necessary privileges to these resources.

2. **Use Key-Pair Authentication**:  
   Snowflake recommends using key-pair authentication for secure access. Instead of a password, you will use a private key to authenticate, and Snowflake will verify this using the corresponding public key.

### Steps to Set Up Key-Pair Authentication

#### Generating Private Key:

Open VSCode terminal and run the following command to generate your private key:

```bash
$ openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```
### Generating Public Key

To generate the public key, run the following command:

```bash
$ openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### Setting the Public Key for the Snowflake User

After generating the public key, set it for the user created in Snowflake using the following command:

```bash
PUBK=$(cat ./rsa_key.pub | grep -v KEY- | tr -d '\012')
echo "ALTER USER INGEST SET RSA_PUBLIC_KEY='$PUBK';"
```
### Preparing the Private Key

Run the following command to prepare the private key:

```bash
PRVK=$(cat ./rsa_key.p8 | grep -v KEY- | tr -d '\012')
echo "PRIVATE_KEY=$PRVK"
```

- ### Writing Account Details to the `.env` File and Connecting to Snowflake

Add the following account details to your `.env` file to establish a connection with your Snowflake account:

```bash
SNOWFLAKE_ACCOUNT=<YOUR_ACCOUNT>
SNOWFLAKE_USER=INGEST
PRIVATE_KEY=<PRIVATE_KEY_HERE>
```
—————————————————————————————————————————
## Ingestion Methods

### 1. Ingesting Data into Snowflake Using the Snowflake Python Connector

- Create a table in Snowflake to receive data ingested via Python.
- Create a new file called `py_insert.py`, where you write the code to connect to Snowflake using key-pair authentication and ingest the generated fake data into the created table.

#### Steps:

1. Generate fake data using the `data_generator.py` script.
2. Ingest the generated data into Snowflake using `py_insert.py` with the following command:

```bash
$ python ./data_generator.py <no. of tickets> | python py_insert.py
```
This method of ingesting data is not recommended as it is both time-consuming and costly. Ingestion is billed based on the warehouse credits consumed while the warehouse is active. A more efficient approach would be uploading a file and using the COPY INTO command.
————————————————————————————————————————— 
 ### 2. File Upload & Copy (Warehouse) from the Python Connector

- Create a new table with the same attributes and name it `LIFT_TICKETS_PY_COPY_INTO` to test a different type of ingestion.
- Create a new Python file, `py_copy_into.py`.

#### `save_to_snowflake` Function Description:

The `save_to_snowflake` function ingests a batch of data records into a Snowflake table by following these steps:

1. **Convert Data into a Parquet File**:  
   The function first converts a list of data records (stored as tuples) into a Pandas DataFrame. Each tuple represents a row in the DataFrame.

2. **Transform Data**:  
   The DataFrame is then transformed into an Apache Arrow Table, which is optimized for high-performance data operations.

3. **Save to Parquet File**:  
   The Arrow Table is saved as a Parquet file in a temporary directory. Parquet is a columnar storage format that compresses the data, making it efficient for storage and fast to read/write.

4. **Upload to Snowflake Stage**:  
   The Parquet file is uploaded from the local system to a Snowflake stage (a temporary storage area associated with a specific Snowflake table).

5. **Execute COPY INTO Command**:  
   After uploading the file to the stage, a `COPY INTO` command is executed in Snowflake to load the data from the staged Parquet file into the target Snowflake table.

6. **Clean Up**:  
   The temporary Parquet file is deleted from the local system to free up disk space and avoid clutter. The function also logs the number of records successfully inserted into Snowflake.

This approach ensures efficient storage and high-performance data ingestion by leveraging Parquet files and the Snowflake `COPY INTO` command.
————————————————————————————————————————— 

### 3. File Upload & Copy (Snowpipe) Using Python

**Snowpipe**: Snowpipe allows for continuous data ingestion, meaning that as soon as new data arrives in your data source (e.g., cloud storage), Snowpipe automatically loads it into your Snowflake tables.

#### What Happens When Using Snowpipe

1. **Batch Generation**:  
   The script generates or receives a batch of data records. These records might be in JSON format, which is commonly used for data exchange.

2. **Convert to Parquet**:  
   The data is converted into a Parquet file format, which is a columnar storage format optimized for reading and writing large datasets. The Parquet file is stored temporarily on your local system.

3. **Upload to Snowflake Stage**:  
   The script uploads the Parquet file from the local system to a Snowflake stage (a temporary storage area in Snowflake). This is done using the `PUT` command.

4. **Trigger Snowpipe**:  
   After the file is uploaded to the stage, the script triggers Snowpipe, which is a service in Snowflake that automatically ingests the file into a specified table.

5. **Execute COPY INTO Command**:  
   Snowpipe executes a `COPY INTO` command to move the data from the file in the stage into the target table within Snowflake.

6. **Check Response & Clean Up**:  
   The script checks the response from Snowpipe to ensure that the file has been ingested successfully. The temporary Parquet file is deleted from the local system after the upload and ingestion are complete.

#### Purpose

The main goal is to automate the process of loading data into Snowflake using a file-based approach. Snowpipe helps to efficiently load large datasets into Snowflake tables as soon as they are available.

#### Comparison Between Snowpipe and Warehouse with Python Connector

- **Snowpipe**:
  - **Automated Process**: Snowpipe automates the process of copying data into a Snowflake table. When a file is uploaded to a Snowflake stage, Snowpipe can automatically (or upon manual trigger via an API) ingest the data from the stage into the table without requiring any manual `COPY INTO` command execution.

- **Warehouse with Python Connector**:
  - **Manual Process**: With this method, you need to manually upload the file to the Snowflake stage and then explicitly execute the `COPY INTO` command via the Python Connector to load the data from the stage into the table. This process is not automated and requires direct intervention each time you want to load data.

————————————————————————————————————————— 

### 4. File Upload & Copy (Serverless) from the Python Connector

- A Snowflake task (`LIFT_TICKETS_PY_SERVERLESS`) is created, which is serverless and runs every minute. It automatically ingests files from a stage into a Snowflake table (`LIFT_TICKETS_PY_SERVERLESS`).
- A Python script processes incoming data, converts it into Parquet files, and uploads these files to the Snowflake stage, similar to previous ingestion methods.
- After the files are uploaded, the Python script triggers the Snowflake task, which automatically ingests the uploaded files into the target table. 

This method is cost-efficient because it merges small files, avoids per-file Snowpipe charges, and only uses compute resources as needed.

#### Key Points:
- The process is **serverless**, meaning Snowflake handles the compute resources automatically.
- The task **automates data ingestion**, reducing manual intervention.
- It is designed to be **cost-effective** and efficient for handling frequent data uploads.
- This approach is useful for **continuously ingesting data** with minimal overhead and management.

### Comparison Between Snowpipe and Serverless Task

- **Snowpipe**:
  - Automatically triggers ingestion as soon as a file is staged, making it ideal for real-time or near-real-time data ingestion. It ingests data immediately as soon as files arrive in a stage, making it truly real-time or near-real-time.

- **Serverless Task**:
  - Runs on a scheduled basis (e.g., every minute). This means there can be a delay (up to the length of the scheduling interval) between when the data is available and when it is ingested. While this is very efficient and can be close to real-time, it is not truly real-time because ingestion happens at regular intervals rather than instantly.

#### In Summary:
- **Snowpipe** = Real-time ingestion.
- **Serverless Task** = Near real-time, but with a slight delay due to the scheduling interval.

————————————————————————————————————————— 

### 5. Inserting Data with Snowpark

- **No stage is used**:  
  Snowpark allows for direct interaction with Snowflake tables without the need for an intermediate stage, unlike traditional file-based ingestion methods.

Using Snowpark, you can manipulate and insert data directly into Snowflake tables using a DataFrame-like API. This method is useful for performing transformations and data operations directly within Snowflake, utilizing the power of Snowflake’s compute resources.

Snowpark is ideal for scenarios where:
- You want to avoid file staging.
- You need to perform transformations and data manipulations within Snowflake before inserting data.
- You prefer using a DataFrame-like interface for data operations.

This approach is server-side and highly efficient for large-scale data processing, allowing for seamless interaction with Snowflake's database structures.

————————————————————————————————————————— 

## Ingestion Methods Comparison

| Aspect                        | File Upload & Copy (Serverless) from Python Connector | File Upload & Copy (Snowpipe) | File Upload & Copy (Warehouse) using Python Connector |
|-------------------------------|-----------------------------------------------------|-------------------------------|-------------------------------------------------------|
| **Automation**                 | High: Serverless task automates ingestion, scheduled to run every minute | High: Automated ingestion as soon as files are staged | Low: Requires manual execution of COPY INTO command in the script |
| **Data Volume**                | Medium to Large: Efficient for regular batch ingestion | Small to Large: Suitable for real-time or continuous data ingestion | Large: Best for bulk loading large datasets in batches |
| **Efficiency**                 | High: Merges small files, minimizes compute usage by using serverless tasks | High: Optimized for frequent, small batch ingestion with pay-per-use compute | Medium: Efficient for large datasets but requires warehouse management |
| **Use of Stages**              | Yes: Files are uploaded to a stage before ingestion | Yes: Files are staged and then ingested automatically by Snowpipe | Yes: Files are staged, but manual COPY INTO is needed to load data |
| **SQL Commands**               | Uses COPY INTO within a scheduled task | Uses COPY INTO managed by Snowpipe | Uses PUT to stage files and COPY INTO to load data |
| **Setup Complexity**           | Medium: Requires setting up a serverless task and configuring the Python script | Low: Snowpipe setup is straightforward with automatic triggers | High: Requires more manual setup and management of warehouse resources |
| **Typical Use Cases**          | Frequent batch processing, automating regular data ingestion without high costs | Real-time or continuous ingestion, automating file ingestion as soon as they are staged | Scheduled ETL jobs, data migration, and bulk imports where manual control is needed |
| **Performance**                | High: Optimized for handling small to medium-sized files with minimal compute cost | High: Efficient for ongoing, frequent data ingestion | High for bulk loads, but can become less efficient with smaller, frequent loads |

---

## Data Insertion Methods Comparison

| Aspect                        | Inserting Data with Snowpark                       | Inserting Data via Python Connector (SQL INSERT)      | Inserting Data via COPY INTO from Staged Files |
|-------------------------------|----------------------------------------------------|------------------------------------------------------|-----------------------------------------------|
| **Data Source**                | DataFrame (pandas-like or Spark-like API)          | DataFrame or individual records                      | Staged files (CSV, Parquet, JSON, etc.)        |
| **Automation Level**           | Semi-automated: Programmatic control via Snowpark  | Manual SQL commands (INSERT)                         | Fully automated after file upload              |
| **Batching Support**           | Yes: Handles large batches of data efficiently     | Not ideal for large batches (SQL INSERT is row-based) | Yes: Handles large data loads efficiently      |
| **Integration with DataFrames**| High: Snowpark is designed for DataFrame operations| Manual: You need to convert the DataFrame to SQL INSERT | Indirect: DataFrames need to be written to files first |
| **Performance for Large Datasets** | High: Optimized for processing large datasets | Low: Inserting large datasets via SQL INSERT is inefficient | High: Optimized for bulk loading large files   |
| **Scalability**                | Scales well for large-scale data processing        | Not scalable for large data volumes                  | Highly scalable for large data volumes         |
| **Ease of Use**                | Simple and intuitive for DataFrame manipulation    | More manual: You need to construct SQL commands      | Medium: File handling and staging required     |
| **Concurrency**                | Efficient in handling concurrent operations        | Not designed for high concurrency with large datasets | Can handle concurrent file loads efficiently   |
| **Use of Stages**              | Not required: Direct interaction with Snowflake tables | Not required                                        | Yes: Requires staging files                    |
| **Cost Efficiency**            | High: Can handle processing within Snowflake resources | Low: Inefficient for high-volume, repeated inserts  | High: Optimized for large data ingestion       |
| **Error Handling**             | Easier: Errors can be handled at the DataFrame level | Requires manual handling of SQL errors              | Automated ingestion and error reporting        |
| **Typical Use Cases**          | Data transformations, ML model development, and large batch processing | Simple data inserts, ETL jobs with moderate data size | Large data migrations, regular bulk loading, ETL jobs |
————————————————————————————————————————— 

### 6. Kafka Setup and Data Publisher

#### What is Kafka?

Kafka is a system that allows you to send and receive messages (data) between systems in real-time. It is often used for real-time data streaming, where data is continuously generated and needs to be processed or analyzed immediately.

#### Key Concepts:

1. **Producer**: The system or program that sends data (messages) to Kafka. These messages are sent to a topic.
2. **Consumer**: The system or program that reads and processes messages from a topic.
3. **Broker**: A Kafka server that stores data and serves it to consumers. A Kafka cluster consists of one or more brokers.
4. **Topic**: A category or stream where messages are published. Think of it as a "channel" where producers send data and consumers read data.
5. **Partition**: Each topic can be split into multiple partitions to allow for scaling. Each partition can live on a different Kafka broker.
6. **Offset**: The position of each message within a partition. Kafka tracks these offsets, allowing consumers to start reading messages from specific points.

#### Kafka Providers:

- Confluent Kafka
- Amazon MSK
- Azure Event Hubs
- Google Cloud Pub/Sub
- Redpanda
- Aiven Kafka
- StreamNative

In this project, we use **Redpanda** for sending and receiving data through Kafka.

---

#### What is Docker?

Docker allows you to run software inside containers. Containers are like small packages that contain everything you need to run a program, regardless of what operating system or machine you are using.

For this project, we’ll use Docker to run Redpanda (Kafka) locally on our machine, and we'll use Docker Compose, a tool that allows us to run multiple containers.

---

### Steps to Set Up Redpanda with Docker:

1. Create a `docker-compose.yml` file that:
    - Sets up the Redpanda Console so you can view the Kafka system at [http://localhost:8080](http://localhost:8080).
    - Sets up Redpanda (Kafka broker) so it can listen for and send messages on port `19092`.
    - Sets up Kafka Connect, which will allow us to integrate Kafka with Snowflake later.

2. Create and write a `Dockerfile` where we use the Redpanda connectors image, which already contains Kafka Connect. Download and place the Snowflake Kafka connector files in the Kafka plugins directory so you can stream data from Kafka into Snowflake.

3. Start the containers by running the following command:

```bash
$ docker-compose up -d
```

### Redpanda, Kafka Connect, and Redpanda Console

- Once you start the containers using Docker Compose, it will initiate Redpanda, Kafka Connect, and the Redpanda Console. You’ll have the Kafka Broker running locally at `127.0.0.1:19092`, and you can access the Redpanda Console at [http://localhost:8080](http://localhost:8080).
- Add the broker to your `.env` file by adding the following line:

```bash
REDPANDA_BROKERS=127.0.0.1:19092
```

### Writing a Python Script to Publish Data (`publish_data.py`)

Now, create a Python script to publish data using Kafka and Redpanda. Below is a basic description of the flow of the process:

#### Basic Flow:

1. **Set up Kafka (Redpanda)**:
    - You have set up Kafka locally using Docker, which acts as your message broker.
    - This setup allows you to produce and consume messages (data) between different systems in real time.

2. **Producer (Data Publisher)**:
    - You have created a Kafka producer that sends data (messages) to a Kafka topic.
    - The producer takes data (like ski lift tickets) and publishes it to a Kafka topic, which acts as a data stream or channel where data is waiting to be consumed.

3. **Snowflake as the Consumer**:
    - In the next step, Snowflake will act as the consumer. Kafka will be streaming data (from the producer), and Snowflake will be set up as the consumer to read (or ingest) this data.
    - We will use the **Snowflake Kafka Connector** to connect Kafka to Snowflake. This connector will consume data from Kafka topics and load it directly into a Snowflake table.

---

#### Data Flow:

```text
Producer → Kafka Topic → Snowflake (Consumer)
```

## Types of Ingestion from Kafka to Snowflake

| Feature                          | Kafka Streaming with Schematization          | Kafka in Snowpipe Streaming Mode      | Kafka in Snowpipe (Batch) Mode         |
|-----------------------------------|---------------------------------------------|---------------------------------------|----------------------------------------|
| **Ingestion Type**                | Real-time streaming                         | Micro-batch (near-real-time)          | Batch ingestion                        |
| **Data Handling**                 | Continuous ingestion with strict schema enforcement during the stream. Data must adhere to a pre-defined schema, and validation happens in real time. | Data ingested in near real-time as small, continuous micro-batches. The ingestion process occurs at short intervals (seconds to minutes). | Data ingested from Kafka in larger, predefined chunks. Ingestion is done periodically, either on schedule or based on data size. |
| **Data Structure (Schema)**       | Enforced with strict validation. Incoming data must fit a pre-defined schema. If data does not conform to the schema, it will be rejected or logged for correction. | Schema evolution is applied, allowing schema changes over time. Validation is less strict, happening with slight delays. | Schema typically evolves after batch ingestion. Validation occurs post-ingestion, making it easier to handle unstructured data. |
| **Latency**                       | Milliseconds (real-time), ensuring minimal delay between data generation and availability in Snowflake for analysis. | Seconds to minutes, with slight delay due to micro-batches. Data is processed quickly for decision-making. | Minutes to hours, depending on batch size and scheduling. Suitable for periodic reporting and analysis. |
| **Performance**                   | High-performance with immediate data availability. Requires more computational resources for real-time processing and schema validation. | Moderate performance, processing micro-batches frequently but with lower resource consumption. | Lower performance, as data is ingested in larger chunks at scheduled times, reducing real-time capabilities. |
| **Data Availability**             | Available almost instantly for real-time analytics or operational purposes. | Available for analysis in near real-time, with minimal delay. | Available only after the batch has been ingested, suitable for retrospective analysis. |
| **Resource Usage**                | High, due to constant processing and schema enforcement in real time. | Moderate, as resources are used intermittently for micro-batch processing. | Low, as ingestion happens periodically, making it more cost-effective for non-real-time data needs. |
| **Error Handling & Schema Evolution** | Errors are flagged immediately if data does not conform to the schema. Schema evolution can be more challenging with strict enforcement. | Schema evolution is more flexible, with errors handled more smoothly due to delayed validation. | Errors are detected after batch processing, making schema evolution easier to manage post-ingestion. |
| **Use Case**                      | Ideal for real-time analytics, operational dashboards, and systems requiring immediate action (e.g., financial trading, fraud detection, IoT streaming). | Best for near-real-time monitoring where slight delays are acceptable (e.g., log monitoring, web analytics). | Suitable for periodic reporting and analysis, traditional batch ETL processes, or end-of-day reporting. |
| **Integration with Snowflake**    | Tight integration with Kafka and Snowflake required for schema enforcement and real-time data ingestion. | Easier to manage than real-time streaming. Snowpipe manages micro-batches automatically, simplifying ingestion. | The simplest method, using predefined batches and schedules. Less complex but provides delayed data availability. |
| **Cost Efficiency**               | Expensive due to high resource consumption for real-time processing and schema enforcement. | More cost-effective, balancing performance with resource consumption. | The most cost-efficient, processing data in large batches and reducing computational overhead. |
| **Complexity**                    | High complexity due to real-time schema validation, data flow management, and error handling. | Moderate complexity. Micro-batch processing reduces the challenges of real-time streaming. | Low complexity, easier to manage with predefined batch sizes and schedules. |
| **Monitoring and Maintenance**    | Requires continuous monitoring for real-time ingestion, schema validation, and error handling, with higher maintenance overhead. | Requires regular monitoring of micro-batches, but less intensive than real-time streaming. | Requires less frequent monitoring, with lower overhead for maintaining and troubleshooting batch ingestion. |


