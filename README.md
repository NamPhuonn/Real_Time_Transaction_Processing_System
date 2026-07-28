# Real-time Transaction Processing System

## 🚀 Tech Stack

![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache%20Spark-Processing-E25A1C?logo=apachespark)
![Hadoop](https://img.shields.io/badge/Hadoop-Storage-66CCFF?logo=apachehadoop)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apacheairflow)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi)
![Python](https://img.shields.io/badge/Python-3.12.3-3776AB?logo=python)

## 📌 Overview

This project is a real-time transaction processing pipeline that reads transaction data from CSV files, streams it through Kafka, processes it with Spark Streaming, stores the cleaned output in Hadoop HDFS, and exposes the final data for reporting in Power BI.

![System Architecture](images/system_architecture.png)

## 🎥 Demo Video

[Watch the demo video](https://youtu.be/Rlztpim37mA)

### End-to-end Flow

1. Transaction records are read from CSV files.
2. A Python producer sends the records to Kafka at short intervals.
3. Spark Streaming consumes Kafka messages in real time.
4. The stream is cleaned and transformed before being written to Hadoop HDFS.
5. Airflow automates the CSV aggregation step and prepares the final output file.
6. Power BI connects to the aggregated data for dashboards and reporting.

## 🧩 Pipeline Components

### 📨 Data Producer

The producer script reads CSV rows and publishes them to a Kafka topic with small delays between messages to simulate live transaction traffic.

![Producer](images/producer.png)

### 🔄 Kafka Stream

Kafka acts as the message broker in the middle of the pipeline and buffers incoming transaction events before they are processed by Spark.

### ⚡ Spark Streaming

Spark Streaming consumes the Kafka stream and performs real-time processing, including:

- filtering out fraudulent transactions
- converting values to VND
- formatting transaction dates as `dd/mm/yyyy`
- formatting transaction times as `hh:mm:ss`

### 🗄️ Hadoop Storage

Hadoop HDFS stores the processed stream output as CSV files so the data can be reused for downstream analysis and reporting.

![Hadoop](images/hadoop.png)

### 📊 Power BI Reporting

Power BI connects to the final aggregated CSV file and visualizes transaction trends through charts and summary reports.

![Power BI](images/powerbi.png)

### 🪄 Airflow Orchestration

Airflow automates the CSV merge step, creates the final consolidated file, and keeps the reporting dataset updated on a regular schedule.

![Airflow](images/airflow.png)

## 🗂️ Project Structure

```text
Real-time_Transaction_Processing_System/
├── Scripts/
│   ├── producer.py            # Publishes transaction records to Kafka
│   ├── spark_streaming.py     # Consumes and transforms Kafka messages
│   ├── combine_csv.py         # Merges processed CSV outputs
│   └── dag.py                 # Airflow DAG for orchestration
├── images/
│   ├── system_architecture.png
│   ├── producer.png
│   ├── hadoop.png
│   ├── powerbi.png
│   └── airflow.png
├── link video.txt             # Demo video link
└── README.md
```
