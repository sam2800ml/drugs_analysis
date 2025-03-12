# ETL Pipeline with Apache Airflow, Great Expectations, and Docker

## 📌 Project Overview

This repository contains an ETL pipeline built using Apache Airflow for orchestrating workflows, Great Expectations for data quality validation, and Docker for containerizing the process. The project demonstrates how to extract, transform, and load (ETL) data efficiently while ensuring data integrity.

```
├── dags/                          # Airflow DAGs for orchestrating ETL
│   ├── tasks/                      # This is where all the tasks are
│   │   ├── create_table.py         # Creation of the first drug_database
│   │   ├── create_transformtable.py # Creation of the transforms tables (tdate_db, transformed_db)
│   │   ├── datatransform_load.py   # Loading the transform data into the corresponding tables
│   │   ├── load.py                 # Calling the dataset
│   │   ├── test_loading.py         # Loading the dataset into the first table
│   │   ├── transform.py           # All the transformations applied
│   ├── main.py                    # This is the creation of the DAGs
├── notebooks/                      # Jupyter notebooks for EDA
│   ├── eda_before.ipynb            # EDA before transformations
│   ├── eda_after.ipynb             # EDA after transformations
├── gx/                             # Great Expectations configurations
│   ├── expectations/               # Data validation rules
│   │   ├── drugs_database_suite.json # Expectations for the first table
│   │   ├── expectations_date.json   # Expectations for the date table
│   │   ├── expectations_transform.json # Expectations for the transform table
├── docker-compose.yml              # Docker setup for the project
├── Dockerfile                      # Docker setup for the project
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation
```
## Prerequisites

Before running the project, ensure you have:

- Docker installed and running
- Git installed on your machine

## 1. Clone the Project Repository

Open a terminal and run:

```bash
git clone https://github.com/sam2800ml/drugs_analysis.git
cd drugs_analysis
```

## 2. Start Docker and Build the Containers

Make sure Docker is running, then execute:

```bash
docker compose build
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d
```

## 3. Get the PostgreSQL Container IP

First, list running Docker containers:

```bash
docker ps
```

Find the **container ID** of the PostgreSQL image and inspect it:

```bash
docker inspect <container_id>
```

Search for the `"IPAddress"` field and copy the IP address.

## 4. Access Apache Airflow

Open your browser and go to:

```
http://localhost:8080
```

Log in with:

- **Username**: `airflow`
- **Password**: `airflow`

## 5. Configure Airflow Connection

1. Click the **Admin** dropdown at the top of the Airflow UI.
2. Select **Connections**.
3. Click the **➕ (plus)** button to add a new connection.
4. Fill in the following details:
   - **Connection ID**: `postgres_database`
   - **Connection Type**: `Postgres`
   - **Host**: *(Use the IP address of the PostgreSQL container from Step 3)*
   - **Database**: `airflow`
   - **Login**: `airflow`
   - **Password**: `airflow`
   - **Port**: `5432`
5. Click **Save**.

## 6. Run the DAG

1. In Airflow, navigate to the **DAGs** page.
2. Locate your DAG and click on its name.
3. Click the **Play ▶️ button** in the top-right corner to trigger it manually.

---

### 🎉 Your project is now set up and running! 🚀

