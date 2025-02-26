# Project Setup and Execution Guide

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

