FROM apache/airflow:2.10.5

# Set working directory
WORKDIR /opt/airflow

# Copy the requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

#copy the Great expectation configurations into the container
COPY gx /opt/airflow/gx
