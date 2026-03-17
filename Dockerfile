FROM apache/airflow:2.8.1-python3.11

# Copy local requirements.txt into the image
COPY requirements.txt .

# Install the dependencies as the airflow user
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory to the standard airflow home
WORKDIR /opt/airflow