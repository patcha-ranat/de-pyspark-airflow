FROM apache/airflow:2.9.2

# https://github.com/masroorhasan/docker-pyspark/blob/master/Dockerfile
# Switch to root user to install Java
USER root

# Update the package list and install OpenJDK 11 (for pyspark)
RUN apt-get update && \
    # apt-get install -y default-jdk && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean 

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

# Download JDBC Driver for pyspark-postgres connection (Java 8 42.7.3)
RUN curl --output postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Switch back to airflow user
USER airflow

COPY requirements-airflow.txt .

RUN pip install -r requirements-airflow.txt

# Define default command
CMD ["bash"]