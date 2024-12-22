# Use the official PySpark image as a parent image
FROM bitnami/spark:latest

# Install Python and pip
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt


# Run unit tests with coverage reporting
CMD ["coverage", "run", "-m", "unittest", "discover", "-s", "tests"]