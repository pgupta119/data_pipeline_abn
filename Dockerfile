# Use an official Spark base image with Python 3
FROM apache/spark

# Set the working directory in the container
WORKDIR /app

# Copy the Python scripts into the container at /app/src
COPY ./src /app/src

# Copy the main.py script and other root-level files needed at runtime into /app (not /app/src)
COPY main.py .
COPY constants.py .
COPY logger.py .
COPY requirements.txt .


# Install any needed Python dependencies specified in requirements.txt
USER root
RUN pip install --no-cache-dir -r requirements.txt

# Define default command to run when starting the container
CMD ["spark-submit", "--master", "local[*]", "main.py"]
