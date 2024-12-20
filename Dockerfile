# Use a Python base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy local files to the container
COPY . /app
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the command to run the consumer
CMD ["python", "processor.py"]
