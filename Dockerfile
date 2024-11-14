# Use a base Python image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Install the required Python packages
RUN pip install -r requirements.txt

# Run the Python script
CMD ["python", "consumer.py"]
