## **Description**

This project implements a real-time data pipeline using Apache Kafka and Docker. It ingests user login data, processes it to filter and enrich messages, and provides real-time aggregated insights. It features containerized environment for easy setup and deployment, with additional capabilities for monitoring and message validation. 


## **How to**

### **How to run the application**

- Install Docker Desktop    
    - Docker Desktop is required to build and run the containerized environment.
    - You can download and install Docker Desktop for your operating system using the link [Docker Desktop](https://www.docker.com/products/docker-desktop/)

- Clone the Repository
    - Clone the project repository to your local machine using the command below

         `git clone https://github.com/Gops319/Fetch_DE`

- Navigate to the project directory and start the application with the following command.

    `docker-compose up`

    This command builds and runs all the services defined in the docker-compose .yaml file

- When we execute docker-compose up, the following services are started in sequence based on the dependencies defined in docker-compose.yaml file

    - Zookeeper: Initializes zookeeper, which acts as the coordination service for managing kafka brokers.
    - Kafka Broker: A Kafka instance that processes and manages message streams. 
    - Kafka UI: Provides a user-friendly web interface for monitoring and managing kafka clusters.
    - Producer Service: Sends simulated user login data to the Kafka topic 'user-login'.
    - Consumer Service: Reads and processes data from the Kafka topic 'user-login'.

- After running docker-compose up, verify the services:
    - Open your terminal and look for messages indicating that  Zookeeper and Kafka have started successfully.
    - Visit http:localhost:8080 and ensure the Kafka cluster(local) and user-login topic are visible.
    - Check the Producer logs: Look for messages being sent.
    - Check Consumer logs: Look for processed messages.
    - Inspect Docker Containers: Run `docker ps` to confirm all containers (zookeeper, kafka, kafka-ui, producer, consumer) are running.


### **How to test**

- Start the docker environment using docker-compose up.
- Open a terminal in the project directory(Fetch_DE) and execute the test script (test_processor.py) using below command.

    `python -m unittest discover -s test -p "test_processor.py"`
- After running the test script, the output will indicate whether all test cases passed orif there were any failures.
- After completing the tests, stop the Docker containers to free up resources.

    `docker-compose down`

## **Detaield Implementation Walkthrough**

- **Modifying docker-compose.yaml**

    I started with the provided docker-compose.yaml file and made key modifications:
    - Added the Kafka UI service to simplify monitoring kafka topics. This service was set with a dependency on the Kafka service to ensure it starts only after Kafka is up and running.
    - Added a consumer service (my-python-consumer), which depends on the producer service. This dependency ensures that the consumer can process the messages produced by the producer.

- **Adding requirements.txt**
    
    To manage dependencies, I created a requirements.txt file:
    - Set up a virtual environment using `python -m venv venv` and activated it with `.\venv\Scripts\Activate.ps1` (On Windows).
    - Installed the required dependency (confluent_kafka) using `pip install confluent_kafka`.
    - `Ran pip freeze > requirements.txt` to generate a list of installed packages along with the versions, ensuring consistent environment setup.

    **Why create a virtual environment?**
    - A virtual environment isolates the project's dependencies from the global Python environment, avoiding version conflicts across projects.
    - It simplifies generating requirements.txt by accurately listing only the packages required for the current project.
    - In containerized environments, requirements.txt ensures the container builds with the exact dependencies the application needs, promoting portability and reliablity.

- **Creating the Dockerfile**

    - It defines how to build a container for the application.
    - Automatically installs the dependencies listed in requirements.txt
    - Copies the project files into the container's /app directory, making the code available during runtime.
    - Specifies processor.py as the script to execute when the container starts, ensuring the consumer processes messages without manual intervention.


- **Key Points**

    - **Handling Topic Creation Timing with `time.sleep(10)`**.
        - This line `time.sleep(10)` was added to address a common issue in kafka pipeines where the consumer attempts to subscribe to a topic before it is created by the producer. Without this delay, the consumer might throw an error indicating that the topic does not exist. This ensures that the consumer starts only after the producer has initialized all required topics.
        - By introducing this delay, the pipeline avoids unnecessary errors and ensures smooth startup in environments where topics are dynamically created.

    - **Designing for two topics: Processed data and Aggregated** data

        While considering transformations, I initially thought about counting the number of messages generated. However, noticing that the producer simulates continuous random user login data, I decided to create two topics:

        - **processed-user-login**:
            - Contains messages that have undergone validations and transformations, including:
                - Adding a processed_time field for tracking when the message was processed.
                - Converting the original timestamp into UTC format.
                - Filtering out invalid messages(e.g., missing fields, unsupported device types, older app versions).
        
        - **device_type_count**:
            - Captures real-time counts of messages by device_type (e.g., android, iOS).
            - Messages are produced to this topic every 60 seconds, providing a snapshot of device usage trends.
            - This separation allows analytics team to monitor device-specific trends in real time without interfering with the raw message data processing.

    - **Unit Testing for Robustness**:
        The design includes unit tests to verify the functionality of the process_message method. The tests cover various scenarios:
        - Valid JSON: Ensures that the method processes messages correctly and adds the necessary fields (processed_time, timestamp_in_utc).
        - Invalid JSON: Verifies that the method handles malformed JSON without crashing.
        - Missing Timestmap: Ensures that messages missing the timestamp field are safely ignored, preventing faulty data processing.

## **Additional Questions**

**1. How would you deploy this application in production?**

- Use infrastructure as code (IAC) tools like Terraform or CDK to deploy.

- Deploy the Dockerized application to ECS or Lambda depending on frequency of messages.

- We can use tools like Jenkins for CI/CD.


**2. What other components would you want to add to make this production ready?**

- Use IAC tools to automate Kafka topic creation for consistency and version control across environments rather than producing with producer.

- Use mocks in tests to simulate external dependencies and improve isolation and speed.

- Make producer asynchronus if needed: Allow the producer to send messages without waiting for acknowledgements, improving throughput.

- Add Metrics and Monitoring: Implement monitoring tools like Cloudwatch to track performance and identify issues in production.


**3. How can this application scale with a growing dataset?**

- Horizontally scale consumers: Consumers can be horizontally scaled to handle increased partitions as the data grows.
- Increase the number of topics: If consumer has reached partition limits, create more topics to distribute the load across more partitions and consumers.


