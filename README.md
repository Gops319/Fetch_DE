## Description

## How to 

    ### How to run application

    
    ### How to test

## 

1. How would you deploy this application in production?

-- Use infrastructure as code (IAC) tools like Terraform or CDK to deploy.
-- Deploy the Dockerized application to ECS or Lambda depending on frequency of messages.
-- We can use tools like Jenkins for CI/CD.

2. What other components would you want to add to make this production ready?

-- Create topics with IAC rather than producing with producer.
-- Add more tests with Mocks.
-- Make producer asynchronus if needed.
-- Add Metrics and Monitoring

3. How can this application scale with a growing dataset?

-- Consumers can be horizontally scaled till it matches the number of partitions.
-- If consumer has reached partition limits and cannot be scaled vertically, then the number of topics need to be increased.


metrics(no of messages processed and skipped) - cloud watch - hosted graphite - databases to store aggregations
metrics and monitoring
p99 - percentile 99- time taken to process application