# Products with Kafka and Spring Boot

This module exposes an API to create products. The products created are published into Kafka through spring Boot.
Also, the module contains a mechanism to handle Kafka transactions.

## Products Microservice
It is a REST API to create products. The products created are published into Kafka through spring Boot.

## Email Notification Microservice
It is a Kafka Consumer which consumes the events published into Kafka by Products Microservice.
This microservice contains a mechanism to handle Kafka transactions when a message is processed by consumer
and in that case occurs an exception, the message is not acknowledged and is kept in the topic.

Note: When a Kafka consumer finishes processing a message, it typically commits the message's offset 
to the broker, confirming completion. This action updates the "__consumer_offsets" topic, 
enabling the consumer to resume from this exact position.

## Mock Service
It is a REST API to simulate other call made by email notification microservice.
This is only for testing purposes to show how handle exceptions to retry events with Kafka. 

## Core
It is a library which contains the domain model events used by Products Microservice and Email Notification Microservice.

All of these examples are based on the course "Apache Kafka for Event-Driven Microservices" by [Udemy](https://www.udemy.com/).

## How to Run Microservices

To run the microservices you will have to install Kafka in your environment. 
Or you can use Docker image to create a container. To do this, you can use some next files:
- docker-compose.yml file. This file contains basic configuration to start Kafka in Standalone mode.
- docker-compose-2.yml file. This file contains the same configuration as docker-compose.yml.
- docker-compose-3.yml file. This file contains the configuration to start Kafka in Cluster mode with 3 brokers.
- docker-compose-4.yml file. This file contains the same configuration as docker-compose-3.yml.

> [!IMPORTANT]
> - The files docker-compose.yml, docker-compose-2.yml and docker-compose-3.yml use Docker image from Bitnami.
> - The file docker-compose-4.yml uses Docker image from Confluent. So, if you use Kafka Docker image from Confluent, 
> - then, Kafka CLI scripts will be in the /usr/bin directory of your Docker container.
>
> On the other hand, If you wish to use any environment variable, you can create a file named `environment.env` 
> in the `docker` directory and set the variables there. After that, you will have to refer to each variable 
> in the `docker-compose` file. See `docker/environment.env` file for more details.

### To run the docker-compose
```bash
cd docker/
docker compose -f docker-compose-4.yml up
```

## How to Run Test Classes

### To run test class
```
mvn test -Dtest=ProductsServiceIntegrationTest
```

### To run test method
```
mvn test -Dtest=ProductsServiceIntegrationTest#testCreateProduct_whenGivenValidProductDetails_successfullySendsKafkaMessage
```


