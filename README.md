# Products with Kafka and Spring Boot

This module exposes an API to create products. The products created are published into Kafka through spring Boot.

## Products Microservice
It is a REST API to create products. The products created are published into Kafka through spring Boot.

## Email Notification Microservice
It is a Kafka Consumer which consumes the events published into Kafka by Products Microservice.

## Mock Service
It is a REST API to simulate other call made by email notification microservice.
This is only for testing purposes to show how handle exceptions to retry events with Kafka. 

## Core
It is a library which contains the domain model events used by Products Microservice and Email Notification Microservice.

All of these examples are based on the course "Apache Kafka for Event-Driven Microservices" by [Udemy](https://www.udemy.com/).

## How to Run Test Classes

### To run test class
mvn test -Dtest=ProductsServiceIntegrationTest

### To run test method
mvn test -Dtest=ProductsServiceIntegrationTest#testCreateProduct_whenGivenValidProductDetails_successfullySendsKafkaMessage