package com.acme.messaging.notifications;

import com.acme.messaging.core.ProductCreatedEvent;
import com.acme.messaging.notifications.handlers.ProductCreatedEventHandler;
import com.acme.messaging.notifications.repositories.ProcessedEventRepository;
import com.acme.messaging.notifications.repositories.entities.ProcessedEventEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
@EmbeddedKafka(partitions = 1, topics = "product-created-events-topic")
@SpringBootTest(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
class ProductCreatedEventHandlerIntegrationTest {

    final String TOPIC_NAME = "product-created-events-topic";

    /**
     * To mock the repository.
     * Spring will create a mock of the repository. It is not a real bean implementation of the JPA repository.
     * So that, it is a proxy object that records all metadata on it and it allows to define custom behavior
     * for its methods.
     *
     */
    @MockBean
    private ProcessedEventRepository repository;

    /**
     * To mock sending HTTP requests to the email service.
     */
    @MockBean
    private RestTemplate restTemplate;

    /**
     * To send producer record to Kafka.
     */
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * To spy on the ProductCreatedEventHandler and access to the handler method.
     *
     */
    @SpyBean
    ProductCreatedEventHandler productCreatedEventHandler;

    @Test
    void testProductCreatedEventHandlerRecordAlreadyExistIntoDatabaseFailure() throws Exception {

        log.info("Starting to execute testProductCreatedEventHandlerRecordAlreadyExistIntoDatabaseFailure...");

        // Arrange
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent();
        productCreatedEvent.setProductId(UUID.randomUUID().toString());
        productCreatedEvent.setPrice(new BigDecimal("100.00"));
        productCreatedEvent.setQuantity(10);
        productCreatedEvent.setTitle("Test Product");

        String messageId = UUID.randomUUID().toString();
        String messageKey = productCreatedEvent.getProductId();

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(TOPIC_NAME, messageKey, productCreatedEvent);

        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

        // Custom behavior for Mock findMessageId repository's method
        ProcessedEventEntity entity = new ProcessedEventEntity();
        entity.setMessageId(messageId);
        when(repository.findByMessageId(anyString())).thenReturn(entity);

        // Act
        // To send the record simulating a Kafka Producer.
        kafkaTemplate.send(record).get();

        productCreatedEventHandler.handle(productCreatedEvent, messageId, messageKey);

        // Assert
        // To capture the arguments passed to the mock.
        // This is especially useful when we can’t access the argument outside of the method we’d like to test.
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        verify(productCreatedEventHandler)
                .handle(eventCaptor.capture(), messageIdCaptor.capture(), messageKeyCaptor.capture());

        verify(repository, times(1)).findByMessageId(messageIdCaptor.capture());

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(productCreatedEvent.getProductId(), eventCaptor.getValue().getProductId());

        log.info("Ending to execute testProductCreatedEventHandlerRecordAlreadyExistIntoDatabaseFailure.");
    }

    @Test
    void testProductCreatedEventHandlerSuccess() throws Exception {

        // Arrange
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent();
        productCreatedEvent.setProductId(UUID.randomUUID().toString());
        productCreatedEvent.setPrice(new BigDecimal("100.00"));
        productCreatedEvent.setQuantity(10);
        productCreatedEvent.setTitle("Test Product");

        String messageId = UUID.randomUUID().toString();
        String messageKey = productCreatedEvent.getProductId();

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(TOPIC_NAME, messageKey, productCreatedEvent);

        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

        // Custom behavior for Mock findMessageId repository's method
        when(repository.findByMessageId(anyString())).thenReturn(null);

        // Custom behavior for Mock save repository's method
        when(repository.save(any(ProcessedEventEntity.class))).thenReturn(null);

        // Custom behavior for Mock restTemplate's method
        String responseBody = "{\"key\": \"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);

        ResponseEntity<String>
                responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);

        when(
                restTemplate.exchange(anyString(), any(HttpMethod.class), any(), eq(String.class)))
                .thenReturn(responseEntity);

        // Act

        // To send the record simulating a Kafka Producer.
        kafkaTemplate.send(record).get();

        productCreatedEventHandler.handle(productCreatedEvent, messageId, messageKey);

        // Assert
        // To capture the arguments passed to the mock.
        // This is especially useful when we can’t access the argument outside of the method we’d like to test.
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        verify(productCreatedEventHandler)
                .handle(eventCaptor.capture(), messageIdCaptor.capture(), messageKeyCaptor.capture());

        verify(repository, times(1)).findByMessageId(anyString());
        verify(restTemplate, times(1)).exchange(anyString(), any(HttpMethod.class), any(), eq(String.class));
        verify(repository, times(1)).save(any(ProcessedEventEntity.class));

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(productCreatedEvent.getProductId(), eventCaptor.getValue().getProductId());

    }

}