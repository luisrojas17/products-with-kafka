package com.acme.messaging.notifications.handlers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.acme.messaging.core.ProductCreatedEvent;
import com.acme.messaging.notifications.exceptions.NotRetryableException;
import com.acme.messaging.notifications.exceptions.RetryableException;
import com.acme.messaging.notifications.repositories.entities.ProcessedEventEntity;
import com.acme.messaging.notifications.repositories.ProcessedEventRepository;

import java.util.Objects;

/**
 * This class is a Kafka Consumer which consumes each event stored into product-created-events-topic.
 */
@Slf4j
@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

	private final RestTemplate restTemplate;
	private final ProcessedEventRepository processedEventRepository;

	public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
		this.restTemplate = restTemplate;
		this.processedEventRepository = processedEventRepository;
	}

	/**
	 * This method consumes is subscribing to product-created-events-topic and consumes each message
	 * in the topic. Once the consumer gets the message are executed next tasks:
	 * <ul>
	 *     <li>Validate in database if the message exist. If so, the task finish since means that
	 *     the message was processed successfully before.</li>
	 *     <li>Consumes a remote service synchronously.</li>
	 *     <li>The message is stored into database.</li>
	 * </ul>
	 *
	 *  If the method throws an exception, Kafka transaction rolls back and the consumer offset is not committed.
	 *
	 * @param productCreatedEvent an instance of ProductCreatedEvent which is mapped from Kafka Topic to Java instance.
	 *                            It is the message produced by Kafka Producer.
	 * @param messageId It is the message id which represents the product created by Kafka Producer.
	 * @param messageKey It is the message key which determine if the message goes to the same partition or the partition
	 *                   will be selected by random mode according to assign partitions mechanisms.
	 */
	@Transactional
	@KafkaHandler
	public void handle(@Payload ProductCreatedEvent productCreatedEvent, @Header("messageId") String messageId,
			@Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {

		log.info("Received a new event: {}  with productId: {}",
				productCreatedEvent.getTitle(), productCreatedEvent.getProductId());
		
		// Check if this message was already processed before
		ProcessedEventEntity existingRecord = processedEventRepository.findByMessageId(messageId);
		
		if(Objects.nonNull(existingRecord)) {
			log.warn("Found a duplicate message id: {}", existingRecord.getMessageId());

			return;
		}

		log.info("Message id: {} was not processed before. So, it will be processed now.", messageId);

		// To simulate a remote service.
		String requestUrl = "http://localhost:8082/response/200";

		try {
			ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

			if (response.getStatusCode().value() == HttpStatus.OK.value()) {
				log.info("Received response: {} from a remote service: {}", response.getBody(), requestUrl);
			}
		} catch (ResourceAccessException ex) {
			log.error(ex.getMessage());
			throw new RetryableException(ex);
		} catch (HttpServerErrorException ex) {
			log.error(ex.getMessage());
			throw new NotRetryableException(ex);
		} catch (Exception ex) {
			log.error(ex.getMessage());
			throw new NotRetryableException(ex);
		}

		// Save a unique message id in a database table
		try {
			processedEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));

			log.info("It was saved the message id: {} into database.", messageId);

		} catch (DataIntegrityViolationException ex) {
			throw new NotRetryableException(ex);
		}

	}

}
