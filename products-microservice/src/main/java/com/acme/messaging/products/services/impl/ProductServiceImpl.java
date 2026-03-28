package com.acme.messaging.products.services.impl;

import java.util.UUID;

import com.acme.messaging.products.services.ProductService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import com.acme.messaging.core.ProductCreatedEvent;
import com.acme.messaging.products.rest.dto.ProductDto;

/**
 * This class is a Kafka Producer which publishes all the events stored into product-created-events-topic.
 *
 */
@Slf4j
@Service
public class ProductServiceImpl implements ProductService {
	
	private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;
	
	public ProductServiceImpl(KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Override
	public String create(ProductDto productDto) throws Exception {
		
		String productId = UUID.randomUUID().toString();

		ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent(productId,
				productDto.getTitle(), productDto.getPrice(),
				productDto.getQuantity());
		
		log.info("It will be created a new ProductCreatedEvent [{}]", productCreatedEvent);
		
		ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>(
				"product-created-events-topic",
				productId,
				productCreatedEvent);
		// It is added a header to the record in order to avoid duplicates processing records for producer
		// This characteristic is related to idempotency
		record.headers().add("messageId", UUID.randomUUID().toString().getBytes());
		
		SendResult<String, ProductCreatedEvent> result = 
				kafkaTemplate.send(record).get();
		
		log.info("Partition: [{}]", result.getRecordMetadata().partition());
		log.info("Topic: [{}]", result.getRecordMetadata().topic());
		log.info("Offset: [{}]", result.getRecordMetadata().offset());
		
		log.info("Product [{}] was created.", productId);
		
		return productId;
	}

}
