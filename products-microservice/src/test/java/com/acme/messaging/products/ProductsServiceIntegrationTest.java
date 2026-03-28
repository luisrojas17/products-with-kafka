package com.acme.messaging.products;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import com.acme.messaging.core.ProductCreatedEvent;
import com.acme.messaging.products.rest.dto.ProductDto;
import com.acme.messaging.products.services.ProductService;

/**
 * This class is uni test used to test com.acme.messaging.products.services.impl.ProductService.
 *
 */
@Slf4j
// This is optional. However, It wil specify that the spring context has been clean in order to avoid any corrupted bean.
@DirtiesContext
// To specify that JUnit creates only one instance per class since by default, Junit will create
// a new instance of the test class before executing each test method.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// To set which profile spring will be used. This will use the application-test.properties
@ActiveProfiles("test") // application-test.properties
// To configure and starting the embedded Kafka server
@EmbeddedKafka(partitions=3, count=3, controlledShutdown=true)
// The property spring.embedded.kafka.brokers contains the host related to embedded Kafka server.
@SpringBootTest(properties="spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}")
class ProductsServiceIntegrationTest {
	
	@Autowired
	private ProductService productService;

	/**
	 * To inject the Kafka Broker self-contained in-memory.
	 */
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;

	/**
	 * To access to test application properties file. The application properties file to use will be
	 * which is related to test profile.
	 */
	@Autowired
	private Environment environment;

	/**
	 * To use as a bridge between Spring application and Kafka Broker.
	 * This is the component that consumes messages from Kafka and invokes your listener code.
	 */
	private KafkaMessageListenerContainer<String, ProductCreatedEvent> container;

	/**
	 * To store the messages received from the Kafka topic.
	 */
	private BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> records;

	/**
	 * To execute one time only before all tests methods.
	 */
	@BeforeAll
	void setUp() {

		//It will be responsible to create the Kafka Consumers using the consumer properties defined into
		// getConsumerProperties() method.
		DefaultKafkaConsumerFactory<String, Object> consumerFactory =
				new DefaultKafkaConsumerFactory<>(getConsumerProperties());

		// To define the runtime behavior of a Kafka listener container.
		// In this case, we are defining topics or partitions to listen to.
		ContainerProperties containerProperties =
				new ContainerProperties(environment.getProperty("product-created-events-topic-name"));

		// To consume the messages when arrives into the Kafka Broker/Topic.
		container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		// When the message arrives this will be added to the Queue.
		records = new LinkedBlockingQueue<>();

		container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) records::add);

		// To start the KafkaMessageListener
		container.start();

		ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
		
	}

	// Usually the test name has to follow next pattern: test<System Under Test>_Condition or State Change_ExpectedResult
	@Test
	void testSendAndConsumeProductCreatedSuccess() throws Exception {

		// AAA Pattern
		// Arrange
		String title="iPhone 11";
		BigDecimal price = new BigDecimal(600);
		Integer quantity = 1;
		
		ProductDto productDto = new ProductDto();
		productDto.setPrice(price);
		productDto.setQuantity(quantity);
		productDto.setTitle(title);
		
		// Act
		productService.create(productDto);

		// Assert

		// To read consumer records from records queue (records instance) waiting after 3 seconds after these
		// are available. If after 3 seconds there are any record available the poll method will return null.
		ConsumerRecord<String, ProductCreatedEvent> message = records.poll(3000, TimeUnit.MILLISECONDS);

		assertNotNull(message);
		assertNotNull(message.key());

		ProductCreatedEvent productCreatedEvent = message.value();
		assertEquals(productDto.getQuantity(), productCreatedEvent.getQuantity());
		assertEquals(productDto.getTitle(), productCreatedEvent.getTitle());
		assertEquals(productDto.getPrice(), productCreatedEvent.getPrice());

		log.info("It was received the product [{}].", productCreatedEvent);
	}

	/**
	 * Returns the properties for the consumer.
	 *
	 * @return a map of configuration properties for Kafka Consumer.
	 */
	private Map<String, Object> getConsumerProperties() {
		return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class,
				ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class,
				ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id", "product-created-events"),
				JsonDeserializer.TRUSTED_PACKAGES, environment.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages", "com.acme.messaging.core"),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, environment.getProperty("spring.kafka.consumer.auto-offset-reset", "earliest")
				);
	}

	/**
	 * To stop the KafkaMessageListener after all test classes have finalized their work.
	 */
	@AfterAll
	void tearDown() {
		container.stop();
	}
	
	
}
