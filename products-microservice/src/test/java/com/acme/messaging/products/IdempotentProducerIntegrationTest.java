package com.acme.messaging.products;

import com.acme.messaging.core.ProductCreatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Slf4j
// If you do not specify an application properties file or test profile,
// the default src/main/resources/application.properties will be used.
@SpringBootTest
class IdempotentProducerIntegrationTest {

    @MockBean
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @Test
    void testIdempotentProducerConfigSuccess() {

        ProducerFactory<String, ProductCreatedEvent> producerFactory =
                kafkaTemplate.getProducerFactory();

        Map<String, Object> producerConfig = producerFactory.getConfigurationProperties();

        log.info("Producer configuration: {}", producerConfig);

        Assertions.assertNotNull(producerConfig);

        Assertions.assertTrue((Boolean) producerConfig.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));

        Assertions.assertTrue("all".equalsIgnoreCase((String)producerConfig.get(ProducerConfig.ACKS_CONFIG)));

        if (producerConfig.containsKey(ProducerConfig.RETRIES_CONFIG)) {
            Assertions.assertTrue(Integer.parseInt(producerConfig.get(ProducerConfig.RETRIES_CONFIG).toString()) > 0);
        }

        log.info("Idempotence configuration is correct.");

    }
}
