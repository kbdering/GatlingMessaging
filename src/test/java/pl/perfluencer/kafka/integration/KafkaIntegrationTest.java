/*
 * Copyright 2026 Perfluencer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.perfluencer.kafka.integration;

import pl.perfluencer.kafka.consumers.KafkaConsumerThread;
import pl.perfluencer.kafka.MessageProcessor;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

// @Ignore("Docker environment issues detected. Check Docker Desktop settings.")
public class KafkaIntegrationTest {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0");
    private static KafkaContainer kafka;

    @BeforeClass
    public static void setUp() throws ExecutionException, InterruptedException {
        kafka = new KafkaContainer(KAFKA_IMAGE);
        kafka.start();

        createTopic("test-topic");
        createTopic("response-topic");
    }

    @AfterClass
    public static void tearDown() {
        kafka.stop();
    }

    private static void createTopic(String topicName) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            admin.createTopics(Collections.singleton(newTopic)).all().get();
        }
    }

    @Test
    public void testEndToEndFlow() throws Exception {
        String responseTopic = "response-topic";
        String correlationId = "corr-123";
        String key = "key-1";

        CountDownLatch messageReceived = new CountDownLatch(1);
        AtomicReference<ConsumerRecords<String, Object>> receivedRecords = new AtomicReference<>();

        // 1. Setup Consumer Thread (simulating the Gatling Kafka Extension's consumer)
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "gatling-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        MessageProcessor mockProcessor = mock(MessageProcessor.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ConsumerRecords<String, Object> records = invocation.getArgument(0);
            if (!records.isEmpty()) {
                receivedRecords.set(records);
                messageReceived.countDown();
            }
            return null;
        }).when(mockProcessor).process(any(ConsumerRecords.class));

        KafkaConsumerThread consumerThread = new KafkaConsumerThread(
                consumerProps, responseTopic, mockProcessor, null, Duration.ofMillis(100), false, 0);
        consumerThread.start();

        try {
            // 2. Setup Producer (simulating the Application Under Test)
            Map<String, Object> producerProps = new HashMap<>();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

            try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
                // 3. Send message to Response Topic (simulating app response)
                ProducerRecord<String, byte[]> responseRecord = new ProducerRecord<>(
                        responseTopic, key, "response-value".getBytes());
                responseRecord.headers().add("correlationId", correlationId.getBytes());
                producer.send(responseRecord).get();
            }

            // 4. Verify Consumer Thread receives the message
            assertTrue("Message should have been received",
                    messageReceived.await(10, TimeUnit.SECONDS));

            assertNotNull("Received records should not be null", receivedRecords.get());
            assertFalse("Received records should not be empty", receivedRecords.get().isEmpty());

        } finally {
            consumerThread.shutdown();
            consumerThread.join(2000);
        }
    }
}
