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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CustomObjectSerializationTest {

    @AfterClass
    public static void teardown() {
    }

    static class MyObject {
        public String name;
        public int id;

        public MyObject(String name, int id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            MyObject myObject = (MyObject) o;
            return id == myObject.id && java.util.Objects.equals(name, myObject.name);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, id);
        }
    }

    static class MyObjectSerializer implements Serializer<MyObject> {
        @Override
        public byte[] serialize(String topic, MyObject data) {
            return (data.name + ":" + data.id).getBytes(StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testSendCustomObject() {
        @SuppressWarnings("unchecked")
        Serializer<Object> valueSerializer = (Serializer<Object>) (Serializer<?>) new MyObjectSerializer();
        MockProducer<String, Object> producer = new MockProducer<>(true, new StringSerializer(),
                valueSerializer);

        String topic = "custom-obj-topic";
        String key = "key";
        MyObject message = new MyObject("test", 123);

        producer.send(new ProducerRecord<>(topic, key, message));

        assertEquals(1, producer.history().size());
        assertEquals(message, producer.history().get(0).value());
    }
}
