/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.sqs.javamessaging.message;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import jakarta.jms.JMSException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.ObjectMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test the SQSObjectMessageTest class
 */
public class SQSObjectMessageTest {

    /**
     * Test set object
     */
    @Test
    public void testSetObject() throws JMSException {
        Map<String, String> expectedPayload = Map.of("testKey", "testValue");

        ObjectMessage objectMessage = new SQSObjectMessage();
        objectMessage.setObject((Serializable) expectedPayload);
        
        Map<String, String> actualPayload = (Map<String, String>) objectMessage.getObject();
        assertEquals(expectedPayload, actualPayload);
    }

    /**
     * Test create message with object
     */
    @Test
    public void testCreateMessageWithObject() throws JMSException {
        Map<String, String> expectedPayload = Map.of("testKey", "testValue");
        ObjectMessage objectMessage = new SQSObjectMessage((Serializable) expectedPayload);
        
        Map<String, String> actualPayload = (Map<String, String>) objectMessage.getObject();
        assertEquals(expectedPayload, actualPayload);
    }

    /**
     * Test object serialization
     */
    @Test
    public void testObjectSerialization() throws JMSException {
        Map<String, String> expectedPayload = Map.of("testKey", "testValue");
        SQSObjectMessage sqsObjectMessage = new SQSObjectMessage();
        String serialized = SQSObjectMessage.serialize((Serializable) expectedPayload);

        assertNotNull(serialized, "Serialized object should not be null.");
        assertNotEquals("", serialized, "Serialized object should not be empty.");

        Map<String, String> deserialized = (Map<String, String>) SQSObjectMessage.deserialize(serialized);

        assertNotNull(deserialized, "Deserialized object should not be null.");
        assertEquals(expectedPayload, deserialized, "Serialized object should be equal to original object.");

        sqsObjectMessage.clearBody();

        assertNull(sqsObjectMessage.getMessageBody());
    }
    
    /**
     * Test serialization and deserialization with illegal input
     */
    @Test
    public void testDeserializeIllegalInput() throws JMSException {
        String wrongString = "Wrong String";

        assertThrows(JMSException.class, () -> SQSObjectMessage.deserialize(wrongString));
        assertNull(SQSObjectMessage.deserialize(null));
        assertNull(SQSObjectMessage.serialize(null));
    }

    @TempDir
    Path tempDir;

    @AfterEach
    public void clearDeserializationProperties() {
        System.clearProperty(ObjectMessageDeserializationFilter.TRUSTED_PACKAGES_PROPERTY);
        System.clearProperty(ObjectMessageDeserializationFilter.TRUST_ALL_PACKAGES_PROPERTY);
        System.clearProperty(ObjectMessageDeserializationFilter.MAX_DEPTH_PROPERTY);
    }

    static class Gadget implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String sentinelPath;

        Gadget(Path sentinelPath) {
            this.sentinelPath = sentinelPath.toString();
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            try (FileWriter writer = new FileWriter(sentinelPath)) {
                writer.write("ran\n");
            }
        }
    }

    public interface Greeter {
        String greet();
    }

    static class GreeterHandler implements InvocationHandler, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            return "hello";
        }
    }

    @Test
    public void testGetObjectRejectsUntrustedClass() throws Exception {
        Path sentinel = tempDir.resolve("getObject");
        SQSObjectMessage message = new SQSObjectMessage();
        message.setMessageBody(SQSObjectMessage.serialize(new Gadget(sentinel)));

        assertThrows(MessageFormatException.class, message::getObject);
        assertFalse(Files.exists(sentinel), "gadget code must not have run");
    }

    @Test
    public void testDeserializeRejectsUntrustedClass() throws Exception {
        Path sentinel = tempDir.resolve("deserialize");
        String body = SQSObjectMessage.serialize(new Gadget(sentinel));

        assertThrows(MessageFormatException.class, () -> SQSObjectMessage.deserialize(body));
        assertFalse(Files.exists(sentinel), "gadget code must not have run");
    }

    @Test
    public void testReceivedMessageRejectsUntrustedClass() throws Exception {
        Path sentinel = tempDir.resolve("received");
        Message sqsMessage = new Message()
                .withBody(SQSObjectMessage.serialize(new Gadget(sentinel)))
                .withMessageId("messageId")
                .withReceiptHandle("receiptHandle")
                .withMessageAttributes(Map.of("JMS_SQSMessageType",
                        new MessageAttributeValue().withDataType("String").withStringValue("object")))
                .withAttributes(Map.of("ApproximateReceiveCount", "1"));

        SQSObjectMessage message = new SQSObjectMessage(null, "https://sqs.us-east-1.amazonaws.com/12345/queue", sqsMessage);

        assertThrows(MessageFormatException.class, message::getObject);
        assertFalse(Files.exists(sentinel), "gadget code must not have run");
    }

    @Test
    public void testDeserializeRejectsProxyClass() throws Exception {
        Serializable proxy = (Serializable) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Greeter.class}, new GreeterHandler());
        String body = SQSObjectMessage.serialize(proxy);

        assertThrows(MessageFormatException.class, () -> SQSObjectMessage.deserialize(body));
    }

    @Test
    public void testTrustedPackagesPropertyAllowsConfiguredPackage() throws Exception {
        System.setProperty(ObjectMessageDeserializationFilter.TRUSTED_PACKAGES_PROPERTY, "com.amazon.sqs.javamessaging.message");

        Path sentinel = tempDir.resolve("trusted");
        String body = SQSObjectMessage.serialize(new Gadget(sentinel));

        assertNotNull(SQSObjectMessage.deserialize(body));
        assertTrue(Files.exists(sentinel), "trusted class should have been deserialized");
    }

    @Test
    public void testTrustAllPackagesDisablesFiltering() throws Exception {
        System.setProperty(ObjectMessageDeserializationFilter.TRUST_ALL_PACKAGES_PROPERTY, "true");

        Path sentinel = tempDir.resolve("trustAll");
        String body = SQSObjectMessage.serialize(new Gadget(sentinel));

        assertNotNull(SQSObjectMessage.deserialize(body));
        assertTrue(Files.exists(sentinel), "filtering should be disabled");
    }

    @Test
    public void testTrustedPackagePrefixIsNotConfused() throws Exception {
        System.setProperty(ObjectMessageDeserializationFilter.TRUSTED_PACKAGES_PROPERTY, "com.amazon.sqs.javamessaging.mess");

        Path sentinel = tempDir.resolve("prefix");
        String body = SQSObjectMessage.serialize(new Gadget(sentinel));

        assertThrows(MessageFormatException.class, () -> SQSObjectMessage.deserialize(body));
        assertFalse(Files.exists(sentinel), "gadget code must not have run");
    }

    @Test
    public void testArraysAreCheckedByElementType() throws Exception {
        assertNotNull(SQSObjectMessage.deserialize(SQSObjectMessage.serialize(new String[]{"a", "b"})));
        assertNotNull(SQSObjectMessage.deserialize(SQSObjectMessage.serialize(new byte[]{1, 2, 3})));

        Path sentinel = tempDir.resolve("array");
        String body = SQSObjectMessage.serialize(new Gadget[]{new Gadget(sentinel)});

        assertThrows(MessageFormatException.class, () -> SQSObjectMessage.deserialize(body));
        assertFalse(Files.exists(sentinel), "gadget code must not have run");
    }

    private static Serializable nestedSets(int nesting) {
        Set<Object> root = new HashSet<>();
        Set<Object> branch1 = root;
        Set<Object> branch2 = new HashSet<>();
        for (int i = 0; i < nesting; i++) {
            Set<Object> next1 = new HashSet<>();
            Set<Object> next2 = new HashSet<>();
            next1.add("distinguishing element");
            branch1.add(next1);
            branch1.add(next2);
            branch2.add(next1);
            branch2.add(next2);
            branch1 = next1;
            branch2 = next2;
        }
        return (Serializable) root;
    }

    @Test
    public void testDeeplyNestedPayloadIsRejected() throws Exception {
        String body = SQSObjectMessage.serialize(nestedSets(ObjectMessageDeserializationFilter.DEFAULT_MAX_DEPTH + 4));

        long start = System.nanoTime();
        assertThrows(MessageFormatException.class, () -> SQSObjectMessage.deserialize(body));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(elapsedMillis < 1_000L,
                "rejection must happen before the expensive rehashing, took " + elapsedMillis + " ms");
    }

    @Test
    public void testShallowNestedPayloadIsAccepted() throws Exception {
        assertNotNull(SQSObjectMessage.deserialize(SQSObjectMessage.serialize(nestedSets(8))));
    }

    @Test
    public void testSubpackagesOfDefaultsAreNotTrusted() throws Exception {
        String body = SQSObjectMessage.serialize(new ConcurrentHashMap<>(Map.of("key", "value")));

        assertThrows(MessageFormatException.class, () -> SQSObjectMessage.deserialize(body));

        System.setProperty(ObjectMessageDeserializationFilter.TRUSTED_PACKAGES_PROPERTY, "java.util.concurrent");
        assertNotNull(SQSObjectMessage.deserialize(body));
    }

    @Test
    public void testDefaultTrustedPackagesCoverCommonValueTypes() throws Exception {
        Map<String, Serializable> payload = new HashMap<>();
        payload.put("string", "text");
        payload.put("integer", 42);
        payload.put("bigDecimal", new BigDecimal("1.23"));
        payload.put("instant", Instant.ofEpochMilli(0));
        payload.put("duration", Duration.ofSeconds(30));

        assertEquals(payload, SQSObjectMessage.deserialize(SQSObjectMessage.serialize((Serializable) payload)));
    }

    @Test
    public void testExistingFilterIsNotWidened() {
        ObjectInputFilter rejectEverything = info -> ObjectInputFilter.Status.REJECTED;
        ObjectInputFilter ours = info -> ObjectInputFilter.Status.ALLOWED;

        ObjectInputFilter combined = ObjectMessageDeserializationFilter.combineWith(ours, rejectEverything);

        assertEquals(ObjectInputFilter.Status.REJECTED, combined.checkInput(filterInfoFor(String.class)));
    }

    private static ObjectInputFilter.FilterInfo filterInfoFor(Class<?> clazz) {
        return new ObjectInputFilter.FilterInfo() {
            public Class<?> serialClass() { return clazz; }
            public long arrayLength() { return -1; }
            public long depth() { return 1; }
            public long references() { return 1; }
            public long streamBytes() { return 1; }
        };
    }
}
