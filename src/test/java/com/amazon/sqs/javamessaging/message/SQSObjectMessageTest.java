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

import jakarta.jms.JMSException;
import jakarta.jms.ObjectMessage;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
        Map<String, String> expectedPayload = new HashMap<>();
        expectedPayload.put("testKey", "testValue");

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
        assertThatThrownBy(() -> SQSObjectMessage.deserialize("Wrong String"))
                .isInstanceOf(JMSException.class);

        assertNull(SQSObjectMessage.deserialize(null));
        assertNull(SQSObjectMessage.serialize(null));
    }
}
