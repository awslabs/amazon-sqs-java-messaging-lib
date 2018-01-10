/*
 * Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.junit.Assert;
import org.junit.Test;

import com.amazon.sqs.javamessaging.message.SQSObjectMessage;

/**
 * Test the SQSObjectMessageTest class
 */
public class SQSObjectMessageTest {

    /**
     * Test set object
     */
    @Test
    public void testSetObject() throws JMSException {
        Map<String, String> expectedPayload = new HashMap<String, String>();
        expectedPayload.put("testKey", "testValue");

        ObjectMessage objectMessage = new SQSObjectMessage();
        objectMessage.setObject((Serializable) expectedPayload);
        
        Map<String, String> actualPayload = (HashMap<String, String>) objectMessage.getObject();
        assertEquals(expectedPayload, actualPayload);
    }

    /**
     * Test create message with object
     */
    @Test
    public void testCreateMessageWithObject() throws JMSException {
        Map<String, String> expectedPayload = new HashMap<String, String>();
        expectedPayload.put("testKey", "testValue");

        ObjectMessage objectMessage = new SQSObjectMessage((Serializable) expectedPayload);
        
        Map<String, String> actualPayload = (HashMap<String, String>) objectMessage.getObject();
        assertEquals(expectedPayload, actualPayload);
    }

    /**
     * Test object serialization
     */
    @Test
    public void testObjectSerialization() throws JMSException {
        Map<String, String> expectedPayload = new HashMap<String, String>();
        expectedPayload.put("testKey", "testValue");

        SQSObjectMessage sqsObjectMessage = new SQSObjectMessage();

        String serialized = sqsObjectMessage.serialize((Serializable) expectedPayload);

        assertNotNull("Serialized object should not be null.", serialized);
        Assert.assertFalse("Serialized object should not be empty.", "".equals(serialized));

        Map<String, String> deserialized = (Map<String, String>) sqsObjectMessage.deserialize(serialized);

        assertNotNull("Deserialized object should not be null.", deserialized);
        assertEquals("Serialized object should be equal to original object.", expectedPayload, deserialized);

        sqsObjectMessage.clearBody();

        assertNull(sqsObjectMessage.getMessageBody());

    }
    
    /**
     * Test serialization and deserialization with illegal input
     */
    @Test
    public void testDeserializeIllegalInput() throws JMSException {
        String wrongString = "Wrong String";
        SQSObjectMessage sqsObjectMessage = new SQSObjectMessage();

        try {
            sqsObjectMessage.deserialize(wrongString);
            fail();
        } catch (JMSException exception) {
        }

        assertNull(sqsObjectMessage.deserialize(null));

        assertNull(sqsObjectMessage.serialize(null));
    }
}