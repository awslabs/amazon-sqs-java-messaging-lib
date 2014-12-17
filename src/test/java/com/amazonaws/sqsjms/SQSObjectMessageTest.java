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
package com.amazonaws.sqsjms;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SQSObjectMessageTest {

    private SQSSession mockSQSSession;

    @Before
    public void setup() {
        mockSQSSession = mock(SQSSession.class);
    }

    @Test
    public void testSetObject() throws JMSException {
        Map<String, String> expectedPayload = new HashMap<String, String>();
        expectedPayload.put("testKey", "testValue");

        when(mockSQSSession.createObjectMessage()).thenReturn(new SQSObjectMessage());
        ObjectMessage objectMessage = mockSQSSession.createObjectMessage();
        objectMessage.setObject((Serializable) expectedPayload);
        
        Map<String, String> actualPayload = (HashMap<String, String>) objectMessage.getObject();
        Assert.assertEquals(expectedPayload, actualPayload);
    }
    
    @Test
    public void testCreateMessageWithObject() throws JMSException {
        Map<String, String> expectedPayload = new HashMap<String, String>();
        expectedPayload.put("testKey", "testValue");

        when(mockSQSSession.createObjectMessage((Serializable) expectedPayload)).thenReturn(new SQSObjectMessage((Serializable) expectedPayload));
        ObjectMessage objectMessage = mockSQSSession.createObjectMessage((Serializable) expectedPayload);
        
        Map<String, String> actualPayload = (HashMap<String, String>) objectMessage.getObject();
        Assert.assertEquals(expectedPayload, actualPayload);
    }

    @Test
    public void testObjectSerialization() throws JMSException {
        Map<String, String> expectedPayload = new HashMap<String, String>();
        expectedPayload.put("testKey", "testValue");

        SQSObjectMessage sqsObjectMessage = new SQSObjectMessage();

        String serialized = sqsObjectMessage.serialize((Serializable) expectedPayload);

        Assert.assertNotNull("Serialized object should not be null.", serialized);
        Assert.assertFalse("Serialized object should not be empty.", "".equals(serialized));

        Map<String, String> deserialized = (Map<String, String>) sqsObjectMessage.deserialize(serialized);

        Assert.assertNotNull("Deserialized object should not be null.", deserialized);
        Assert.assertEquals("Serialized object should be equal to original object.", expectedPayload, deserialized);
    }

    @Test(expected = JMSException.class)
    public void testDeserializeFault() throws JMSException {
        String wrongString = "Wrong String";
        SQSObjectMessage sqsObjectMessage = new SQSObjectMessage();
        sqsObjectMessage.deserialize(wrongString);
    }


}