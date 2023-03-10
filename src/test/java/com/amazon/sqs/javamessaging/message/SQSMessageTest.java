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

import com.amazon.sqs.javamessaging.SQSMessagingClientConstants;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.JMSX_DELIVERY_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageTest class
 */
public class SQSMessageTest {
    private SQSSession mockSQSSession;
    final String myTrueBoolean = "myTrueBoolean";
    final String myFalseBoolean = "myFalseBoolean";
    final String myInteger = "myInteger";
    final String myDouble = "myDouble";
    final String myFloat = "myFloat";
    final String myLong = "myLong";
    final String myShort = "myShort";
    final String myByte = "myByte";
    final String myString = "myString";
    final String myCustomString = "myCustomString";
    final String myNumber = "myNumber";

    @BeforeEach
    public void setup() {
        mockSQSSession = mock(SQSSession.class);
    }

    /**
     * Test setting SQSMessage property
     */
    @Test
    public void testProperty() throws JMSException {
        when(mockSQSSession.createMessage()).thenReturn(new SQSMessage());
        Message message = mockSQSSession.createMessage();

        message.setBooleanProperty("myTrueBoolean", true);
        message.setBooleanProperty("myFalseBoolean", false);
        message.setIntProperty("myInteger", 100);
        message.setDoubleProperty("myDouble", 2.1768);
        message.setFloatProperty("myFloat", 3.1457f);
        message.setLongProperty("myLong", 1290772974281L);
        message.setShortProperty("myShort", (short) 123);
        message.setByteProperty("myByteProperty", (byte) 'a');
        message.setStringProperty("myString", "StringValue");
        message.setStringProperty("myNumber", "500");

        assertTrue(message.propertyExists("myTrueBoolean"));
        assertEquals(message.getObjectProperty("myTrueBoolean"), true);
        assertTrue(message.getBooleanProperty("myTrueBoolean"));
        
        assertTrue(message.propertyExists("myFalseBoolean"));
        assertEquals(message.getObjectProperty("myFalseBoolean"), false);
        assertFalse(message.getBooleanProperty("myFalseBoolean"));
        
        assertTrue(message.propertyExists("myInteger"));
        assertEquals(message.getObjectProperty("myInteger"), 100);
        assertEquals(message.getIntProperty("myInteger"), 100);
        
        assertTrue(message.propertyExists("myDouble"));
        assertEquals(message.getObjectProperty("myDouble"), 2.1768);
        assertEquals(message.getDoubleProperty("myDouble"), 2.1768);
        
        assertTrue(message.propertyExists("myFloat"));
        assertEquals(message.getObjectProperty("myFloat"), 3.1457f);
        assertEquals(message.getFloatProperty("myFloat"), 3.1457f);
        
        assertTrue(message.propertyExists("myLong"));
        assertEquals(message.getObjectProperty("myLong"), 1290772974281L);
        assertEquals(message.getLongProperty("myLong"), 1290772974281L);
        
        assertTrue(message.propertyExists("myShort"));
        assertEquals(message.getObjectProperty("myShort"), (short) 123);
        assertEquals(message.getShortProperty("myShort"), (short) 123);
        
        assertTrue(message.propertyExists("myByteProperty"));
        assertEquals(message.getObjectProperty("myByteProperty"), (byte) 'a');
        assertEquals(message.getByteProperty("myByteProperty"), (byte) 'a');
        
        assertTrue(message.propertyExists("myString"));
        assertEquals(message.getObjectProperty("myString"), "StringValue");
        assertEquals(message.getStringProperty("myString"), "StringValue");

        assertTrue(message.propertyExists("myNumber"));
        assertEquals(message.getObjectProperty("myNumber"), "500");
        assertEquals(message.getStringProperty("myNumber"), "500");
        assertEquals(message.getLongProperty("myNumber"), 500L);
        assertEquals(message.getFloatProperty("myNumber"), 500f);
        assertEquals(message.getShortProperty("myNumber"), (short) 500);
        assertEquals(message.getDoubleProperty("myNumber"), 500d);
        assertEquals(message.getIntProperty("myNumber"), 500);

        // Validate property names
        Set<String> propertyNamesSet = Set.of(
                "myTrueBoolean",
                "myFalseBoolean",
                "myInteger",
                "myDouble",
                "myFloat",
                "myLong",
                "myShort",
                "myByteProperty",
                "myNumber",
                "myString"
        );

        Enumeration<String> propertyNames = message.getPropertyNames();
        int counter = 0;
        while (propertyNames.hasMoreElements()) {
            assertTrue(propertyNamesSet.contains(propertyNames.nextElement()));
            counter++;
        }
        assertEquals(propertyNamesSet.size(), counter);
        
        message.clearProperties();
        assertFalse(message.propertyExists("myTrueBoolean"));
        assertFalse(message.propertyExists("myInteger"));
        assertFalse(message.propertyExists("myDouble"));
        assertFalse(message.propertyExists("myFloat"));
        assertFalse(message.propertyExists("myLong"));
        assertFalse(message.propertyExists("myShort"));
        assertFalse(message.propertyExists("myByteProperty"));
        assertFalse(message.propertyExists("myString"));
        assertFalse(message.propertyExists("myNumber"));

        propertyNames = message.getPropertyNames();
        assertFalse(propertyNames.hasMoreElements());
    }

    /**
     * Test check property write permissions
     */
    @Test
    public void testCheckPropertyWritePermissions() throws JMSException {
        SQSMessage msg =  new SQSMessage();
        msg.checkBodyWritePermissions();
        msg.setBodyWritePermissions(false);

        assertThrows(MessageNotWriteableException.class, msg::checkBodyWritePermissions, "Message body is not writable");

        msg.checkPropertyWritePermissions();
        msg.setWritePermissionsForProperties(false);

        assertThrows(MessageNotWriteableException.class, msg::checkPropertyWritePermissions, "Message properties are not writable");
    }

    /**
     * Test get primitive property
     */
    @Test
    public void testGetPrimitiveProperty() throws JMSException {
        SQSMessage msg = spy(new SQSMessage());
        when(msg.getObjectProperty("testProperty")).thenReturn(null);

        assertThrows(NullPointerException.class, () -> msg.getPrimitiveProperty(null, String.class),
                "Property name is null");

        assertThrows(NumberFormatException.class, () -> msg.getPrimitiveProperty("testProperty", List.class),
                "Value of property with name testProperty is null.");

        assertThrows(NullPointerException.class, () -> msg.getPrimitiveProperty("testProperty", Double.class),
                "Value of property with name testProperty is null.");

        assertThrows(NullPointerException.class, () -> msg.getPrimitiveProperty("testProperty", Float.class),
                "Value of property with name testProperty is null.");

        assertFalse(msg.getPrimitiveProperty("testProperty", Boolean.class));
        assertNull(msg.getPrimitiveProperty("testProperty", String.class));
    }

    /**
     * Test set object property
     */
    @Test
    public void testSetObjectProperty() throws JMSException {
        SQSMessage msg = spy(new SQSMessage());

        assertThrows(IllegalArgumentException.class, () -> msg.setObjectProperty(null, 1),
                "Property name can not be null or empty.");

        assertThrows(IllegalArgumentException.class, () -> msg.setObjectProperty("", 1),
                "Property name can not be null or empty.");

        assertThrows(IllegalArgumentException.class, () -> msg.setObjectProperty("Property", null),
                "Property value can not be null or empty.");

        assertThrows(IllegalArgumentException.class, () -> msg.setObjectProperty("Property", ""),
                "Property value can not be null or empty.");

        assertThrows(MessageFormatException.class, () -> msg.setObjectProperty("Property", new HashSet<String>()),
                "Value of property with name Property has incorrect type java.util.HashSet.");

        msg.setWritePermissionsForProperties(false);

        assertThrows(MessageNotWriteableException.class, () -> msg.setObjectProperty("Property", "1"),
                "Message properties are not writable");

        msg.setWritePermissionsForProperties(true);
        msg.setObjectProperty("Property", "1");

        assertEquals("1", msg.getJMSMessagePropertyValue("Property").getValue());
    }

    /**
     * Test using SQS message attribute during SQS Message constructing
     */
    @Test
    public void testSQSMessageAttributeToProperty() throws JMSException {

        Acknowledger ack = mock(Acknowledger.class);

        Map<String,String> systemAttributes = Map.of(APPROXIMATE_RECEIVE_COUNT, "100");

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

        messageAttributes.put(myTrueBoolean, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.BOOLEAN)
                .withStringValue("1"));

        messageAttributes.put(myFalseBoolean, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.BOOLEAN)
                .withStringValue("0"));

        messageAttributes.put(myInteger, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.INT)
                .withStringValue("100"));

        messageAttributes.put(myDouble, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.DOUBLE)
                .withStringValue("2.1768"));

        messageAttributes.put(myFloat, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.FLOAT)
                .withStringValue("3.1457"));

        messageAttributes.put(myLong, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.LONG)
                .withStringValue("1290772974281"));

        messageAttributes.put(myShort, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.SHORT)
                .withStringValue("123"));

        messageAttributes.put(myByte, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.BYTE)
                .withStringValue("1"));

        messageAttributes.put(myString, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.STRING)
                .withStringValue("StringValue"));

        messageAttributes.put(myCustomString, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.NUMBER + ".custom")
                .withStringValue("['one', 'two']"));

        messageAttributes.put(myNumber, new MessageAttributeValue()
                .withDataType(SQSMessagingClientConstants.NUMBER)
                .withStringValue("500"));

        com.amazonaws.services.sqs.model.Message sqsMessage = new com.amazonaws.services.sqs.model.Message()
                .withMessageAttributes(messageAttributes)
                .withAttributes(systemAttributes)
                .withMessageId("messageId")
                .withReceiptHandle("ReceiptHandle");

        SQSMessage message = new SQSMessage(ack, "QueueUrl", sqsMessage);

        assertTrue(message.propertyExists(myTrueBoolean));
        assertEquals(message.getObjectProperty(myTrueBoolean), true);
        assertTrue(message.getBooleanProperty(myTrueBoolean));

        assertTrue(message.propertyExists(myFalseBoolean));
        assertEquals(message.getObjectProperty(myFalseBoolean), false);
        assertFalse(message.getBooleanProperty(myFalseBoolean));

        assertTrue(message.propertyExists(myInteger));
        assertEquals(message.getObjectProperty(myInteger), 100);
        assertEquals(message.getIntProperty(myInteger), 100);

        assertTrue(message.propertyExists(myDouble));
        assertEquals(message.getObjectProperty(myDouble), 2.1768);
        assertEquals(message.getDoubleProperty(myDouble), 2.1768);

        assertTrue(message.propertyExists(myFloat));
        assertEquals(message.getObjectProperty(myFloat), 3.1457f);
        assertEquals(message.getFloatProperty(myFloat), 3.1457f);

        assertTrue(message.propertyExists(myLong));
        assertEquals(message.getObjectProperty(myLong), 1290772974281L);
        assertEquals(message.getLongProperty(myLong), 1290772974281L);

        assertTrue(message.propertyExists(myShort));
        assertEquals(message.getObjectProperty(myShort), (short) 123);
        assertEquals(message.getShortProperty(myShort), (short) 123);

        assertTrue(message.propertyExists(myByte));
        assertEquals(message.getObjectProperty(myByte), (byte) 1);
        assertEquals(message.getByteProperty(myByte), (byte) 1);

        assertTrue(message.propertyExists(myString));
        assertEquals(message.getObjectProperty(myString), "StringValue");
        assertEquals(message.getStringProperty(myString), "StringValue");

        assertTrue(message.propertyExists(myCustomString));
        assertEquals(message.getObjectProperty(myCustomString), "['one', 'two']");
        assertEquals(message.getStringProperty(myCustomString), "['one', 'two']");

        assertTrue(message.propertyExists(myNumber));
        assertEquals(message.getObjectProperty(myNumber), "500");
        assertEquals(message.getStringProperty(myNumber), "500");
        assertEquals(message.getIntProperty(myNumber), 500);
        assertEquals(message.getShortProperty(myNumber), (short) 500);
        assertEquals(message.getLongProperty(myNumber), 500L);
        assertEquals(message.getFloatProperty(myNumber), 500f);
        assertEquals(message.getDoubleProperty(myNumber), 500d);


        // Validate property names
        Set<String> propertyNamesSet = Set.of(
                myTrueBoolean,
                myFalseBoolean,
                myInteger,
                myDouble,
                myFloat,
                myLong,
                myShort,
                myByte,
                myString,
                myCustomString,
                myNumber,
                JMSX_DELIVERY_COUNT
        );

        Enumeration<String> propertyNames = message.getPropertyNames();
        int counter = 0;
        while (propertyNames.hasMoreElements()) {
            assertTrue(propertyNamesSet.contains(propertyNames.nextElement()));
            counter++;
        }
        assertEquals(propertyNamesSet.size(), counter);

        message.clearProperties();
        assertFalse(message.propertyExists("myTrueBoolean"));
        assertFalse(message.propertyExists("myInteger"));
        assertFalse(message.propertyExists("myDouble"));
        assertFalse(message.propertyExists("myFloat"));
        assertFalse(message.propertyExists("myLong"));
        assertFalse(message.propertyExists("myShort"));
        assertFalse(message.propertyExists("myByteProperty"));
        assertFalse(message.propertyExists("myString"));
        assertFalse(message.propertyExists("myNumber"));

        propertyNames = message.getPropertyNames();
        assertFalse(propertyNames.hasMoreElements());
    }
}
