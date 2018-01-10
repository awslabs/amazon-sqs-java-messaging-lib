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

import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.JMSX_DELIVERY_COUNT;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.SQSMessagingClientConstants;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import junit.framework.Assert;

import java.util.*;

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

    @Before
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

        Assert.assertTrue(message.propertyExists("myTrueBoolean"));
        Assert.assertEquals(message.getObjectProperty("myTrueBoolean"), true);
        Assert.assertEquals(message.getBooleanProperty("myTrueBoolean"), true);
        
        Assert.assertTrue(message.propertyExists("myFalseBoolean"));
        Assert.assertEquals(message.getObjectProperty("myFalseBoolean"), false);
        Assert.assertEquals(message.getBooleanProperty("myFalseBoolean"), false);
        
        Assert.assertTrue(message.propertyExists("myInteger"));
        Assert.assertEquals(message.getObjectProperty("myInteger"), 100);
        Assert.assertEquals(message.getIntProperty("myInteger"), 100);
        
        Assert.assertTrue(message.propertyExists("myDouble"));
        Assert.assertEquals(message.getObjectProperty("myDouble"), 2.1768);
        Assert.assertEquals(message.getDoubleProperty("myDouble"), 2.1768);
        
        Assert.assertTrue(message.propertyExists("myFloat"));
        Assert.assertEquals(message.getObjectProperty("myFloat"), 3.1457f);
        Assert.assertEquals(message.getFloatProperty("myFloat"), 3.1457f);
        
        Assert.assertTrue(message.propertyExists("myLong"));
        Assert.assertEquals(message.getObjectProperty("myLong"), 1290772974281L);
        Assert.assertEquals(message.getLongProperty("myLong"), 1290772974281L);
        
        Assert.assertTrue(message.propertyExists("myShort"));
        Assert.assertEquals(message.getObjectProperty("myShort"), (short) 123);
        Assert.assertEquals(message.getShortProperty("myShort"), (short) 123);
        
        Assert.assertTrue(message.propertyExists("myByteProperty"));
        Assert.assertEquals(message.getObjectProperty("myByteProperty"), (byte) 'a');
        Assert.assertEquals(message.getByteProperty("myByteProperty"), (byte) 'a');
        
        Assert.assertTrue(message.propertyExists("myString"));
        Assert.assertEquals(message.getObjectProperty("myString"), "StringValue");
        Assert.assertEquals(message.getStringProperty("myString"), "StringValue");

        // Validate property names
        Set<String> propertyNamesSet = new HashSet<String>(Arrays.asList(
                "myTrueBoolean",
                "myFalseBoolean",
                "myInteger",
                "myDouble",
                "myFloat",
                "myLong",
                "myShort",
                "myByteProperty",
                "myString"));

        Enumeration<String > propertyNames = message.getPropertyNames();
        int counter = 0;
        while (propertyNames.hasMoreElements()) {
            assertTrue(propertyNamesSet.contains(propertyNames.nextElement()));
            counter++;
        }
        assertEquals(propertyNamesSet.size(), counter);
        
        message.clearProperties();
        Assert.assertFalse(message.propertyExists("myTrueBoolean"));
        Assert.assertFalse(message.propertyExists("myInteger"));
        Assert.assertFalse(message.propertyExists("myDouble"));
        Assert.assertFalse(message.propertyExists("myFloat"));
        Assert.assertFalse(message.propertyExists("myLong"));
        Assert.assertFalse(message.propertyExists("myShort"));
        Assert.assertFalse(message.propertyExists("myByteProperty"));
        Assert.assertFalse(message.propertyExists("myString"));

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

        try {
            msg.checkBodyWritePermissions();
        } catch (MessageNotWriteableException exception) {
            assertEquals("Message body is not writable", exception.getMessage());
        }

        msg.checkPropertyWritePermissions();

        msg.setWritePermissionsForProperties(false);

        try {
            msg.checkPropertyWritePermissions();
        } catch (MessageNotWriteableException exception) {
            assertEquals("Message properties are not writable", exception.getMessage());
        }
    }

    /**
     * Test get primitive property
     */
    @Test
    public void testGetPrimitiveProperty() throws JMSException {
        SQSMessage msg =  spy(new SQSMessage());
        when(msg.getObjectProperty("testProperty"))
                .thenReturn(null);

        try {
            msg.getPrimitiveProperty(null, String.class);
        } catch (NullPointerException npe) {
            assertEquals("Property name is null", npe.getMessage());
        }

        try {
            msg.getPrimitiveProperty("testProperty", List.class);
        } catch (NumberFormatException exp) {
            assertEquals("Value of property with name testProperty is null.", exp.getMessage());
        }

        try {
            msg.getPrimitiveProperty("testProperty", Double.class);
        } catch (NullPointerException exp) {
            assertEquals("Value of property with name testProperty is null.", exp.getMessage());
        }

        try {
            msg.getPrimitiveProperty("testProperty", Float.class);
        } catch (NullPointerException exp) {
            assertEquals("Value of property with name testProperty is null.", exp.getMessage());
        }

        assertFalse(msg.getPrimitiveProperty("testProperty", Boolean.class));
        assertNull(msg.getPrimitiveProperty("testProperty", String.class));
    }

    /**
     * Test set object property
     */
    @Test
    public void testSetObjectProperty() throws JMSException {
        SQSMessage msg =  spy(new SQSMessage());

        try {
            msg.setObjectProperty(null, 1);
        } catch (IllegalArgumentException exception) {
            assertEquals("Property name can not be null or empty.", exception.getMessage());
        }

        try {
            msg.setObjectProperty("", 1);
        } catch (IllegalArgumentException exception) {
            assertEquals("Property name can not be null or empty.", exception.getMessage());
        }

        try {
            msg.setObjectProperty("Property", null);
        } catch (IllegalArgumentException exception) {
            assertEquals("Property value can not be null or empty.", exception.getMessage());
        }

        try {
            msg.setObjectProperty("Property", "");
        } catch (IllegalArgumentException exception) {
            assertEquals("Property value can not be null or empty.", exception.getMessage());
        }

        try {
            msg.setObjectProperty("Property", new HashSet<String>());
        } catch (MessageFormatException exception) {
            assertEquals("Value of property with name Property has incorrect type java.util.HashSet.",
                         exception.getMessage());
        }

        msg.setWritePermissionsForProperties(false);
        try {
            msg.setObjectProperty("Property", "1");
        } catch (MessageNotWriteableException exception) {
            assertEquals("Message properties are not writable", exception.getMessage());
        }

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

        Map<String,String> systemAttributes = new HashMap<String, String>();
        systemAttributes.put(APPROXIMATE_RECEIVE_COUNT, "100");

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();

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

        com.amazonaws.services.sqs.model.Message sqsMessage = new com.amazonaws.services.sqs.model.Message()
                .withMessageAttributes(messageAttributes)
                .withAttributes(systemAttributes)
                .withMessageId("messageId")
                .withReceiptHandle("ReceiptHandle");

        SQSMessage message = new SQSMessage(ack, "QueueUrl", sqsMessage);

        Assert.assertTrue(message.propertyExists(myTrueBoolean));
        Assert.assertEquals(message.getObjectProperty(myTrueBoolean), true);
        Assert.assertEquals(message.getBooleanProperty(myTrueBoolean), true);

        Assert.assertTrue(message.propertyExists(myFalseBoolean));
        Assert.assertEquals(message.getObjectProperty(myFalseBoolean), false);
        Assert.assertEquals(message.getBooleanProperty(myFalseBoolean), false);

        Assert.assertTrue(message.propertyExists(myInteger));
        Assert.assertEquals(message.getObjectProperty(myInteger), 100);
        Assert.assertEquals(message.getIntProperty(myInteger), 100);

        Assert.assertTrue(message.propertyExists(myDouble));
        Assert.assertEquals(message.getObjectProperty(myDouble), 2.1768);
        Assert.assertEquals(message.getDoubleProperty(myDouble), 2.1768);

        Assert.assertTrue(message.propertyExists(myFloat));
        Assert.assertEquals(message.getObjectProperty(myFloat), 3.1457f);
        Assert.assertEquals(message.getFloatProperty(myFloat), 3.1457f);

        Assert.assertTrue(message.propertyExists(myLong));
        Assert.assertEquals(message.getObjectProperty(myLong), 1290772974281L);
        Assert.assertEquals(message.getLongProperty(myLong), 1290772974281L);

        Assert.assertTrue(message.propertyExists(myShort));
        Assert.assertEquals(message.getObjectProperty(myShort), (short) 123);
        Assert.assertEquals(message.getShortProperty(myShort), (short) 123);

        Assert.assertTrue(message.propertyExists(myByte));
        Assert.assertEquals(message.getObjectProperty(myByte), (byte) 1);
        Assert.assertEquals(message.getByteProperty(myByte), (byte) 1);

        Assert.assertTrue(message.propertyExists(myString));
        Assert.assertEquals(message.getObjectProperty(myString), "StringValue");
        Assert.assertEquals(message.getStringProperty(myString), "StringValue");


        // Validate property names
        Set<String> propertyNamesSet = new HashSet<String>(Arrays.asList(
                myTrueBoolean,
                myFalseBoolean,
                myInteger,
                myDouble,
                myFloat,
                myLong,
                myShort,
                myByte,
                myString,
                JMSX_DELIVERY_COUNT));

        Enumeration<String > propertyNames = message.getPropertyNames();
        int counter = 0;
        while (propertyNames.hasMoreElements()) {
            assertTrue(propertyNamesSet.contains(propertyNames.nextElement()));
            counter++;
        }
        assertEquals(propertyNamesSet.size(), counter);

        message.clearProperties();
        Assert.assertFalse(message.propertyExists("myTrueBoolean"));
        Assert.assertFalse(message.propertyExists("myInteger"));
        Assert.assertFalse(message.propertyExists("myDouble"));
        Assert.assertFalse(message.propertyExists("myFloat"));
        Assert.assertFalse(message.propertyExists("myLong"));
        Assert.assertFalse(message.propertyExists("myShort"));
        Assert.assertFalse(message.propertyExists("myByteProperty"));
        Assert.assertFalse(message.propertyExists("myString"));

        propertyNames = message.getPropertyNames();
        assertFalse(propertyNames.hasMoreElements());
    }
}