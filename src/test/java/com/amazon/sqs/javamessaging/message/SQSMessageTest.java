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
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.JMSX_DELIVERY_COUNT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
        assertEquals(message.getDoubleProperty("myDouble"), 2.1768, 0.0);

        assertTrue(message.propertyExists("myFloat"));
        assertEquals(message.getObjectProperty("myFloat"), 3.1457f);
        assertEquals(message.getFloatProperty("myFloat"), 3.1457f, 0.0);

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
        assertEquals(message.getFloatProperty("myNumber"), 500f, 0.0);
        assertEquals(message.getShortProperty("myNumber"), (short) 500);
        assertEquals(message.getDoubleProperty("myNumber"), 500d, 0.0);
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
                "myString");

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
        SQSMessage msg = new SQSMessage();
        msg.checkBodyWritePermissions();
        msg.setBodyWritePermissions(false);

        assertThatThrownBy(msg::checkBodyWritePermissions)
                .isInstanceOf(MessageNotWriteableException.class)
                .hasMessage("Message body is not writable");

        msg.checkPropertyWritePermissions();
        msg.setWritePermissionsForProperties(false);

        assertThatThrownBy(msg::checkPropertyWritePermissions)
                .isInstanceOf(MessageNotWriteableException.class)
                .hasMessage("Message properties are not writable");
    }

    /**
     * Test get primitive property
     */
    @Test
    public void testGetPrimitiveProperty() throws JMSException {
        SQSMessage msg = spy(new SQSMessage());
        when(msg.getObjectProperty("testProperty")).thenReturn(null);

        assertThatThrownBy(() -> msg.getPrimitiveProperty(null, String.class))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Property name is null");

        assertThatThrownBy(() -> msg.getPrimitiveProperty("testProperty", List.class))
                .isInstanceOf(NumberFormatException.class)
                .hasMessage("Value of property with name testProperty is null.");

        assertThatThrownBy(() -> msg.getPrimitiveProperty("testProperty", Double.class))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Value of property with name testProperty is null.");

        assertThatThrownBy(() -> msg.getPrimitiveProperty("testProperty", Float.class))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Value of property with name testProperty is null.");

        assertFalse(msg.getPrimitiveProperty("testProperty", Boolean.class));
        assertNull(msg.getPrimitiveProperty("testProperty", String.class));
    }

    /**
     * Test set object property
     */
    @Test
    public void testSetObjectProperty() throws JMSException {
        SQSMessage msg = spy(new SQSMessage());

        assertThatThrownBy(() -> msg.setObjectProperty(null, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property name can not be null or empty.");

        assertThatThrownBy(() -> msg.setObjectProperty("", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property name can not be null or empty.");

        assertThatThrownBy(() -> msg.setObjectProperty("Property", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property value can not be null or empty.");

        assertThatThrownBy(() -> msg.setObjectProperty("Property", ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Property value can not be null or empty.");

        assertThatThrownBy(() -> msg.setObjectProperty("Property", new HashSet<>()))
                .isInstanceOf(MessageFormatException.class)
                .hasMessage("Value of property with name Property has incorrect type java.util.HashSet.");

        msg.setWritePermissionsForProperties(false);

        assertThatThrownBy(() -> msg.setObjectProperty("Property", "1"))
                .isInstanceOf(MessageNotWriteableException.class)
                .hasMessage("Message properties are not writable");

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

        Map<MessageSystemAttributeName, String> systemAttributes = new HashMap<>();
        systemAttributes.put(MessageSystemAttributeName.fromValue(APPROXIMATE_RECEIVE_COUNT), "100");

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();

        messageAttributes.put(myTrueBoolean, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.BOOLEAN)
                .stringValue("1")
                .build());

        messageAttributes.put(myFalseBoolean, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.BOOLEAN)
                .stringValue("0")
                .build());

        messageAttributes.put(myInteger, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.INT)
                .stringValue("100")
                .build());

        messageAttributes.put(myDouble, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.DOUBLE)
                .stringValue("2.1768")
                .build());

        messageAttributes.put(myFloat, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.FLOAT)
                .stringValue("3.1457")
                .build());

        messageAttributes.put(myLong, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.LONG)
                .stringValue("1290772974281")
                .build());

        messageAttributes.put(myShort, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.SHORT)
                .stringValue("123")
                .build());

        messageAttributes.put(myByte, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.BYTE)
                .stringValue("1")
                .build());

        messageAttributes.put(myString, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.STRING)
                .stringValue("StringValue")
                .build());

        messageAttributes.put(myCustomString, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.NUMBER + ".custom")
                .stringValue("['one', 'two']")
                .build());

        messageAttributes.put(myNumber, MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.NUMBER)
                .stringValue("500")
                .build());

        software.amazon.awssdk.services.sqs.model.Message sqsMessage = software.amazon.awssdk.services.sqs.model.Message.builder()
                .messageAttributes(messageAttributes)
                .attributes(systemAttributes)
                .messageId("messageId")
                .receiptHandle("ReceiptHandle")
                .build();

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
        assertEquals(message.getDoubleProperty(myDouble), 2.1768, 0.0);

        assertTrue(message.propertyExists(myFloat));
        assertEquals(message.getObjectProperty(myFloat), 3.1457f);
        assertEquals(message.getFloatProperty(myFloat), 3.1457f, 0.0);

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
        assertEquals(message.getFloatProperty(myNumber), 500f, 0.0);
        assertEquals(message.getDoubleProperty(myNumber), 500d, 0.0);


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
                JMSX_DELIVERY_COUNT);

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
