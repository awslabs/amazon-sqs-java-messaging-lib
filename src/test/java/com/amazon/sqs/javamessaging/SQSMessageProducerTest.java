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
package com.amazon.sqs.javamessaging;


import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSMessageProducer;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.Base64;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.IllegalStateException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Destination;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageProducerTest class
 */
public class SQSMessageProducerTest {

    public static final String QUEUE_URL = "QueueUrl";
    public static final String QUEUE_NAME = "QueueName";
    public static final String MESSAGE_ID_1 = "MessageId1";
    public static final String MESSAGE_ID_2 = "MessageId2";

    private SQSMessageProducer producer;
    private SQSQueueDestination destination;
    private SQSSession sqsSession;
    private SQSConnection sqsConnection;
    private AmazonSQSMessagingClientWrapper amazonSQSClient;
    private Acknowledger acknowledger;

    @Before
    public void setup() throws JMSException {

        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);
        sqsConnection = mock(SQSConnection.class);

        acknowledger = mock(Acknowledger.class);

        sqsSession = mock(SQSSession.class);
        destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        producer = spy(new SQSMessageProducer(amazonSQSClient, sqsSession, destination));
    }

    /**
     * Test check If Destination Already Set
     */
    @Test
    public void testCheckIfDestinationAlreadySet() throws JMSException {

        try {
            producer.checkIfDestinationAlreadySet();
        } catch (UnsupportedOperationException uoe) {
            assertEquals("MessageProducer already specified a destination at creation time.", uoe.getMessage());
        }

        producer = spy(new SQSMessageProducer(amazonSQSClient, sqsSession, null));
        producer.checkIfDestinationAlreadySet();
    }

    /**
     * Test check closed
     */
    @Test
    public void testCheckClosed() throws JMSException {

        /*
         * Check exception is thrown when producer is closed
         */
        producer.isClosed().set(true);
        try {
            producer.checkClosed();
        } catch (IllegalStateException ise) {
            assertEquals("The producer is closed.", ise.getMessage());
        }

        /*
         * Check no op when producer is closed
         */
        producer.isClosed().set(false);
        producer.checkClosed();
    }

    /**
     * Test propertyToMessageAttribute with empty messages of different type
     */
    @Test
    public void testPropertyToMessageAttributeWithEmpty() throws JMSException {

        /*
         * Test Empty text message default attribute
         */
        SQSMessage sqsText = new SQSTextMessage();
        Map<String, MessageAttributeValue> messageAttributeText = producer.propertyToMessageAttribute(sqsText);

        assertEquals(0, messageAttributeText.size());

        /*
         * Test Empty object message default attribute
         */
        SQSMessage sqsObject = new SQSObjectMessage();
        Map<String, MessageAttributeValue> messageAttributeObject = producer.propertyToMessageAttribute(sqsObject);

        assertEquals(0, messageAttributeObject.size());

        /*
         * Test Empty byte message default attribute
         */
        MessageAttributeValue messageAttributeValueByte = new MessageAttributeValue();
        messageAttributeValueByte.setDataType("String");
        messageAttributeValueByte.setStringValue("byte");

        SQSMessage sqsByte = new SQSBytesMessage();
        Map<String, MessageAttributeValue> messageAttributeByte = producer.propertyToMessageAttribute(sqsByte);

        assertEquals(0, messageAttributeObject.size());
    }

    /**
     * Test propertyToMessageAttribute with messages of different type
     */
    @Test
    public void testPropertyToMessageAttribute() throws JMSException {

        internalTestPropertyToMessageAttribute(new SQSTextMessage());

        internalTestPropertyToMessageAttribute(new SQSObjectMessage());

        internalTestPropertyToMessageAttribute(new SQSBytesMessage());
    }

    public void internalTestPropertyToMessageAttribute(SQSMessage sqsText) throws JMSException {

        /*
         * Setup JMS message property
         */
        String booleanProperty = "BooleanProperty";
        String byteProperty = "ByteProperty";
        String shortProperty = "ShortProperty";
        String intProperty = "IntProperty";
        String longProperty = "LongProperty";
        String floatProperty = "FloatProperty";
        String doubleProperty = "DoubleProperty";
        String stringProperty = "StringProperty";
        String objectProperty = "ObjectProperty";

        sqsText.setBooleanProperty(booleanProperty, true);
        sqsText.setByteProperty(byteProperty, (byte)1);
        sqsText.setShortProperty(shortProperty, (short) 2);
        sqsText.setIntProperty(intProperty, 3);
        sqsText.setLongProperty(longProperty, 4L);
        sqsText.setFloatProperty(floatProperty, (float)5.0);
        sqsText.setDoubleProperty(doubleProperty, 6.0);
        sqsText.setStringProperty(stringProperty, "seven");
        sqsText.setObjectProperty(objectProperty, new Integer(8));

        MessageAttributeValue messageAttributeValueBoolean = new MessageAttributeValue();
        messageAttributeValueBoolean.setDataType("Number.Boolean");
        messageAttributeValueBoolean.setStringValue("1");

        MessageAttributeValue messageAttributeValueByte = new MessageAttributeValue();
        messageAttributeValueByte.setDataType("Number.byte");
        messageAttributeValueByte.setStringValue("1");

        MessageAttributeValue messageAttributeValueShort = new MessageAttributeValue();
        messageAttributeValueShort.setDataType("Number.short");
        messageAttributeValueShort.setStringValue("2");

        MessageAttributeValue messageAttributeValueInt = new MessageAttributeValue();
        messageAttributeValueInt.setDataType("Number.int");
        messageAttributeValueInt.setStringValue("3");

        MessageAttributeValue messageAttributeValueLong = new MessageAttributeValue();
        messageAttributeValueLong.setDataType("Number.long");
        messageAttributeValueLong.setStringValue("4");

        MessageAttributeValue messageAttributeValueFloat = new MessageAttributeValue();
        messageAttributeValueFloat.setDataType("Number.float");
        messageAttributeValueFloat.setStringValue("5.0");

        MessageAttributeValue messageAttributeValueDouble = new MessageAttributeValue();
        messageAttributeValueDouble.setDataType("Number.double");
        messageAttributeValueDouble.setStringValue("6.0");

        MessageAttributeValue messageAttributeValueString = new MessageAttributeValue();
        messageAttributeValueString.setDataType("String");
        messageAttributeValueString.setStringValue("seven");

        MessageAttributeValue messageAttributeValueObject = new MessageAttributeValue();
        messageAttributeValueObject.setDataType("Number.int");
        messageAttributeValueObject.setStringValue("8");

        MessageAttributeValue messageAttributeValueJMSSQSMessageType = new MessageAttributeValue();
        messageAttributeValueJMSSQSMessageType.setDataType("String");
        messageAttributeValueJMSSQSMessageType.setStringValue("text");

        /*
         * Convert property to sqs message attribute
         */
        Map<String, MessageAttributeValue> messageAttribute = producer.propertyToMessageAttribute(sqsText);

        /*
         * Verify results
         */
        assertEquals(messageAttributeValueBoolean, messageAttribute.get(booleanProperty));
        assertEquals(messageAttributeValueByte, messageAttribute.get(byteProperty));
        assertEquals(messageAttributeValueShort, messageAttribute.get(shortProperty));
        assertEquals(messageAttributeValueInt, messageAttribute.get(intProperty));
        assertEquals(messageAttributeValueLong, messageAttribute.get(longProperty));
        assertEquals(messageAttributeValueFloat, messageAttribute.get(floatProperty));
        assertEquals(messageAttributeValueDouble, messageAttribute.get(doubleProperty));
        assertEquals(messageAttributeValueString, messageAttribute.get(stringProperty));
        assertEquals(messageAttributeValueObject, messageAttribute.get(objectProperty));

    }

    /**
     * Test sendInternal input of Non SQS message
     */
    @Test
    public void testSendInternalNonSQSMessage() throws JMSException {

        Message msg = mock(Message.class);

        try {
            producer.sendInternal(destination, msg);
            fail();
        } catch (JMSException jmse) {
            // expected
        }
    }

    /**
     * Test sendInternal input of Non SQS message
     */
    @Test
    public void testSendInternalAlreadyClosed() throws JMSException {

        producer.isClosed().set(true);
        SQSMessage msg = mock(SQSMessage.class);

        try {
            producer.sendInternal(destination, msg);
            fail();
        } catch (JMSException jmse) {
            // expected
        }
    }

    /**
     * Test sendInternal input that SQSMessage is not a valid input
     */
    @Test
    public void testSendInternalNoMessageBody() throws JMSException {

        SQSMessage msg = mock(SQSMessage.class);

        try {
            producer.sendInternal(destination, msg);
            fail();
        } catch (JMSException jmse) {
            //expected
        }

        verify(msg).setJMSDestination(destination);
    }

    /**
     * Test sendInternal input with SQSTextMessage
     */
    @Test
    public void testSendInternalSQSTextMessage() throws JMSException {

        String messageBody1 = "MyText1";
        String messageBody2 = "MyText2";
        SQSTextMessage msg = spy(new SQSTextMessage(messageBody1));

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("text");

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_1))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_2));

        producer.sendInternal(destination, msg);

        /*
         * Re send the message
         */
        msg.setText(messageBody2);
        producer.sendInternal(destination, msg);

        List<String> messagesBody = Arrays.asList(messageBody1, messageBody2);
        verify(amazonSQSClient, times(2)).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messagesBody, messageAttributes)));
        verify(msg, times(2)).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_2);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_2);
    }

    /**
     * Test sendInternal input with SQSTextMessage
     */
    @Test
    public void testSendInternalSQSTextMessageFromReceivedMessage() throws JMSException {

        /*
         * Set up non JMS sqs message
         */
        Map<String,MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.TEXT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                            .withMessageAttributes(mapMessageAttributes)
                            .withAttributes(mapAttributes)
                            .withBody("MessageBody");

        SQSTextMessage msg = spy(new SQSTextMessage(acknowledger, QUEUE_URL, message));

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_1));

        producer.sendInternal(destination, msg);

        List<String> messagesBody = Arrays.asList("MessageBody");
        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messagesBody, mapMessageAttributes)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
    }

    /**
     * Test sendInternal input with SQSObjectMessage
     */
    @Test
    public void testSendInternalSQSObjectMessage() throws JMSException {

        HashSet<String> set1 = new HashSet<String>();
        set1.add("data1");
        HashSet<String> set2 = new HashSet<String>();
        set2.add("data2");

        SQSObjectMessage msg = spy(new SQSObjectMessage(set1));
        String megBody1 = msg.getMessageBody();

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("object");

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_1))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_2));

        producer.sendInternal(destination, msg);

        /*
         * Re send the message
         */
        msg.clearBody();
        msg.setObject(set2);
        String megBody2 = msg.getMessageBody();
        producer.sendInternal(destination, msg);
        
        ArgumentCaptor<SendMessageRequest> argumentCaptor = ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(amazonSQSClient, times(2)).sendMessage(argumentCaptor.capture());
        
        assertEquals(megBody1, argumentCaptor.getAllValues().get(0).getMessageBody());
        assertEquals(megBody2, argumentCaptor.getAllValues().get(1).getMessageBody());
        verify(msg, times(2)).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_2);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_2);
    }

    /**
     * Test sendInternal input with SQSObjectMessage
     */
    @Test
    public void testSendInternalSQSObjectMessageFromReceivedMessage() throws JMSException, IOException {

        /*
         * Set up non JMS sqs message
         */
        Map<String,MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();

        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.OBJECT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        // Encode an object to byte array
        Integer integer = new Integer("10");
        ByteArrayOutputStream array = new ByteArrayOutputStream(10);
        ObjectOutputStream oStream = new ObjectOutputStream(array);
        oStream.writeObject(integer);
        oStream.close();

        String messageBody = Base64.encodeAsString(array.toByteArray());
        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                        .withMessageAttributes(mapMessageAttributes)
                        .withAttributes(mapAttributes)
                        .withBody(messageBody);

        SQSObjectMessage msg = spy(new SQSObjectMessage(acknowledger, QUEUE_URL, message));

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("object");

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_1))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_2));

        producer.sendInternal(destination, msg);

        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, Arrays.asList(messageBody),
                messageAttributes)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
    }

    /**
     * Test sendInternal input with SQSByteMessage
     */
    @Test
    public void testSendInternalSQSByteMessage() throws JMSException {

        SQSBytesMessage msg = spy(new SQSBytesMessage());
        msg.writeByte((byte)0);
        msg.reset();

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("byte");

        String messageId = "MessageId";
        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_1))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_2));

        producer.sendInternal(destination, msg);

        /*
         * Re send the message
         */
        msg.clearBody();
        msg.writeInt(42);
        producer.sendInternal(destination, msg);

        List<String> messagesBody = Arrays.asList("AA==", "AAAAKg==");
        verify(amazonSQSClient, times(2)).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messagesBody,
                                                                                            messageAttributes)));

        verify(msg, times(2)).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_2);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_2);
    }

    /**
     * Test sendInternal input with SQSByteMessage
     */
    @Test
    public void testSendInternalSQSByteMessageFromReceivedMessage() throws JMSException, IOException {
        
        /*
         * Set up non JMS sqs message
         */
        Map<String,MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.BYTE_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        String messageBody = Base64.encodeAsString(byteArray);
        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                        .withMessageAttributes(mapMessageAttributes)
                        .withAttributes(mapAttributes)
                        .withBody(messageBody);

        SQSObjectMessage msg = spy(new SQSObjectMessage(acknowledger, QUEUE_URL, message));

        Map<String, MessageAttributeValue> messageAttributes = createMessageAttribute("object");

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_1))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID_2));

        producer.sendInternal(destination, msg);

        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, Arrays.asList(messageBody),
                messageAttributes)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID_1);
        verify(msg).setSQSMessageId(MESSAGE_ID_1);
    }

    /**
     * Test getQueue
     */
    @Test
    public void testGetQueue() throws JMSException {
        assertEquals(destination, producer.getQueue());
    }

    /**
     * Test send with non SQSDestination
     */
    @Test
    public void testSendNonSQSDestination() throws JMSException {

        Queue queue = mock(Queue.class);

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        try {
            producer.send(queue, msg);
            fail();
        } catch (InvalidDestinationException ide) {
            // expected
        }

        Destination destination = mock(Destination.class);
        try {
            producer.send(destination, msg);
            fail();
        } catch (InvalidDestinationException ide) {
            // expected
        }

        try {
            producer.send(null, msg);
            fail();
        } catch (InvalidDestinationException ide) {
            // expected
        }
    }

    /**
     * Test send when destination already specified
     */
    @Test
    public void testSendDestinationAlreadySpecified() throws JMSException {

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        try {
            producer.send(destination, msg);
            fail();
        } catch (UnsupportedOperationException ide) {
            // expected
        }

        verify(producer).checkIfDestinationAlreadySet();
    }

    /**
     * Test send with destination
     */
    @Test
    public void testSendWithDestination() throws JMSException {

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));
        producer = spy(new SQSMessageProducer(amazonSQSClient, sqsSession, null));

        doNothing()
                .when(producer).sendInternal(destination, msg);

        producer.send(destination, msg);

        verify(producer).checkIfDestinationAlreadySet();
        verify(producer).sendInternal(destination, msg);
    }

    /**
     * Test send API with unsupported feature are not used
     */
    @Test
    public void testSendDropUnsupportedFeatures() throws JMSException {

        int deliveryMode = 1;
        int priority = 1;
        long timeToLive = 1;
        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        doNothing()
                .when(producer).send(destination, msg);
        doNothing()
                .when(producer).send(msg);

        producer.send(destination, msg, deliveryMode, priority, timeToLive);
        producer.send(msg, deliveryMode, priority, timeToLive);

        verify(producer).send(destination, msg);
        verify(producer).send(msg);
    }

    /**
     * Test send without destination
     */
    @Test
    public void testSendWithoutDestination() throws JMSException {

        SQSTextMessage msg = spy(new SQSTextMessage("MyText"));

        doNothing()
                .when(producer).sendInternal(destination, msg);

        producer.send(msg);
        verify(producer).sendInternal(destination, msg);
    }

    /**
     * Test Close when producer is already closed
     */
    @Test
    public void testCloseAlreadyClosed() throws JMSException {

        producer.isClosed().set(true);

        producer.close();

        verify(sqsSession, never()).removeProducer(producer);
    }

    /**
     * Test Close
     */
    @Test
    public void testClose() throws JMSException {

        producer.close();

        verify(sqsSession).removeProducer(producer);
    }

    private Map<String, MessageAttributeValue> createMessageAttribute(String type) {
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setDataType("String");
        messageAttributeValue.setStringValue(type);

        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
        messageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        return messageAttributes;
    }

    private class sendMessageRequestMatcher extends ArgumentMatcher<SendMessageRequest> {

        private String queueUrl;
        private List<String> messagesBody;
        private Map<String, MessageAttributeValue> messageAttributes;

        private sendMessageRequestMatcher(String queueUrl, List<String> messagesBody,
                                          Map<String, MessageAttributeValue> messageAttributes) {
            this.queueUrl = queueUrl;
            this.messagesBody = messagesBody;
            this.messageAttributes = messageAttributes;
        }

        @Override
        public boolean matches(Object argument) {

            if (!(argument instanceof SendMessageRequest)) {
                return false;
            }

            SendMessageRequest reqeust = (SendMessageRequest)argument;
            assertEquals(queueUrl, reqeust.getQueueUrl());
            assertTrue(messagesBody.contains(reqeust.getMessageBody()));
            assertEquals(messageAttributes , reqeust.getMessageAttributes());
            return true;
        }
    }
}