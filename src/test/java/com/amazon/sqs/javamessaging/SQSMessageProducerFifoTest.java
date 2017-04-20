/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.sqs.javamessaging.SQSMessageProducer;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.util.Base64;

import javax.jms.JMSException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageProducerTest class
 */
public class SQSMessageProducerFifoTest {

    public static final String QUEUE_URL = "QueueUrl.fifo";
    public static final String QUEUE_NAME = "QueueName.fifo";
    public static final String MESSAGE_ID = "MessageId";
    public static final String SEQ_NUMBER = "10010101231012312354534";
    public static final String SEQ_NUMBER_2 = "10010101231012312354535";
    public static final String GROUP_ID = "G1";
    public static final String DEDUP_ID = "D1";

    private SQSMessageProducer producer;
    private SQSQueueDestination destination;
    private SQSSession sqsSession;
    private AmazonSQSMessagingClientWrapper amazonSQSClient;
    private Acknowledger acknowledger;

    @Before
    public void setup() throws JMSException {

        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);

        acknowledger = mock(Acknowledger.class);

        sqsSession = mock(SQSSession.class);
        destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        producer = spy(new SQSMessageProducer(amazonSQSClient, sqsSession, destination));
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
        SQSMessage sqsByte = new SQSBytesMessage();
        Map<String, MessageAttributeValue> messageAttributeByte = producer.propertyToMessageAttribute(sqsByte);

        assertEquals(0, messageAttributeByte.size());
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
     * Test sendInternal input with SQSTextMessage
     */
    @Test
    public void testSendInternalSQSTextMessage() throws JMSException {

        String messageBody = "MyText1";
        SQSTextMessage msg = spy(new SQSTextMessage(messageBody));
        msg.setStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID, GROUP_ID);
        msg.setStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID, DEDUP_ID);

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER));

        producer.sendInternal(destination, msg);

        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.TEXT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        verify(msg).setSQSMessageId(MESSAGE_ID);
        verify(msg).setSequenceNumber(SEQ_NUMBER);
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
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, GROUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, DEDUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, SEQ_NUMBER);

        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                            .withMessageAttributes(mapMessageAttributes)
                            .withAttributes(mapAttributes)
                            .withBody("MessageBody");

        SQSTextMessage msg = spy(new SQSTextMessage(acknowledger, QUEUE_URL, message));

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER_2));

        producer.sendInternal(destination, msg);

        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, "MessageBody", SQSMessage.TEXT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        verify(msg).setSQSMessageId(MESSAGE_ID);
        verify(msg).setSequenceNumber(SEQ_NUMBER_2);
    }

    /**
     * Test sendInternal input with SQSObjectMessage
     */
    @Test
    public void testSendInternalSQSObjectMessage() throws JMSException {

        HashSet<String> set = new HashSet<String>();
        set.add("data1");

        SQSObjectMessage msg = spy(new SQSObjectMessage(set));
        msg.setStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID, GROUP_ID);
        msg.setStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID, DEDUP_ID);
        String msgBody = msg.getMessageBody();

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER));

        producer.sendInternal(destination, msg);

        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, msgBody, SQSMessage.OBJECT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        verify(msg).setSQSMessageId(MESSAGE_ID);
        verify(msg).setSequenceNumber(SEQ_NUMBER);
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
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, GROUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, DEDUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, SEQ_NUMBER);

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

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER_2));

        producer.sendInternal(destination, msg);

        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.OBJECT_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        verify(msg).setSQSMessageId(MESSAGE_ID);
        verify(msg).setSequenceNumber(SEQ_NUMBER_2);
    }

    /**
     * Test sendInternal input with SQSByteMessage
     */
    @Test
    public void testSendInternalSQSByteMessage() throws JMSException {

        SQSBytesMessage msg = spy(new SQSBytesMessage());
        msg.setStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID, GROUP_ID);
        msg.setStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID, DEDUP_ID);
        msg.writeByte((byte)0);
        msg.reset();

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER));

        producer.sendInternal(destination, msg);

        String messageBody = "AA==";
        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.BYTE_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));

        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        verify(msg).setSQSMessageId(MESSAGE_ID);
        verify(msg).setSequenceNumber(SEQ_NUMBER);
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
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, GROUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, DEDUP_ID);
        mapAttributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, SEQ_NUMBER);

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        String messageBody = Base64.encodeAsString(byteArray);
        com.amazonaws.services.sqs.model.Message message =
                new com.amazonaws.services.sqs.model.Message()
                        .withMessageAttributes(mapMessageAttributes)
                        .withAttributes(mapAttributes)
                        .withBody(messageBody);

        SQSBytesMessage msg = spy(new SQSBytesMessage(acknowledger, QUEUE_URL, message));

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(new SendMessageResult().withMessageId(MESSAGE_ID).withSequenceNumber(SEQ_NUMBER_2));

        producer.sendInternal(destination, msg);

        verify(amazonSQSClient).sendMessage(argThat(new sendMessageRequestMatcher(QUEUE_URL, messageBody, SQSMessage.BYTE_MESSAGE_TYPE, GROUP_ID, DEDUP_ID)));
        verify(msg).setJMSDestination(destination);
        verify(msg).setJMSMessageID("ID:" + MESSAGE_ID);
        verify(msg).setSQSMessageId(MESSAGE_ID);
        verify(msg).setSequenceNumber(SEQ_NUMBER_2);
    }

    private class sendMessageRequestMatcher extends ArgumentMatcher<SendMessageRequest> {

        private String queueUrl;
        private String messagesBody;
        private String messageType;
        private String groupId;
        private String deduplicationId;

        private sendMessageRequestMatcher(String queueUrl, String messagesBody, String messageType, String groupId, String deduplicationId) {
            this.queueUrl = queueUrl;
            this.messagesBody = messagesBody;
            this.messageType = messageType;
            this.groupId = groupId;
            this.deduplicationId = deduplicationId;
        }

        @Override
        public boolean matches(Object argument) {

            if (!(argument instanceof SendMessageRequest)) {
                return false;
            }

            SendMessageRequest request = (SendMessageRequest)argument;
            assertEquals(queueUrl, request.getQueueUrl());
            assertEquals(messagesBody, request.getMessageBody());
            String messageType = request.getMessageAttributes().get(SQSMessage.JMS_SQS_MESSAGE_TYPE).getStringValue();
            assertEquals(this.messageType, messageType);
            assertEquals(this.groupId, request.getMessageGroupId());
            assertEquals(this.deduplicationId, request.getMessageDeduplicationId());
            return true;
        }
    }
}