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

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.utils.BinaryUtils;

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
        sqsText.setObjectProperty(objectProperty, Integer.valueOf(8));

        MessageAttributeValue messageAttributeValueBoolean = MessageAttributeValue.builder()
        		.dataType("Number.Boolean")
        		.stringValue("1")
        		.build();

        MessageAttributeValue messageAttributeValueByte = MessageAttributeValue.builder()
        		.dataType("Number.byte")
        		.stringValue("1")
        		.build();

        MessageAttributeValue messageAttributeValueShort = MessageAttributeValue.builder()
        		.dataType("Number.short")
        		.stringValue("2")
        		.build();
        
        MessageAttributeValue messageAttributeValueInt = MessageAttributeValue.builder()
        		.dataType("Number.int")
        		.stringValue("3")
        		.build();
        
        MessageAttributeValue messageAttributeValueLong = MessageAttributeValue.builder()
        		.dataType("Number.long")
        		.stringValue("4")
        		.build();
        
        MessageAttributeValue messageAttributeValueFloat = MessageAttributeValue.builder()
        		.dataType("Number.float")
        		.stringValue("5.0")
        		.build();

        MessageAttributeValue messageAttributeValueDouble = MessageAttributeValue.builder()
        		.dataType("Number.double")
        		.stringValue("6.0")
        		.build();
        
        MessageAttributeValue messageAttributeValueString = MessageAttributeValue.builder()
        		.dataType("String")
        		.stringValue("seven")
        		.build();

        MessageAttributeValue messageAttributeValueObject = MessageAttributeValue.builder()
        		.dataType("Number.int")
        		.stringValue("8")
        		.build();

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
                .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID).sequenceNumber(SEQ_NUMBER).build());

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
        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
	        .stringValue(SQSMessage.TEXT_MESSAGE_TYPE)
	        .dataType(SQSMessagingClientConstants.STRING)
	        .build();
	        
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.MESSAGE_GROUP_ID), GROUP_ID);
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID), DEDUP_ID);
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.SEQUENCE_NUMBER), SEQ_NUMBER);

        Message message = Message.builder()
                            .messageAttributes(mapMessageAttributes)
                            .attributes(mapAttributes)
                            .body("MessageBody")
                            .build();

        SQSTextMessage msg = spy(new SQSTextMessage(acknowledger, QUEUE_URL, message));

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID).sequenceNumber(SEQ_NUMBER_2).build());

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
                .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID).sequenceNumber(SEQ_NUMBER).build());

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

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
	        .stringValue(SQSMessage.OBJECT_MESSAGE_TYPE)
	       	.dataType(SQSMessagingClientConstants.STRING)
	       	.build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.MESSAGE_GROUP_ID), GROUP_ID);
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID), DEDUP_ID);
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.SEQUENCE_NUMBER), SEQ_NUMBER);

        // Encode an object to byte array
        Integer integer = Integer.valueOf("10");
        ByteArrayOutputStream array = new ByteArrayOutputStream(10);
        ObjectOutputStream oStream = new ObjectOutputStream(array);
        oStream.writeObject(integer);
        oStream.close();

        String messageBody = BinaryUtils.toBase64(array.toByteArray());
        Message message = Message.builder()
	                        .messageAttributes(mapMessageAttributes)
	                        .attributes(mapAttributes)
	                        .body(messageBody)
	                        .build();

        SQSObjectMessage msg = spy(new SQSObjectMessage(acknowledger, QUEUE_URL, message));

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID).sequenceNumber(SEQ_NUMBER_2).build());

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
                .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID).sequenceNumber(SEQ_NUMBER).build());

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
        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
	        .stringValue(SQSMessage.BYTE_MESSAGE_TYPE)
	        .dataType(SQSMessagingClientConstants.STRING)
	        .build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.MESSAGE_GROUP_ID), GROUP_ID);
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID), DEDUP_ID);
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.SEQUENCE_NUMBER), SEQ_NUMBER);

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        String messageBody = BinaryUtils.toBase64(byteArray);
        Message message = Message.builder()
	                        .messageAttributes(mapMessageAttributes)
	                        .attributes(mapAttributes)
	                        .body(messageBody)
	                        .build();

        SQSBytesMessage msg = spy(new SQSBytesMessage(acknowledger, QUEUE_URL, message));

        when(amazonSQSClient.sendMessage(any(SendMessageRequest.class)))
                .thenReturn(SendMessageResponse.builder().messageId(MESSAGE_ID).sequenceNumber(SEQ_NUMBER_2).build());

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
            assertEquals(queueUrl, request.queueUrl());
            assertEquals(messagesBody, request.messageBody());
            String messageType = request.messageAttributes().get(SQSMessage.JMS_SQS_MESSAGE_TYPE).stringValue();
            assertEquals(this.messageType, messageType);
            assertEquals(this.groupId, request.messageGroupId());
            assertEquals(this.deduplicationId, request.messageDeduplicationId());
            return true;
        }
    }
}
