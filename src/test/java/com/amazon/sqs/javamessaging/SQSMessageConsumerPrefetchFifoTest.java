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

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.util.Base64;
import jakarta.jms.JMSException;
import jakarta.jms.ObjectMessage;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageConsumerPrefetchTest class
 */
public class SQSMessageConsumerPrefetchFifoTest {
    private static final String NAMESPACE = "123456789012";
    private static final String QUEUE_NAME = "QueueName.fifo";
    private static final  String QUEUE_URL = NAMESPACE + "/" + QUEUE_NAME;

    private NegativeAcknowledger negativeAcknowledger;
    private SQSMessageConsumerPrefetch consumerPrefetch;

    private AmazonSQSMessagingClientWrapper amazonSQSClient;

    public void init(int numberOfMessagesToPrefetch) {
        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);

        SQSConnection parentSQSConnection = mock(SQSConnection.class);
        when(parentSQSConnection.getWrappedAmazonSQSClient()).thenReturn(amazonSQSClient);

        SQSSessionCallbackScheduler sqsSessionRunnable = mock(SQSSessionCallbackScheduler.class);
        Acknowledger acknowledger = mock(Acknowledger.class);
        negativeAcknowledger = mock(NegativeAcknowledger.class);
        ExponentialBackoffStrategy backoffStrategy = mock(ExponentialBackoffStrategy.class);
        SQSQueueDestination sqsDestination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        consumerPrefetch =
                spy(new SQSMessageConsumerPrefetch(sqsSessionRunnable, acknowledger, negativeAcknowledger,
                        sqsDestination, amazonSQSClient, numberOfMessagesToPrefetch));

        consumerPrefetch.backoffStrategy = backoffStrategy;
    }

    /**
     * Test one full prefetch operation works as expected
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testOneFullPrefetch(int numberOfMessagesToPrefetch) throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);

        // Set up consumer prefetch and mocks
        final int numMessages = numberOfMessagesToPrefetch > 0 ? numberOfMessagesToPrefetch : 1;
        List<com.amazonaws.services.sqs.model.Message> messages = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            messages.add(createValidFifoMessage(i, "G" + i));
        }

        // First start the consumer prefetch
        consumerPrefetch.start();

        // Mock SQS call for receive message and return messages
        final int receiveMessageLimit = Math.min(10, numMessages);
        when(amazonSQSClient.receiveMessage(argThat(new BaseMatcher<>() {

            @Override
            public void describeTo(Description description) {

            }
            @Override
            public boolean matches(Object argument) {
                if (!(argument instanceof ReceiveMessageRequest other))
                    return false;
                return other.getQueueUrl().equals(QUEUE_URL)
                        && other.getMaxNumberOfMessages() == receiveMessageLimit
                        && other.getMessageAttributeNames().size() == 1
                        && other.getMessageAttributeNames().get(0).equals(SQSMessageConsumerPrefetch.ALL)
                        && other.getWaitTimeSeconds() == SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS
                        && other.getReceiveRequestAttemptId() != null
                        && other.getReceiveRequestAttemptId().length() > 0;
            }
        })))
                .thenReturn(new ReceiveMessageResult().withMessages(messages));

        // Mock isClosed and exit after a single prefetch loop
        when(consumerPrefetch.isClosed())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        // Request a message (only relevant when prefetching is off).
        consumerPrefetch.requestMessage();

        // Run the prefetch
        consumerPrefetch.run();

        // Ensure Consumer was started
        verify(consumerPrefetch).waitForStart();

        // Ensure Consumer Prefetch backlog is not full
        verify(consumerPrefetch).waitForPrefetch();

        // Ensure no message was nack
        verify(negativeAcknowledger).action(QUEUE_URL, new ArrayList<>());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);

        // Ensure message queue was filled with expected messages
        assertEquals(numMessages, consumerPrefetch.messageQueue.size());
        int index = 0;
        for (SQSMessageConsumerPrefetch.MessageManager messageManager : consumerPrefetch.messageQueue) {
            com.amazonaws.services.sqs.model.Message mockedMessage = messages.get(index);
            SQSMessage sqsMessage = (SQSMessage)messageManager.message();
            assertEquals(
                    mockedMessage.getReceiptHandle(),
                    sqsMessage.getReceiptHandle(),
                    "Receipt handle is the same"
            );
            assertEquals(
                    mockedMessage.getAttributes().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
                    sqsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID),
                    "Group id is the same"
            );
            assertEquals(
                    mockedMessage.getAttributes().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
                    sqsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER),
                    "Sequence number is the same"
            );
            assertEquals(
                    mockedMessage.getAttributes().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
                    sqsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID),
                    "Deduplication id is the same"
            );
            
            index++;
        }
    }

    /**
     * Test ConvertToJMSMessage when message type is not set in the message attribute
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageNoTypeAttribute(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);

        // Set up consumer prefetch and mocks
        com.amazonaws.services.sqs.model.Message message = createValidFifoMessage(1, "G");

        // Return message attribute with no message type attribute
        message.setBody("MessageBody");

        // Convert the SQS message to JMS Message
        jakarta.jms.Message jmsMessage = consumerPrefetch.convertToJMSMessage(message);

        // Verify results
        assertTrue(jmsMessage instanceof SQSTextMessage);
        assertEquals(((SQSTextMessage) jmsMessage).getText(), "MessageBody");
        assertEquals(
                message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID)
        );
        assertEquals(
                message.getAttributes().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER)
        );
        assertEquals(
                message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID)
        );
    }

    /**
     * Test ConvertToJMSMessage with byte message type
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageByteTypeAttribute(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);

        // Set up consumer prefetch and mocks
        com.amazonaws.services.sqs.model.Message message = createValidFifoMessage(1, "G");

        // Return message attributes with message type 'BYTE'
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.BYTE_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        message.getMessageAttributes().put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        message.setBody(Base64.encodeAsString(byteArray));

        //  Convert the SQS message to JMS Message
        jakarta.jms.Message jmsMessage = consumerPrefetch.convertToJMSMessage(message);

        // Verify results
        assertTrue(jmsMessage instanceof SQSBytesMessage);
        for (byte b : byteArray) {
            assertEquals(b, ((SQSBytesMessage)jmsMessage).readByte());
        }
        assertEquals(
                message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID)
        );
        assertEquals(
                message.getAttributes().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER)
        );
        assertEquals(
                message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID)
        );
    }

    /**
     * Test ConvertToJMSMessage with byte message that contains illegal sqs message body
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageByteTypeIllegalBody(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);

        // Set up consumer prefetch and mocks
        com.amazonaws.services.sqs.model.Message message = createValidFifoMessage(1, "G");

        // Return message attributes with message type 'BYTE'
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.BYTE_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        message.getMessageAttributes().put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        // Return illegal message body for byte message type
        message.setBody("Text Message");

        // Convert the SQS message to JMS Message
        assertThrows(JMSException.class, () -> consumerPrefetch.convertToJMSMessage(message));
    }

    /**
     * Test ConvertToJMSMessage with an object message
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageObjectTypeAttribute(int numberOfMessagesToPrefetch) throws JMSException, IOException {
        init(numberOfMessagesToPrefetch);

        // Set up consumer prefetch and mocks
        com.amazonaws.services.sqs.model.Message message = createValidFifoMessage(1, "G");
        // Return message attributes with message type 'OBJECT'
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.OBJECT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        message.getMessageAttributes().put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        // Encode an object to byte array
        Integer integer = 10;
        ByteArrayOutputStream array = new ByteArrayOutputStream(10);
        ObjectOutputStream oStream = new ObjectOutputStream(array);
        oStream.writeObject(integer);
        oStream.close();
        
        message.setBody(Base64.encodeAsString(array.toByteArray()));

        // Convert the SQS message to JMS Message
        jakarta.jms.Message jmsMessage = consumerPrefetch.convertToJMSMessage(message);

        // Verify results
        assertTrue(jmsMessage instanceof SQSObjectMessage);
        assertEquals(integer, ((SQSObjectMessage) jmsMessage).getObject());
        assertEquals(message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID), jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID));
        assertEquals(message.getAttributes().get(SQSMessagingClientConstants.SEQUENCE_NUMBER), jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER));
        assertEquals(message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID), jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID));
    }

    /**
     * Test ConvertToJMSMessage with an object message that contains illegal sqs message body
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageObjectIllegalBody(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);

        // Set up consumer prefetch and mocks
        com.amazonaws.services.sqs.model.Message message = createValidFifoMessage(1, "G");
        // Return message attributes with message type 'OBJECT'
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.OBJECT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        message.getMessageAttributes().put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        message.setBody("Some text that does not represent an object");

        // Convert the SQS message to JMS Message
        ObjectMessage jmsMessage = (ObjectMessage) consumerPrefetch.convertToJMSMessage(message);

        // Verify results
        assertThrows(JMSException.class, jmsMessage::getObject);
    }

    /**
     * Test ConvertToJMSMessage with text message with text type attribute
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageTextTypeAttribute(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);

        // Set up consumer prefetch and mocks
        com.amazonaws.services.sqs.model.Message message = createValidFifoMessage(1, "G");

        // Return message attributes with message type 'TEXT'
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.TEXT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        message.getMessageAttributes().put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        message.setBody("MessageBody");

        // Convert the SQS message to JMS Message
        jakarta.jms.Message jmsMessage = consumerPrefetch.convertToJMSMessage(message);

        // Verify results
        assertTrue(jmsMessage instanceof SQSTextMessage);
        assertEquals(message.getBody(), "MessageBody");
        assertEquals(message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID));
        assertEquals(message.getAttributes().get(SQSMessagingClientConstants.SEQUENCE_NUMBER),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER));
        assertEquals(message.getAttributes().get(SQSMessagingClientConstants.MESSAGE_GROUP_ID),
                jmsMessage.getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID));
    }

    /**
     * Utility functions
     */
    private com.amazonaws.services.sqs.model.Message createValidFifoMessage(int messageNumber, String groupId) {
        Map<String,String> mapAttributes = Map.of(
            SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1",
            SQSMessagingClientConstants.SEQUENCE_NUMBER, "10000000000000000000" + messageNumber,
            SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, "d" + messageNumber,
            SQSMessagingClientConstants.MESSAGE_GROUP_ID, groupId);
        
        return new com.amazonaws.services.sqs.model.Message()
            .withReceiptHandle("r" + messageNumber)
            .withAttributes(mapAttributes);
    }

    /**
     * MethodSource
     */
    private static Stream<Arguments> prefetchParameters() {
        return Stream.of(
                Arguments.of(0),
                Arguments.of(1),
                Arguments.of(5),
                Arguments.of(10),
                Arguments.of(15)
        );
    }
}
