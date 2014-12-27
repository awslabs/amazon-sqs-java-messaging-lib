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
import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.SQSSessionCallbackScheduler;
import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch.MessageManager;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;
import com.amazonaws.util.Base64;
import com.amazonaws.services.sqs.model.*;

import javax.jms.*;
import javax.jms.Message;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageConsumerPrefetchTest class
 */
@SuppressWarnings("unchecked")
public class SQSMessageConsumerPrefetchTest {

    private static final String NAMESPACE = "123456789012";
    private static final String QUEUE_NAME = "QueueName";
    private static final  String QUEUE_URL = NAMESPACE + "/" + QUEUE_NAME;
    private static final int NUMBER_OF_MESSAGES_TO_PREFETCH = 10;

    private Acknowledger acknowledger;
    private NegativeAcknowledger negativeAcknowledger;
    private SQSSessionCallbackScheduler sqsSessionRunnable;
    private SQSMessageConsumerPrefetch consumerPrefetch;
    private ExponentialBackoffStrategy backoffStrategy;

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private AmazonSQSMessagingClientWrapper amazonSQSClient;

    @Before
    public void setup() {

        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);

        SQSConnection parentSQSConnection = mock(SQSConnection.class);
        when(parentSQSConnection.getWrappedAmazonSQSClient()).thenReturn(amazonSQSClient);

        sqsSessionRunnable = mock(SQSSessionCallbackScheduler.class);

        acknowledger = mock(Acknowledger.class);

        negativeAcknowledger = mock(NegativeAcknowledger.class);

        backoffStrategy = mock(ExponentialBackoffStrategy.class);

        SQSQueueDestination sqsDestination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        consumerPrefetch =
                spy(new SQSMessageConsumerPrefetch(sqsSessionRunnable, acknowledger, negativeAcknowledger,
                        sqsDestination, amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH));

        consumerPrefetch.backoffStrategy = backoffStrategy;
    }

    /**
     * Test one full prefetch operation works as expected
     */
    @Test
    public void testEndToEnd() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */

        // First start the consumer prefetch
        consumerPrefetch.start();

        // Create messages return from SQS
        List<String> receipt = new ArrayList<String>();
        for (int i = 0; i < 10; ++i) {
            receipt.add("r" + i);
        }
        ReceiveMessageResult receivedMessageResult = createReceiveMessageResult(receipt);

        // Mock SQS call for receive message and return messages
        when(amazonSQSClient.receiveMessage(
                eq(new ReceiveMessageRequest(QUEUE_URL)
                        .withMaxNumberOfMessages(10)
                        .withAttributeNames(SQSMessageConsumerPrefetch.ALL)
                        .withMessageAttributeNames(SQSMessageConsumerPrefetch.ALL)
                        .withWaitTimeSeconds(SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS))))
                .thenReturn(receivedMessageResult);

        // Mock isClosed and exit after a single prefetch loop
        when(consumerPrefetch.isClosed())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        /*
         * Run the prefetch
         */
        consumerPrefetch.run();

        /*
         * Verify the results
         */

        // Ensure Consumer was started
        verify(consumerPrefetch).waitForStart();

        // Ensure Consumer Prefetch backlog is not full
        verify(consumerPrefetch).waitForPrefetch();

        // Ensure no message was nack
        verify(negativeAcknowledger).action(QUEUE_URL, new ArrayList<String>());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);

        // Ensure message queue was filled with expected messages
        assertEquals(10, consumerPrefetch.messageQueue.size());
        for (SQSMessageConsumerPrefetch.MessageManager messageManager : consumerPrefetch.messageQueue) {
            SQSMessage sqsMessage = (SQSMessage)messageManager.getMessage();
            assertTrue(receipt.contains(sqsMessage.getReceiptHandle()));
        }
    }

    /**
     * Test that a get message is not called when consumer is closed while waiting for prefetch
     */
    @Test
    public void testStopWhenConsumerClosedDuringWaitForPrefetch() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */

        // First start the consumer prefetch
        consumerPrefetch.running = true;

        // Mock isClosed and exit after a single prefetch loop
        when(consumerPrefetch.isClosed())
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(true);

        /*
         * Run the prefetch
         */
        consumerPrefetch.run();

        /*
         * Verify the results
         */

        // Ensure Consumer was started
        verify(consumerPrefetch).waitForStart();

        // Ensure Consumer Prefetch backlog is not full
        verify(consumerPrefetch).waitForPrefetch();

        // Ensure Consumer Prefetch nack any messages when closed
        verify(consumerPrefetch, times(2)).nackQueueMessages();

        // Ensure we do not get messages when closed while waiting for prefetch
        verify(consumerPrefetch, never()).getMessages(anyInt());

        // Ensure we do not process any messages
        verify(consumerPrefetch, never()).processReceivedMessages(any(List.class));

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Interrupt in waitForStart
     */
    @Test
    public void testStopAfterInterruptWaitForStart() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doThrow(new InterruptedException("Interrupt"))
                .when(consumerPrefetch).waitForStart();

        /*
         * Run the prefetch
         */
        consumerPrefetch.run();

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).waitForPrefetch();
        verify(consumerPrefetch, never()).getMessages(anyInt());
        verify(consumerPrefetch, never()).processReceivedMessages(any(List.class));

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Error in waitForStart
     */
    @Test
    public void testStopAfterErrorWaitForStart() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doThrow(new Error("error"))
                .when(consumerPrefetch).waitForStart();

        /*
         * Run the prefetch
         */
        try {
            consumerPrefetch.run();
            fail("expect error");
        } catch (Error e) {
            // Expected error
        }

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).waitForPrefetch();
        verify(consumerPrefetch, never()).getMessages(anyInt());
        verify(consumerPrefetch, never()).processReceivedMessages(any(List.class));

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Interrupt in waitForPrefetch
     */
    @Test
    public void testStopAfterInterruptWaitForPrefetch() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doNothing()
                .when(consumerPrefetch).waitForStart();
        doThrow(new InterruptedException("Interrupt"))
                .when(consumerPrefetch).waitForPrefetch();

        /*
         * Run the prefetch
         */
        consumerPrefetch.run();

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).getMessages(anyInt());
        verify(consumerPrefetch, never()).processReceivedMessages(any(List.class));

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Error in waitForPrefetch
     */
    @Test
    public void testStopAfterErrorWaitForPrefetch() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doNothing()
                .when(consumerPrefetch).waitForStart();
        doThrow(new Error("error"))
                .when(consumerPrefetch).waitForPrefetch();

        /*
         * Run the prefetch
         */
        try {
            consumerPrefetch.run();
            fail("expect error");
        } catch (Error e) {
            // Expected error
        }

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).getMessages(anyInt());
        verify(consumerPrefetch, never()).processReceivedMessages(any(List.class));

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Interrupt in getMessages
     */
    @Test
    public void testStopAfterInterruptGetMessages() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doNothing()
                .when(consumerPrefetch).waitForStart();
        doNothing()
                .when(consumerPrefetch).waitForPrefetch();
        doThrow(new InterruptedException("Interrupt"))
                .when(consumerPrefetch).getMessages(anyInt());

        /*
         * Run the prefetch
         */
        consumerPrefetch.run();

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();
        verify(consumerPrefetch).getMessages(anyInt());

        verify(consumerPrefetch, never()).processReceivedMessages(any(List.class));

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Error in waitForPrefetch
     */
    @Test
    public void testStopAfterErrorGetMessages() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doNothing()
                .when(consumerPrefetch).waitForStart();
        doNothing()
                .when(consumerPrefetch).waitForPrefetch();
        doThrow(new Error("error"))
                .when(consumerPrefetch).getMessages(anyInt());

        /*
         * Run the prefetch
         */
        try {
            consumerPrefetch.run();
            fail("expect error");
        } catch (Error e) {
            // Expected error
        }

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();
        verify(consumerPrefetch).getMessages(anyInt());

        verify(consumerPrefetch, never()).processReceivedMessages(any(List.class));

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test Run when consumer is closed
     */
    @Test
    public void testRunExitOnClose() {

        consumerPrefetch.close();

        /*
         * Run the prefetch
         */
        consumerPrefetch.run();

        verifyNoMoreInteractions(amazonSQSClient);
    }

    /**
     * Test SetMessageListener to Null
     */
    @Test
    public void testSetNullMessageListener() {

        consumerPrefetch.setMessageListener(null);
        verifyNoMoreInteractions(sqsSessionRunnable);
    }

    /**
     * Test SetMessageListener when message were prefetched
     */
    @Test
    public void testSetMessageListener() {

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        Message message1 = mock(Message.class);
        when(msgManager1.getMessage())
                .thenReturn(message1);

        SQSMessageConsumerPrefetch.MessageManager msgManager2 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        Message message2 = mock(Message.class);
        when(msgManager2.getMessage())
                .thenReturn(message2);

        consumerPrefetch.messageQueue.add(msgManager1);
        consumerPrefetch.messageQueue.add(msgManager2);

        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.running = true;
        consumerPrefetch.setMessageListener(msgListener);
              
        assertTrue(consumerPrefetch.messageQueue.isEmpty());

        verify(sqsSessionRunnable).scheduleCallBack(msgListener, msgManager1);
        verify(sqsSessionRunnable).scheduleCallBack(msgListener, msgManager2);

        verifyNoMoreInteractions(sqsSessionRunnable);
    }

    /**
     * Test getting message listener
     */
    @Test
    public void testGetMessageListener() {

        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);

        assertEquals(msgListener, consumerPrefetch.getMessageListener());
    }

    /**
     * Test WaitForStart when preftech already started
     */
    @Test
    public void testWaitForStartCurrentStateStart() throws javax.jms.IllegalStateException, InterruptedException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.running = true;
        final CountDownLatch passedWaitForStartCall = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    consumerPrefetch.waitForStart();

                    // Indicate that we no longer waiting
                    passedWaitForStartCall.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        /*
         * verify result
         */
        assertEquals(true, passedWaitForStartCall.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForStart when preftech already closed
     */
    @Test
    public void testWaitForStartCurrentStateClose() throws javax.jms.IllegalStateException, InterruptedException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.closed = true;
        final CountDownLatch passedWaitForStartCall = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    consumerPrefetch.waitForStart();

                    // Indicate that we no longer waiting
                    passedWaitForStartCall.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        /*
         * verify result
         */
        assertEquals(true, passedWaitForStartCall.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForStart when preftech state is updated to started while another thread is waiting
     * for the prefetch to start
     */
    @Test
    public void testWaitForStartUpdateStateToStart() throws javax.jms.IllegalStateException, InterruptedException {

        /*
         * Set up consumer prefetch and mocks
         */
        final CountDownLatch beforeWaitForStartCall = new CountDownLatch(1);
        final CountDownLatch passedWaitForStart = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeWaitForStartCall.countDown();
                    consumerPrefetch.waitForStart();
                    passedWaitForStart.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Yield execution to allow the consumer to wait
        assertEquals(true, beforeWaitForStartCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Update the state and notify
        consumerPrefetch.start();

        /*
         * verify result
         */

        // Ensure consumer is not waiting to move to start state
        assertEquals(true, passedWaitForStart.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForStart when waiting thread is interrupted
     */
    @Test
    public void testWaitForStartInterrupted() throws javax.jms.IllegalStateException, InterruptedException {

        /*
         * Set up consumer prefetch and mocks
         */
        final CountDownLatch beforeWaitForStartCall = new CountDownLatch(1);
        final CountDownLatch recvInterruptedExceptionLatch = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeWaitForStartCall.countDown();
                    consumerPrefetch.waitForStart();
                } catch (InterruptedException e) {
                    recvInterruptedExceptionLatch.countDown();
                    e.printStackTrace();
                }
            }
        });

        t.start();

        // Yield execution to allow the consumer to wait
        assertEquals(true, beforeWaitForStartCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Interrupt waiting thread
        t.interrupt();

        /*
         * verify result
         */
        assertEquals(true, recvInterruptedExceptionLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForPrefetch and ensure that message are not prefetch when limit has already reached
     */
    @Test
    public void testWaitForPrefetchLimitReached() throws InterruptedException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.messagesPrefetched = NUMBER_OF_MESSAGES_TO_PREFETCH + 5;
        final CountDownLatch beforeWaitForPrefetchCall = new CountDownLatch(1);
        final CountDownLatch passedWaitForPrefetch = new CountDownLatch(1);

        /*
         * call waitForPrefetch in different thread
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeWaitForPrefetchCall.countDown();
                    consumerPrefetch.waitForPrefetch();
                    passedWaitForPrefetch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Yield execution to allow the consumer to wait
        assertEquals(true, beforeWaitForPrefetchCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the local and ensure that we are still waiting since the prefetch message still equal to the limit
        consumerPrefetch.notifyStateChange();
        assertEquals(false, passedWaitForPrefetch.await(3, TimeUnit.SECONDS));

        // Simulate messages were processes
        consumerPrefetch.messagesPrefetched = NUMBER_OF_MESSAGES_TO_PREFETCH - 1;

        // Release the local and ensure that we no longer waiting since the prefetch message is below the limit
        consumerPrefetch.notifyStateChange();
        assertEquals(true, passedWaitForPrefetch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForPrefetch when prefetch consumer is closed
     */
    @Test
    public void testWaitForPrefetchIsClosed() throws InterruptedException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.messagesPrefetched = NUMBER_OF_MESSAGES_TO_PREFETCH + 5;
        consumerPrefetch.close();

        final CountDownLatch beforeWaitForPrefetchCall = new CountDownLatch(1);
        final CountDownLatch passedWaitForPrefetch = new CountDownLatch(1);

        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeWaitForPrefetchCall.countDown();
                    consumerPrefetch.waitForPrefetch();
                    passedWaitForPrefetch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Yield execution to allow the consumer to wait
        assertEquals(true, beforeWaitForPrefetchCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Validate we do not wait when the consumer is closed
        assertEquals(true, passedWaitForPrefetch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForPrefetch when waiting thread is interrupted
     */
    @Test
    public void testWaitForPrefetchInterrupted() throws InterruptedException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.messagesPrefetched = NUMBER_OF_MESSAGES_TO_PREFETCH + 5;
        final CountDownLatch beforeWaitForPrefetchCall = new CountDownLatch(1);
        final CountDownLatch recvInterruptedExceptionLatch = new CountDownLatch(1);

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeWaitForPrefetchCall.countDown();
                    consumerPrefetch.waitForPrefetch();
                } catch (InterruptedException e) {
                    recvInterruptedExceptionLatch.countDown();
                    e.printStackTrace();
                }
            }
        });

        t.start();

        assertEquals(true, beforeWaitForPrefetchCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        t.interrupt();

        // Validate that we no longer waiting due to the interrupt
        assertEquals(true, recvInterruptedExceptionLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test ConvertToJMSMessage when message type is not set in the message attribute
     */
    @Test
    public void testConvertToJMSMessageNoTypeAttribute() throws JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        com.amazonaws.services.sqs.model.Message message = mock(com.amazonaws.services.sqs.model.Message.class);

        // Return message attribute with no message type attribute
        when(message.getMessageAttributes())
                .thenReturn(new HashMap<String, MessageAttributeValue>());
        when(message.getAttributes())
                .thenReturn(mapAttributes);
        when(message.getBody())
                .thenReturn("MessageBody");

        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSTextMessage);
        assertEquals(((SQSTextMessage) jsmMessage).getText(), "MessageBody");
    }

    /**
     * Test ConvertToJMSMessage with byte message type
     */
    @Test
    public void testConvertToJMSMessageByteTypeAttribute() throws JMSException, IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Map<String,MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.BYTE_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        com.amazonaws.services.sqs.model.Message message = mock(com.amazonaws.services.sqs.model.Message.class);

        // Return message attributes with message type 'BYTE'
        when(message.getMessageAttributes()).thenReturn(mapMessageAttributes);
        when(message.getAttributes()).thenReturn(mapAttributes);

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        when(message.getBody()).thenReturn(Base64.encodeAsString(byteArray));

        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSBytesMessage);
        for (byte b : byteArray) {
            assertEquals(b, ((SQSBytesMessage)jsmMessage).readByte());
        }
    }

    /**
     * Test ConvertToJMSMessage with byte message that contains illegal sqs message body
     */
    @Test
    public void testConvertToJMSMessageByteTypeIllegalBody() throws JMSException, IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Map<String,MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();

        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.BYTE_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        com.amazonaws.services.sqs.model.Message message = mock(com.amazonaws.services.sqs.model.Message.class);

        // Return message attributes with message type 'BYTE'
        when(message.getMessageAttributes()).thenReturn(mapMessageAttributes);
        when(message.getAttributes()).thenReturn(mapAttributes);
        // Return illegal message body for byte message type
        when(message.getBody()).thenReturn("Text Message");

        /*
         * Convert the SQS message to JMS Message
         */
        try {
            consumerPrefetch.convertToJMSMessage(message);
            fail("Expect JMSException");
        } catch (JMSException jmse) {
            // Expected JMS exception
        }
    }

    /**
     * Test ConvertToJMSMessage with an object message
     */
    @Test
    public void testConvertToJMSMessageObjectTypeAttribute() throws JMSException, IOException {

        /*
         * Set up consumer prefetch and mocks
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

        com.amazonaws.services.sqs.model.Message message = mock(com.amazonaws.services.sqs.model.Message.class);
        // Return message attributes with message type 'OBJECT'
        when(message.getMessageAttributes()).thenReturn(mapMessageAttributes);
        when(message.getAttributes()).thenReturn(mapAttributes);
        when(message.getBody()).thenReturn(Base64.encodeAsString(array.toByteArray()));

        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSObjectMessage);
        assertEquals(integer, ((SQSObjectMessage) jsmMessage).getObject());
    }

    /**
     * Test ConvertToJMSMessage with an object message that contains illegal sqs message body
     */
    @Test
    public void testConvertToJMSMessageObjectIllegalBody() throws JMSException, IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Map<String,MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();

        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.OBJECT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        com.amazonaws.services.sqs.model.Message message = mock(com.amazonaws.services.sqs.model.Message.class);
        // Return message attributes with message type 'OBJECT'
        when(message.getMessageAttributes()).thenReturn(mapMessageAttributes);
        when(message.getAttributes()).thenReturn(mapAttributes);
        when(message.getBody()).thenReturn("Some text that does not represent an object");

        /*
         * Convert the SQS message to JMS Message
         */
        ObjectMessage jsmMessage = (ObjectMessage) consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        try {
            jsmMessage.getObject();
            fail("Expect JMSException");
        } catch (JMSException jmse) {
            // Expected JMS exception
        }
    }

    /**
     * Test ConvertToJMSMessage with text message with text type attribute
     */
    @Test
    public void testConvertToJMSMessageTextTypeAttribute() throws JMSException, IOException {

        /*
         * Set up consumer prefetch and mocks
         */

        Map<String,MessageAttributeValue> mapMessageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();
        messageAttributeValue.setStringValue(SQSMessage.TEXT_MESSAGE_TYPE);
        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        com.amazonaws.services.sqs.model.Message message = mock(com.amazonaws.services.sqs.model.Message.class);
        // Return message attributes with message type 'TEXT'
        when(message.getMessageAttributes()).thenReturn(mapMessageAttributes);
        when(message.getAttributes()).thenReturn(mapAttributes);
        when(message.getBody()).thenReturn("MessageBody");

        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSTextMessage);
        assertEquals(message.getBody(), "MessageBody");
    }

    /**
     * Test received messages when consumer prefetch has not started
     */
    @Test
    public void testReceiveWhenNotStarted() throws JMSException {

        consumerPrefetch.running = false;

        assertNull(consumerPrefetch.receive());
        assertNull(consumerPrefetch.receive(100));
        assertNull(consumerPrefetch.receiveNoWait());
    }

    /**
     * Test received messages when consumer prefetch has is closed
     */
    @Test
    public void testReceiveWhenClosed() throws JMSException {

        consumerPrefetch.closed = true;

        assertNull(consumerPrefetch.receive());
        assertNull(consumerPrefetch.receive(100));
        assertNull(consumerPrefetch.receiveNoWait());
    }

    /**
     * Test received messages
     */
    @Test
    public void testReceiveMessagePrefetch() throws JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.running = true;

        List<String> receiptHandlers = createReceiptHandlersList(20);

        addMessagesToQueue(receiptHandlers);

        /*
         * Call receive messages
         */
        SQSMessage msg = (SQSMessage)consumerPrefetch.receive();

        /*
         * Verify results
         */
        receiptHandlers.contains(msg.getReceiptHandle());
        verify(acknowledger).notifyMessageReceived(msg);
        verify(consumerPrefetch).notifyStateChange();
    }

    /**
     * Test received messages call wait for messages and exists when consumer prefterch is closed
     */
    @Test
    public void testReceiveMessageEmptyThenClosed() throws InterruptedException {

        /*
         * Set up consumer prefetch and lactches
         */
        consumerPrefetch.running = true;
        final CountDownLatch beforeReceiveCall = new CountDownLatch(1);
        final CountDownLatch passedReceiveCall = new CountDownLatch(1);
        final AtomicBoolean noMessageReturned = new AtomicBoolean(false);

        /*
         * Call receive messages
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeReceiveCall.countDown();
                    Message msg = consumerPrefetch.receive(0);
                    if (msg == null) {
                        noMessageReturned.set(true);
                    }
                    passedReceiveCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        assertEquals(true, beforeReceiveCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Update the state and notify
        consumerPrefetch.close();

        // Wait till receive execution finishes
        assertEquals(true, passedReceiveCall.await(10, TimeUnit.SECONDS));

        // Validate that after session is closed receive returns null
        assertEquals(true, noMessageReturned.get());
    }

    /**
     * Test received messages wait when no message are prefetch and return newly added message
     */
    @Test
    public void testReceiveMessageEmptyThenAddMessage() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and lactches
         */
        consumerPrefetch.running = true;

        final String receiptHandle = "r1";
        final CountDownLatch beforeReceiveCall = new CountDownLatch(1);
        final CountDownLatch passedReceiveCall = new CountDownLatch(1);
        final AtomicBoolean messageReceived = new AtomicBoolean(false);

        /*
         * Call receive messages
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeReceiveCall.countDown();
                    SQSMessage msg = (SQSMessage) consumerPrefetch.receive(0);
                    if ((msg != null) && (msg.getReceiptHandle().equals(receiptHandle))) {
                        messageReceived.set(true);
                    }
                    passedReceiveCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        assertEquals(true, beforeReceiveCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Add message to the internal queue
        List<String> receiptHandlers = new ArrayList<String>();
        receiptHandlers.add(receiptHandle);
        addMessagesToQueue(receiptHandlers);
        consumerPrefetch.notifyStateChange();

        // Wait till receive execution finishes
        assertEquals(true, passedReceiveCall.await(10, TimeUnit.SECONDS));

        // Validate that after adding a single message it was receive correctly
        assertEquals(true, messageReceived.get());
    }

    /**
     * Test received messages with timeout
     */
    @Test
    public void testReceiveMessageTimeout() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and time stamps
         */
        consumerPrefetch.running = true;

        long waitTime = TimeUnit.SECONDS.toMillis(5);
        long startTime = System.currentTimeMillis();

        /*
         * Call receive messages
         */
        SQSMessage msg = (SQSMessage) consumerPrefetch.receive(waitTime);

        assertNull(msg);

        // verify that we did not exist early
        assertTrue(System.currentTimeMillis() - startTime > waitTime);
    }

    /**
     * Test process received messages with empty input
     */
    @Test
    public void testProcessReceivedMessagesEmptyInput() {

        consumerPrefetch.processReceivedMessages(new ArrayList<com.amazonaws.services.sqs.model.Message>());
        verifyNoMoreInteractions(sqsSessionRunnable);
    }

    /**
     * Test process received messages
     */
    @Test
    public void testProcessReceivedMessages() throws JMSException {

        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        List<String> receiptHandlers = createReceiptHandlersList(3);
        List<com.amazonaws.services.sqs.model.Message> messages = createSQSServiceMessages(receiptHandlers);

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */
        assertEquals(receiptHandlers.size(), consumerPrefetch.messageQueue.size());
        assertEquals(receiptHandlers.size(), consumerPrefetch.messagesPrefetched);

        while (!consumerPrefetch.messageQueue.isEmpty()) {
            SQSMessageConsumerPrefetch.MessageManager msgManager = consumerPrefetch.messageQueue.pollFirst();
            SQSMessage msg = (SQSMessage)msgManager.getMessage();
            receiptHandlers.contains(msg.getReceiptHandle());
        }

        verify(negativeAcknowledger).action(QUEUE_URL, new ArrayList<String>());
    }


    /**
     * Test process messages when message listener is set
     */
    @Test
    public void testProcessReceivedMessagesWithMessageListener() throws JMSException {

        /*
         * Set up mocks and consumer
         */
        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);

        // Create messages
        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        List<String> receiptHandlers = createReceiptHandlersList(3);

        List<com.amazonaws.services.sqs.model.Message> messages = createSQSServiceMessages(receiptHandlers);

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */
        verify(sqsSessionRunnable, times(3)).scheduleCallBack(eq(msgListener), any(MessageManager.class));

        // Ensure no messages were added to the queue
        assertEquals(0, consumerPrefetch.messageQueue.size());
        assertEquals(3, consumerPrefetch.messagesPrefetched);

        verify(negativeAcknowledger).action(QUEUE_URL, new ArrayList<String>());
    }

    /**
     * Test process messages when message listener is set
     */
    @Test
    public void testProcessReceivedMessagesThrowsException() throws JMSException {

        /*
         * Set up mocks and consumer
         */

        // Create messages
        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        List<String> receiptHandlers = createReceiptHandlersList(3);

        List<com.amazonaws.services.sqs.model.Message> messages = createSQSServiceMessages(receiptHandlers);

        when(consumerPrefetch.convertToJMSMessage(messages.get(1)))
                .thenThrow(new JMSException("Exception"));
        when(consumerPrefetch.convertToJMSMessage(messages.get(2)))
                .thenThrow(new JMSException("Exception"));

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */

        // Ensure no messages were added to the queue
        assertEquals(1, consumerPrefetch.messageQueue.size());
        assertEquals(1, consumerPrefetch.messagesPrefetched);

        List<String> failedMessages = new ArrayList<String>();
        failedMessages.add("r1");
        failedMessages.add("r2");
        verify(negativeAcknowledger).action(QUEUE_URL, failedMessages);
    }

    /**
     * Test process messages when message listener is set
     */
    @Test
    public void testProcessReceivedMessagesNegativeAcknowledgerThrowJMSException() throws JMSException {

        /*
         * Set up mocks and consumer
         */
        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);

        doThrow(new JMSException("Exception"))
                .when(negativeAcknowledger).action(eq(QUEUE_URL), any(List.class));

        // Create messages
        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        List<String> receiptHandlers = createReceiptHandlersList(3);
        List<com.amazonaws.services.sqs.model.Message> messages = createSQSServiceMessages(receiptHandlers);

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */
        verify(sqsSessionRunnable, times(3)).scheduleCallBack(eq(msgListener), any(MessageManager.class));

        // Ensure no messages were added to the queue
        assertEquals(0, consumerPrefetch.messageQueue.size());
        assertEquals(3, consumerPrefetch.messagesPrefetched);

        verify(negativeAcknowledger).action(QUEUE_URL, new ArrayList<String>());
    }

    /**
     * Test Get Messages
     */
    @Test
    public void testGetMessages() throws InterruptedException, JMSException {

        /*
         * Set up mocks and consumer
         */
        int prefetchBatchSize = 5;
        consumerPrefetch.retriesAttempted = 5;

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QUEUE_URL)
                .withMaxNumberOfMessages(prefetchBatchSize)
                .withAttributeNames(SQSMessageConsumerPrefetch.ALL)
                .withMessageAttributeNames(SQSMessageConsumerPrefetch.ALL)
                .withWaitTimeSeconds(SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS);

        List<com.amazonaws.services.sqs.model.Message> messages = new ArrayList<>();
        messages.add(new com.amazonaws.services.sqs.model.Message().withReceiptHandle("r1"));
        messages.add(new com.amazonaws.services.sqs.model.Message().withReceiptHandle("r2"));
        messages.add(new com.amazonaws.services.sqs.model.Message().withReceiptHandle("r3"));
        messages.add(new com.amazonaws.services.sqs.model.Message().withReceiptHandle("r4"));
        messages.add(new com.amazonaws.services.sqs.model.Message().withReceiptHandle("r5"));

        ReceiveMessageResult receivedMessageResult =
                new ReceiveMessageResult().withMessages(messages);

        when(amazonSQSClient.receiveMessage(receiveMessageRequest))
                .thenReturn(receivedMessageResult);

        /*
         * Get messages
         */
        List<com.amazonaws.services.sqs.model.Message> result = consumerPrefetch.getMessages(prefetchBatchSize);

        /*
         * Verify results
         */
        assertEquals(result, messages);
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test Get Messages with illegal prefetch size
     */
    @Test
    public void testGetMessagesIllegalPrefetchSize() throws InterruptedException {

        int negativeSize = -10;
        try {
            consumerPrefetch.getMessages(negativeSize);
            fail();
        } catch(AssertionError ae) {
            // expected exception
        }

        try {
            consumerPrefetch.getMessages(0);
            fail();
        } catch(AssertionError ae) {
            // expected exception
        }
    }

    /**
     * Test Get Messages throws JMS exception
     */
    @Test
    public void testGetMessagesJMSException() throws InterruptedException, JMSException {

        /*
         * Set up mocks and consumer
         */

        int retriesAttempted = 3;
        int prefetchBatchSize = 5;
        long firstSleepTime = 100L;
        long secondSleepTime = 200L;
        consumerPrefetch.retriesAttempted = retriesAttempted;

        when(amazonSQSClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenThrow(new JMSException("test exception"));
        when(backoffStrategy.delayBeforeNextRetry(retriesAttempted))
                .thenReturn(firstSleepTime);

        when(backoffStrategy.delayBeforeNextRetry(retriesAttempted + 1))
                .thenReturn(secondSleepTime);

        consumerPrefetch.getMessages(prefetchBatchSize);

        consumerPrefetch.getMessages(prefetchBatchSize);

        /*
         * Verify results
         */
        verify(backoffStrategy).delayBeforeNextRetry(retriesAttempted);
        verify(consumerPrefetch).sleep(firstSleepTime);
        verify(consumerPrefetch).sleep(secondSleepTime);
        assertEquals(retriesAttempted + 2, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test Get Messages interrupted
     */
    @Test
    public void testGetMessagesInterruptDuringBackoff() throws InterruptedException, JMSException {

        /*
         * Set up mocks and consumer
         */

        int retriesAttempted = 3;
        final int prefetchBatchSize = 5;
        consumerPrefetch.retriesAttempted = retriesAttempted;

        when(backoffStrategy.delayBeforeNextRetry(retriesAttempted)).thenReturn(10000L);
        when(amazonSQSClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(new JMSException("test exception"));

        final CountDownLatch beforeGetMessagesCall = new CountDownLatch(1);
        final CountDownLatch recvInterruptedExceptionLatch = new CountDownLatch(1);

        /*
         * Get messages on a different execution thread
         */
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeGetMessagesCall.countDown();
                    consumerPrefetch.getMessages(prefetchBatchSize);
                } catch (InterruptedException e) {
                    recvInterruptedExceptionLatch.countDown();
                    e.printStackTrace();
                }
            }
        });
        t.start();

        assertEquals(true, beforeGetMessagesCall.await(5, TimeUnit.SECONDS));
        Thread.sleep(10);

        /*
         * Interrupt the getMessage execution
         */
        t.interrupt();

        assertEquals(true, recvInterruptedExceptionLatch.await(5, TimeUnit.SECONDS));
    }

    /**
     * Test Get Messages throws error
     */
    @Test
    public void testGetMessagesError() throws InterruptedException, JMSException {

        int retriesAttempted = 3;
        int prefetchBatchSize = 5;
        consumerPrefetch.retriesAttempted = retriesAttempted;

        when(amazonSQSClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenThrow(new Error());

        try {
            consumerPrefetch.getMessages(prefetchBatchSize);
        } catch (Error e) {
            // Expected error exception
        }
    }

    /**
     * Test start when consumer prefetch is already closed
     */
    @Test
    public void testStartAlreadyClosed() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch
         */
        consumerPrefetch.closed = true;

        /*
         * CAll Start
         */
        consumerPrefetch.start();

        /*
         * Verify the results
         */
        verify(consumerPrefetch, never()).notifyStateChange();
    }

    /**
     * Test start when consumer prefetch is already started
     */
    @Test
    public void testStartAlreadyStarted() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch
         */
        consumerPrefetch.running = true;

        /*
         * CAll Start
         */
        consumerPrefetch.start();

        /*
         * Verify the results
         */
        verify(consumerPrefetch, never()).notifyStateChange();
    }

    /**
     * Test start update the state lock
     */
    @Test
    public void testStart() throws InterruptedException, JMSException {

        /*
         * CAll Start
         */
        consumerPrefetch.start();

        /*
         * Verify the results
         */
        verify(consumerPrefetch).notifyStateChange();
        assertTrue(consumerPrefetch.running);
    }

    /**
     * Test stop when consumer prefetch is already closed
     */
    @Test
    public void testStopAlreadyClosed() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch
         */
        consumerPrefetch.closed = true;

        /*
         * CAll Start
         */
        consumerPrefetch.stop();

        /*
         * Verify the results
         */
        verify(consumerPrefetch, never()).notifyStateChange();
    }

    /**
     * Test stop when consumer prefetch is not started
     */
    @Test
    public void testStopAlreadyStarted() throws InterruptedException, JMSException {

        /*
         * CAll Start
         */
        consumerPrefetch.stop();

        /*
         * Verify the results
         */
        verify(consumerPrefetch, never()).notifyStateChange();
    }

    /**
     * Test stop update the state lock
     */
    @Test
    public void testStop() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch
         */
        consumerPrefetch.running = true;

        /*
         * CAll Start
         */
        consumerPrefetch.stop();

        /*
         * Verify the results
         */
        verify(consumerPrefetch).notifyStateChange();
        assertFalse(consumerPrefetch.running);
    }

    /**
     * Test stop when consumer prefetch is already closed
     */
    @Test
    public void testCloseAlreadyClosed() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch
         */
        consumerPrefetch.closed = true;

        /*
         * CAll Start
         */
        consumerPrefetch.close();

        /*
         * Verify the results
         */
        verify(consumerPrefetch, never()).notifyStateChange();
    }

    /**
     * Test stop when consumer prefetch is not started
     */
    @Test
    public void testClose() throws InterruptedException, JMSException {

        /*
         * CAll Start
         */
        consumerPrefetch.close();

        /*
         * Verify the results
         */
        verify(consumerPrefetch).notifyStateChange();
        assertTrue(consumerPrefetch.closed);
    }


    /*
     * Utility functions
     */

    private void addMessagesToQueue(List<String> receiptHandlers) throws JMSException {

        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        for (String receiptHandler : receiptHandlers) {

            SQSMessageConsumerPrefetch.MessageManager msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);
            com.amazonaws.services.sqs.model.Message message =
                    new com.amazonaws.services.sqs.model.Message().withReceiptHandle(receiptHandler)
                            .withAttributes(mapAttributes);
            Message m1 = consumerPrefetch.convertToJMSMessage(message);
            when(msgManager.getMessage()).thenReturn(m1);

            consumerPrefetch.messageQueue.add(msgManager);
        }
    }

    private List<com.amazonaws.services.sqs.model.Message>
    createSQSServiceMessages(List<String> receiptHandlers) throws JMSException {

        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        List<com.amazonaws.services.sqs.model.Message> resultList =
                new ArrayList<com.amazonaws.services.sqs.model.Message>();

        for (String receiptHandler : receiptHandlers) {

            resultList.add(
                    new com.amazonaws.services.sqs.model.Message().withReceiptHandle(receiptHandler)
                            .withAttributes(mapAttributes));
        }

        return resultList;
    }

    private ReceiveMessageResult createReceiveMessageResult(List<String> receiptList) {

        Map<String,String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

        List<com.amazonaws.services.sqs.model.Message> messages = new ArrayList<>();
        for (String receipt : receiptList) {
            messages.add(new com.amazonaws.services.sqs.model.Message()
                                                                .withReceiptHandle(receipt)
                                                                .withAttributes(mapAttributes));
        }

        return new ReceiveMessageResult().withMessages(messages);
    }

    private List<String> createReceiptHandlersList(int count) {
        List<String> receiptHandlers = new ArrayList<String>();
        for (int i = 0; i < count; ++i) {
            receiptHandlers.add("r" + i);
        }
        return receiptHandlers;
    }
}
