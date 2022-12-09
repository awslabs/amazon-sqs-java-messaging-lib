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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch.MessageManager;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.utils.BinaryUtils;

/**
 * Test the SQSMessageConsumerPrefetchTest class
 */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class SQSMessageConsumerPrefetchTest {

    private static final String NAMESPACE = "123456789012";
    private static final String QUEUE_NAME = "QueueName";
    private static final  String QUEUE_URL = NAMESPACE + "/" + QUEUE_NAME;
    
    @Parameters
    public static List<Object[]> getParameters() {
        return Arrays.asList(new Object[][] { {0}, {1}, {5}, {10}, {15} });
    }
   
    private final int numberOfMessagesToPrefetch;

    private Acknowledger acknowledger;
    private NegativeAcknowledger negativeAcknowledger;
    private SQSSessionCallbackScheduler sqsSessionRunnable;
    private SQSMessageConsumerPrefetch consumerPrefetch;
    private ExponentialBackoffStrategy backoffStrategy;

    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private AmazonSQSMessagingClientWrapper amazonSQSClient;

    public SQSMessageConsumerPrefetchTest(int numberOfMessagesToPrefetch) {
        this.numberOfMessagesToPrefetch = numberOfMessagesToPrefetch;
    }
    
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

        consumerPrefetch = spy(new SQSMessageConsumerPrefetch(sqsSessionRunnable, acknowledger, negativeAcknowledger,
                        sqsDestination, amazonSQSClient, numberOfMessagesToPrefetch));

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
        final int numMessages = numberOfMessagesToPrefetch > 0 ? numberOfMessagesToPrefetch : 1;
        final List<String> receipt = new ArrayList<String>();
        for (int i = 0; i < numMessages; ++i) {
            receipt.add("r" + i);
        }
        ReceiveMessageResponse receivedMessageResult = createReceiveMessageResult(receipt);

        // Mock SQS call for receive message and return messages
        int receiveMessageLimit = Math.min(10, numMessages);
        when(amazonSQSClient.receiveMessage(
                eq(ReceiveMessageRequest.builder().queueUrl(QUEUE_URL)
                        .maxNumberOfMessages(receiveMessageLimit)
                        .attributeNamesWithStrings(SQSMessageConsumerPrefetch.ALL)
                        .messageAttributeNames(SQSMessageConsumerPrefetch.ALL)
                        .waitTimeSeconds(SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS)
                        .build())))
                .thenReturn(receivedMessageResult);

        // Mock isClosed and exit after a single prefetch loop
        when(consumerPrefetch.isClosed())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(false)
                .thenAnswer(new Answer<Boolean>() {

                    @Override
                    public Boolean answer(InvocationOnMock invocation) throws Throwable {
                        // Ensure message queue was filled with expected messages
                        //after we return 'isClosed() == true' we will empty the prefetch queue while nacking messages
                        assertEquals(numMessages, consumerPrefetch.messageQueue.size());
                        for (SQSMessageConsumerPrefetch.MessageManager messageManager : consumerPrefetch.messageQueue) {
                            SQSMessage sqsMessage = (SQSMessage)messageManager.getMessage();
                            assertTrue(receipt.contains(sqsMessage.getReceiptHandle()));
                        }
                        
                        return true;
                    }
                });

        /*
         * Request a message (only relevant when prefetching is off).
         */
        consumerPrefetch.requestMessage();
        
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
        assertEquals(numMessages, consumerPrefetch.messageQueue.size());
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
        verify(consumerPrefetch, never()).getMessages(anyInt(), anyInt());

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
        verify(consumerPrefetch, never()).getMessages(anyInt(), anyInt());
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
            fail("expect exception");
        } catch (RuntimeException e) {
            // Expected exception
        }

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).waitForPrefetch();
        verify(consumerPrefetch, never()).getMessages(anyInt(), anyInt());
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

        verify(consumerPrefetch, never()).getMessages(anyInt(), anyInt());
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
        } catch (RuntimeException e) {
            // Expected exception
        }

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).getMessages(anyInt(), anyInt());
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
                .when(consumerPrefetch).getMessagesWithBackoff(anyInt());

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
        verify(consumerPrefetch).getMessagesWithBackoff(anyInt());

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
                .when(consumerPrefetch).getMessages(anyInt(), anyInt());

        /*
         * Run the prefetch
         */
        try {
            consumerPrefetch.run();
            fail("expect error");
        } catch (RuntimeException e) {
            // Expected exception
        }

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();
        verify(consumerPrefetch).getMessages(anyInt(), anyInt());

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
        javax.jms.Message message1 = mock(javax.jms.Message.class);
        when(msgManager1.getMessage())
                .thenReturn(message1);

        SQSMessageConsumerPrefetch.MessageManager msgManager2 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        javax.jms.Message message2 = mock(javax.jms.Message.class);
        when(msgManager2.getMessage())
                .thenReturn(message2);

        consumerPrefetch.messageQueue.add(msgManager1);
        consumerPrefetch.messageQueue.add(msgManager2);

        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.running = true;
        consumerPrefetch.setMessageListener(msgListener);
              
        assertTrue(consumerPrefetch.messageQueue.isEmpty());

        List<MessageManager> expectedList = new ArrayList<MessageManager>();
        expectedList.add(msgManager1);
        expectedList.add(msgManager2);
        verify(sqsSessionRunnable).scheduleCallBacks(msgListener, expectedList);

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
        assertEquals(false, passedWaitForStart.await(10, TimeUnit.SECONDS));
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
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch + 5;
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
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch - 1;

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
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch + 5;
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
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch + 5;
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
        Map<MessageSystemAttributeName,String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        Message message = Message.builder()
        		.messageAttributes(new HashMap<String, MessageAttributeValue>())
        		.attributes(mapAttributes)
        		.body("MessageBody")
        		.build();

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
        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()		
        		.stringValue(SQSMessage.BYTE_MESSAGE_TYPE)
        		.dataType(SQSMessagingClientConstants.STRING)
        		.build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");
        
        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };

        Message message = Message.builder()
        		.messageAttributes(mapMessageAttributes)
        		.attributes(mapAttributes)
        		.body(BinaryUtils.toBase64(byteArray))
        		.build();

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

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
        		.stringValue(SQSMessage.BYTE_MESSAGE_TYPE)
        		.dataType(SQSMessagingClientConstants.STRING)
        		.build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        // Return message attributes with message type 'BYTE'
        // Return illegal message body for byte message type
        Message message = Message.builder()
        			.messageAttributes(mapMessageAttributes)
        			.attributes(mapAttributes)
					.body("Text Message")
					.build();

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

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
        	.stringValue(SQSMessage.OBJECT_MESSAGE_TYPE)
        	.dataType(SQSMessagingClientConstants.STRING)
        	.build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        // Encode an object to byte array
        Integer integer = Integer.valueOf("10");
        ByteArrayOutputStream array = new ByteArrayOutputStream(10);
        ObjectOutputStream oStream = new ObjectOutputStream(array);
        oStream.writeObject(integer);
        oStream.close();

        
        // Return message attributes with message type 'OBJECT'
       Message message = Message.builder()
	        .messageAttributes(mapMessageAttributes)
	        .attributes(mapAttributes)
	        .body(BinaryUtils.toBase64(array.toByteArray()))
	        .build();

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

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
        		.stringValue(SQSMessage.OBJECT_MESSAGE_TYPE)
        		.dataType(SQSMessagingClientConstants.STRING)
        		.build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        // Return message attributes with message type 'OBJECT'
        Message message = Message.builder()
	        .messageAttributes(mapMessageAttributes)
	        .attributes(mapAttributes)
	        .body("Some text that does not represent an object")
	        .build();

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
        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
        		.stringValue(SQSMessage.TEXT_MESSAGE_TYPE)
        		.dataType(SQSMessagingClientConstants.STRING)
        		.build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        Map<String, String> mapAttributes = new HashMap<String, String>();
        mapAttributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");
        Long now = DateTime.now().getMillis();
        mapAttributes.put(SQSMessagingClientConstants.SENT_TIMESTAMP, now.toString());

        // Return message attributes with message type 'TEXT'
        Message message = Message.builder()
	        .messageAttributes(mapMessageAttributes)
	        .attributesWithStrings(mapAttributes)
	        .body("MessageBody")
	        .build();

        /*
         * Convert the SQS message to JMS Message
         */
        javax.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSTextMessage);
        assertEquals(message.body(), "MessageBody");
        assertEquals(jsmMessage.getJMSTimestamp(), now.longValue());
    }

    /**
     * Test received messages when consumer prefetch has not started
     */
    @Test
    public void testReceiveWhenNotStarted() throws JMSException {

        consumerPrefetch.running = false;

        assertNull(consumerPrefetch.receive());
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
        
        assertNull(consumerPrefetch.receive(100));
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
        
        assertNull(consumerPrefetch.receiveNoWait());
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages when consumer prefetch has is closed
     */
    @Test
    public void testReceiveWhenClosed() throws JMSException {

        consumerPrefetch.closed = true;

        assertNull(consumerPrefetch.receive());
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
        
        assertNull(consumerPrefetch.receive(100));
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
        
        assertNull(consumerPrefetch.receiveNoWait());
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
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
        verify(consumerPrefetch, times(2)).notifyStateChange();
        
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages
     */
    @Test
    public void testReceiveNoWaitPrefetch() throws JMSException {

        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.running = true;

        List<String> receiptHandlers = createReceiptHandlersList(20);

        addMessagesToQueue(receiptHandlers);

        /*
         * Call receive messages
         */
        SQSMessage msg = (SQSMessage)consumerPrefetch.receiveNoWait();

        /*
         * Verify results
         */
        receiptHandlers.contains(msg.getReceiptHandle());
        verify(acknowledger).notifyMessageReceived(msg);
        verify(consumerPrefetch, times(2)).notifyStateChange();
        
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
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
                    javax.jms.Message msg = consumerPrefetch.receive(0);
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
        
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages wait when no message are prefetch and return newly added message
     */
    @Test
    public void testReceiveMessageEmptyThenAddMessage() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and latches
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
        
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
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

        // verify that we did not exit early
        long measuredTime = System.currentTimeMillis() - startTime; 
        assertTrue(String.format("Expected wait time = %1$s ms and has to be less than or equal to measured time = %2$s ms", waitTime, measuredTime), waitTime <= measuredTime);
        
        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }
    
    /**
     * Test received messages with timeout
     */
    @Test
    public void testReceiveNoWaitEmpty() throws InterruptedException, JMSException {

        /*
         * Set up consumer prefetch and time stamps
         */
        consumerPrefetch.running = true;

        if (numberOfMessagesToPrefetch == 0) {
            when(amazonSQSClient.receiveMessage(any(ReceiveMessageRequest.class)))
                    .thenReturn(ReceiveMessageResponse.builder().build());
        }
        
        /*
         * Call receive messages
         */
        SQSMessage msg = (SQSMessage) consumerPrefetch.receiveNoWait();

        assertNull(msg);

        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test process received messages with empty input
     */
    @Test
    public void testProcessReceivedMessagesEmptyInput() {

        consumerPrefetch.processReceivedMessages(new ArrayList<Message>());
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
        List<Message> messages = createSQSServiceMessages(receiptHandlers);

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

        List<Message> messages = createSQSServiceMessages(receiptHandlers);

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(sqsSessionRunnable, times(1)).scheduleCallBacks(eq(msgListener), captor.capture());
        assertEquals(3, captor.getValue().size());

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

        List<Message> messages = createSQSServiceMessages(receiptHandlers);

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
        List<Message> messages = createSQSServiceMessages(receiptHandlers);

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(sqsSessionRunnable, times(1)).scheduleCallBacks(eq(msgListener), captor.capture());
        assertEquals(3, captor.getValue().size());

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

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
        		.queueUrl(QUEUE_URL)
                .maxNumberOfMessages(prefetchBatchSize)
                .attributeNamesWithStrings(SQSMessageConsumerPrefetch.ALL)
                .messageAttributeNames(SQSMessageConsumerPrefetch.ALL)
                .waitTimeSeconds(SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS)
                .build();

        List<Message> messages = new ArrayList<Message>();
        messages.add(Message.builder().receiptHandle("r1").build());
        messages.add(Message.builder().receiptHandle("r2").build());
        messages.add(Message.builder().receiptHandle("r3").build());
        messages.add(Message.builder().receiptHandle("r4").build());
        messages.add(Message.builder().receiptHandle("r5").build());

        ReceiveMessageResponse receivedMessageResult = ReceiveMessageResponse.builder()
        		.messages(messages)
        		.build();

        when(amazonSQSClient.receiveMessage(receiveMessageRequest))
                .thenReturn(receivedMessageResult);

        /*
         * Get messages
         */
        List<Message> result = consumerPrefetch.getMessagesWithBackoff(prefetchBatchSize);

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
    public void testGetMessagesIllegalPrefetchSize() throws JMSException {

        int negativeSize = -10;
        try {
            consumerPrefetch.getMessages(negativeSize, 0);
            fail();
        } catch(AssertionError ae) {
            // expected exception
        }

        try {
            consumerPrefetch.getMessages(0, 0);
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

        consumerPrefetch.getMessagesWithBackoff(prefetchBatchSize);

        consumerPrefetch.getMessagesWithBackoff(prefetchBatchSize);

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
                    consumerPrefetch.getMessagesWithBackoff(prefetchBatchSize);
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
            consumerPrefetch.getMessages(prefetchBatchSize, 0);
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
       // verify(consumerPrefetch).notifyStateChange();
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

    /**
     * Test that concurrent receive requests results in fetching more messages
     * from the queue with a single request, even if prefetching is set lower or even to 0.
     */
    @Test
    public void testRequestedMessageTracking() throws InterruptedException, JMSException, ExecutionException {
        int concurrentReceives = 3;
        int receiveBatchSize = Math.min(SQSMessagingClientConstants.MAX_BATCH,
                Math.max(concurrentReceives, numberOfMessagesToPrefetch));
        
        // Create messages return from SQS
        final List<String> receipt = new ArrayList<String>();
        for (int i = 0; i < receiveBatchSize; ++i) {
            receipt.add("r" + i);
        }
        ReceiveMessageResponse receivedMessageResult = createReceiveMessageResult(receipt);

        // Mock SQS call for receive message and return messages
        when(amazonSQSClient.receiveMessage(
                eq(ReceiveMessageRequest.builder()
                		.queueUrl(QUEUE_URL)
                        .maxNumberOfMessages(receiveBatchSize)
                        .attributeNamesWithStrings(SQSMessageConsumerPrefetch.ALL)
                        .messageAttributeNames(SQSMessageConsumerPrefetch.ALL)
                        .waitTimeSeconds(SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS)
                        .build())))
                .thenReturn(receivedMessageResult);
        
        final CountDownLatch allReceivesWaiting = new CountDownLatch(concurrentReceives);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                invocation.callRealMethod();
                allReceivesWaiting.countDown();
                return null;
            }
        }).when(consumerPrefetch).requestMessage();
        
        // Close the prefetcher immediately after completing one loop
        final List<Future<javax.jms.Message>> receivedMessageFutures = new ArrayList<>();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                invocation.callRealMethod();
                for (Future<javax.jms.Message> messageFuture : receivedMessageFutures) {
                    Assert.assertNotNull(messageFuture.get());
                }
                consumerPrefetch.close();
                return null;
            }
        }).when(consumerPrefetch).processReceivedMessages(any(List.class));
        
        // Set running to true first so that the receive calls don't terminate early
        consumerPrefetch.running = true;
        
        ExecutorService receiveExecutor = Executors.newFixedThreadPool(concurrentReceives);
        for (int i = 0; i < concurrentReceives; i++) {
            receivedMessageFutures.add(receiveExecutor.submit(new Callable<javax.jms.Message>() {
                @Override
                public javax.jms.Message call() throws Exception {
                    return consumerPrefetch.receive();
                }
            }));
        }
        
        // Wait to make sure the receive calls have gotten far enough to
        // wait on the message queue
        allReceivesWaiting.await();
        
        Assert.assertEquals(concurrentReceives, consumerPrefetch.messagesRequested);
        
        consumerPrefetch.run();
    }

    /**
     * Test SetMessageListener before starting prefetch
     * Setting MessageListener before starting prefetch would not mark
     * the message listener as ready. Therefore start() method should
     * do this work in order to get pre-fetch going even when
     * number of messages to pre-fetch is set to 0.
     */
    @Test
    public void testSetMessageListenerBeforeStart() {

        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);
        consumerPrefetch.start();
        assertEquals(1, consumerPrefetch.messagesRequested);
    }

    /*
     * Utility functions
     */

    private void addMessagesToQueue(List<String> receiptHandlers) throws JMSException {

        Map<MessageSystemAttributeName,String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        for (String receiptHandler : receiptHandlers) {

            SQSMessageConsumerPrefetch.MessageManager msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);
            Message message = Message.builder()
            		.receiptHandle(receiptHandler)
                    .attributes(mapAttributes)
                    .build();
            javax.jms.Message m1 = consumerPrefetch.convertToJMSMessage(message);
            when(msgManager.getMessage()).thenReturn(m1);

            consumerPrefetch.messageQueue.add(msgManager);
        }
    }

    private List<Message> createSQSServiceMessages(List<String> receiptHandlers) throws JMSException {

        Map<MessageSystemAttributeName,String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        List<Message> resultList =
                new ArrayList<Message>();

        for (String receiptHandler : receiptHandlers) {

            resultList.add(
                    Message.builder().receiptHandle(receiptHandler)
                            .attributes(mapAttributes).build());
        }

        return resultList;
    }

    private ReceiveMessageResponse createReceiveMessageResult(List<String> receiptList) {

        Map<MessageSystemAttributeName,String> mapAttributes = new HashMap<>();
        mapAttributes.put(MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        List<Message> messages = new ArrayList<Message>();
        for (String receipt : receiptList) {
            messages.add(Message.builder().receiptHandle(receipt).attributes(mapAttributes).build());
        }

        return ReceiveMessageResponse.builder().messages(messages).build();
    }

    private List<String> createReceiptHandlersList(int count) {
        List<String> receiptHandlers = new ArrayList<String>();
        for (int i = 0; i < count; ++i) {
            receiptHandlers.add("r" + i);
        }
        return receiptHandlers;
    }
}
