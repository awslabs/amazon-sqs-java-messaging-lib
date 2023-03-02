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

import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.ObjectMessage;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSSessionTest class
 */
public class SQSSessionTest  {

    private static final String QUEUE_URL = "queueUrl";
    private static final String QUEUE_NAME = "queueName";
    private static final String OWNER_ACCOUNT_ID = "accountId";

    private SQSSession sqsSession;
    private SQSConnection parentSQSConnection;
    private Set<SQSMessageConsumer> messageConsumers;
    private Set<SQSMessageProducer> messageProducers;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
    private SQSMessageConsumer consumer1;
    private SQSMessageConsumer consumer2;
    private SQSMessageProducer producer1;
    private SQSMessageProducer producer2;
    private AmazonSQSMessagingClientWrapper sqsClientJMSWrapper;

    @BeforeEach
    public void setup() throws JMSException {
        sqsClientJMSWrapper = mock(AmazonSQSMessagingClientWrapper.class);

        parentSQSConnection = mock(SQSConnection.class);
        when(parentSQSConnection.getWrappedAmazonSQSClient()).thenReturn(sqsClientJMSWrapper);

        consumer1 = mock(SQSMessageConsumer.class);
        consumer2 = mock(SQSMessageConsumer.class);

        when(consumer1.getQueue()).thenReturn(new SQSQueueDestination("name1", "url1"));
        when(consumer2.getQueue()).thenReturn(new SQSQueueDestination("name2", "url2"));

        messageConsumers = new HashSet<>(Set.of(consumer1, consumer2));

        producer1 = mock(SQSMessageProducer.class);
        producer2 = mock(SQSMessageProducer.class);

        messageProducers = new HashSet<>(Set.of(producer1, producer2));

        sqsSession = spy(new SQSSession(parentSQSConnection, AcknowledgeMode.ACK_AUTO,
                messageConsumers, messageProducers));
    }

    /**
     * Test stop is a no op if already closed
     */
    @Test
    public void testStopNoOpIfAlreadyClosed() throws JMSException {

        /*
         * Set up session
         */
        sqsSession = new SQSSession(parentSQSConnection, AcknowledgeMode.ACK_AUTO, messageConsumers, messageProducers);
        sqsSession.close();

        /*
         * stop consumer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.stop(), "Session is closed");

        /*
         * Verify results
         */
        verify(consumer1, never()).stopPrefetch();
        verify(consumer2, never()).stopPrefetch();
    }

    /**
     * Test stop is a no op if closing
     */
    @Test
    public void testStopNoOpIfClosing() {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);

        /*
         * stop consumer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.stop(), "Session is closed or closing");

        /*
         * Verify results
         */
        verify(consumer1, never()).stopPrefetch();
        verify(consumer2, never()).stopPrefetch();
    }

    /**
     * Test start is a no op if closing
     */
    @Test
    public void testStartNoOpIfClosing() {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);

        /*
         * stop consumer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.start(), "Session is closed or closing");

        /*
         * Verify results
         */
        verify(consumer1, never()).startPrefetch();
        verify(consumer2, never()).startPrefetch();
    }

    /**
     * Test start is a no op if already closed
     */
    @Test
    public void testStartNoOpIfAlreadyClosed() throws JMSException {

        /*
         * Set up session
         */
        sqsSession = new SQSSession(parentSQSConnection, AcknowledgeMode.ACK_AUTO, messageConsumers, messageProducers);
        sqsSession.close();
        SQSMessageConsumer consumer1 = mock(SQSMessageConsumer.class);
        SQSMessageConsumer consumer2 = mock(SQSMessageConsumer.class);
        messageConsumers.add(consumer1);
        messageConsumers.add(consumer2);

        /*
         * stop consumer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.start(), "Session is closed");

        /*
         * Verify results
         */
        verify(consumer1, never()).startPrefetch();
        verify(consumer2, never()).startPrefetch();
    }

    /**
     * Test stop blocks on state lock
     */
    @Test
    public void testStopBlocksOnStateLock() throws InterruptedException {

        /*
         * Set up the latches and mocks
         */
        final CountDownLatch mainRelease = new CountDownLatch(1);
        final CountDownLatch holdStateLock = new CountDownLatch(1);
        final CountDownLatch beforeSessionStopCall = new CountDownLatch(1);
        final CountDownLatch passedSessionStopCall = new CountDownLatch(1);

        // Run a thread to hold the stateLock
        executorService.execute(() -> {
            try {
                synchronized (sqsSession.getStateLock()) {
                    holdStateLock.countDown();
                    mainRelease.await();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to stop the session while state lock is being held
        executorService.execute(() -> {
            beforeSessionStopCall.countDown();
            try {
                sqsSession.stop();
                passedSessionStopCall.countDown();
            } catch (IllegalStateException e) {
                e.printStackTrace();
            }
        });

        beforeSessionStopCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock
        assertFalse(passedSessionStopCall.await(2, TimeUnit.SECONDS));

        // Release the thread holding the state lock
        mainRelease.countDown();

        // Ensure that the session stop call completed
        passedSessionStopCall.await();

        verify(consumer1).stopPrefetch();
        verify(consumer2).stopPrefetch();
    }

    /**
     * Test start blocks on state lock
     */
    @Test
    public void testStartBlocksOnStateLock() throws InterruptedException {

        /*
         * Set up the latches and mocks
         */
        final CountDownLatch mainRelease = new CountDownLatch(1);
        final CountDownLatch holdStateLock = new CountDownLatch(1);
        final CountDownLatch beforeSessionStartCall = new CountDownLatch(1);
        final CountDownLatch passedSessionStartCall = new CountDownLatch(1);

        // Run a thread to hold the stateLock
        executorService.execute(() -> {
            try {
                synchronized (sqsSession.getStateLock()) {
                    holdStateLock.countDown();
                    mainRelease.await();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to start the session while state lock is being held
        executorService.execute(() -> {
            try {
                beforeSessionStartCall.countDown();
                sqsSession.start();
                passedSessionStartCall.countDown();
            } catch (IllegalStateException e) {
                e.printStackTrace();
            }
        });

        beforeSessionStartCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock
        assertFalse(passedSessionStartCall.await(2, TimeUnit.SECONDS));

        // Release the thread holding the state lock
        mainRelease.countDown();

        // Ensure that the session start completed
        passedSessionStartCall.await();

        verify(consumer1).startPrefetch();
        verify(consumer2).startPrefetch();
    }

    /**
     * Test unsupported feature throws the correct exception
     */
    @Test
    public void testUnsupportedFeature() throws JMSException {
        assertFalse(sqsSession.getTransacted());

        assertThrows(JMSException.class, () -> sqsSession.commit(), SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.rollback(), SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.unsubscribe("test"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createTopic("topic"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createDurableSubscriber(null, "name"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createDurableSubscriber(null, "name", "messageSelector", false),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createBrowser(null),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createBrowser(null, "messageSelector"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createTemporaryQueue(),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createTemporaryTopic(),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.getMessageListener(),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.setMessageListener(null),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createStreamMessage(),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createMapMessage(),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createSharedConsumer(null, "name"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createSharedConsumer(null, "name", "messageSelector"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createDurableConsumer(null, "name"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () ->
                        sqsSession.createDurableConsumer(null, "name", "messageSelector", true),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createSharedDurableConsumer(null, "name"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);

        assertThrows(JMSException.class, () -> sqsSession.createSharedDurableConsumer(null, "name", "messageSelector"),
                SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /**
     * Test waitForAllCallbackComplete blocks on state lock
     */
    @Test
    public void testWaitForAllCallbackCompleteBlocksOnStateLock() throws InterruptedException, JMSException {

        /*
         * Set up the latches and mocks
         */
        final CountDownLatch mainRelease = new CountDownLatch(1);
        final CountDownLatch holdStateLock = new CountDownLatch(1);
        final CountDownLatch beforeSessionWaitCall = new CountDownLatch(1);
        final CountDownLatch passedSessionWaitCall = new CountDownLatch(1);

        sqsSession = new SQSSession(parentSQSConnection, AcknowledgeMode.ACK_AUTO, messageConsumers, messageProducers);
        sqsSession.start();

        SQSMessageConsumerPrefetch.MessageManager msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);

        PrefetchManager prefetchManager = new PrefetchManager() {
            @Override
            public void messageDispatched()  {
                holdStateLock.countDown();
                try {
                    mainRelease.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void messageListenerReady() {
            }

            @Override
            public SQSMessageConsumer getMessageConsumer() {
                return consumer1;
            }
        };

        when(msgManager.prefetchManager()).thenReturn(prefetchManager);

        sqsSession.getSqsSessionRunnable().scheduleCallBacks(null, Collections.singletonList(msgManager));

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to close the consumer while state lock is being held
        executorService.execute(() -> {
            beforeSessionWaitCall.countDown();
            sqsSession.waitForCallbackComplete();
            passedSessionWaitCall.countDown();
        });

        beforeSessionWaitCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock this time is longer then waitForAllCallbackComplete timeoutMillis input
        assertFalse(passedSessionWaitCall.await(1, TimeUnit.SECONDS));

        // Release the state lock
        mainRelease.countDown();

        // Ensure that the session wait for callback completed has finished
        passedSessionWaitCall.await();
    }

    /**
     * Test waitForAllCallbackComplete get notify on lock change
     */
    @Test
    public void testWaitForAllCallbackComplete() throws InterruptedException, JMSException {

        /*
         * Set up session and mocks
         */
        sqsSession = new SQSSession(parentSQSConnection, AcknowledgeMode.ACK_AUTO, messageConsumers, messageProducers);
        sqsSession.start();
        sqsSession.startingCallback(consumer1);
        final CountDownLatch beforeWaitCall = new CountDownLatch(1);
        final CountDownLatch passedWaitCall = new CountDownLatch(1);

        /*
         * call waitForAllCallbackComplete in different thread
         */
        executorService.execute(() -> {
            beforeWaitCall.countDown();
            sqsSession.waitForCallbackComplete();
            passedWaitCall.countDown();
        });

        // Yield execution to allow the consumer to wait
        assertTrue(beforeWaitCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the lock and ensure that we are still waiting since the prefetch message still equal to the limit
        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        assertFalse(passedWaitCall.await(1, TimeUnit.SECONDS));

        // Simulate callback finished
        sqsSession.finishedCallback();
        passedWaitCall.await();
    }

    /**
     * Test finishedCallback decrease callbackCounter
     */
    @Test
    public void testFinishedCallback() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setActiveConsumerInCallback(consumer1);

        /*
         * Call finishedCallback
         */
        sqsSession.finishedCallback();

        /*
         * verify result
         */
        assertFalse(sqsSession.isCallbackActive());
    }

    /**
     * Test startingCallback is a no op if already closed
     */
    @Test
    public void testStartingCallbackNoOpIfAlreadyClosed() throws InterruptedException, JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);
        sqsSession.setActiveConsumerInCallback(null); 
        /*
         * Start callback
         */
        sqsSession.startingCallback(consumer1);

        /*
         * Verify results
         */
        assertFalse(sqsSession.isCallbackActive());
    }

    /**
     * Test startingCallback is a no op if closing
     */
    @Test
    public void testStartingCallbackNoOpIfClosing() {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);
        sqsSession.setActiveConsumerInCallback(null);
        
        /*
         * Starting Callback
         */
        assertThrows(JMSException.class, () -> sqsSession.startingCallback(consumer1));
    }

    /**
     * Test startingCallback get notify on lock change
     */
    @Test
    public void testStartingCallback() throws InterruptedException {

        /*
         * Set up session and mocks
         */
        sqsSession.setActiveConsumerInCallback(null);
        final CountDownLatch beforeWaitCall = new CountDownLatch(1);
        final CountDownLatch passedWaitCall = new CountDownLatch(1);

        /*
         * call startingCallback in different thread
         */
        executorService.execute(() -> {
            try {
                beforeWaitCall.countDown();
                sqsSession.startingCallback(consumer1);
                passedWaitCall.countDown();
            } catch (InterruptedException | JMSException e) {
                e.printStackTrace();
            }
        });

        // Yield execution to allow the session to wait
        assertTrue(beforeWaitCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the lock and ensure that we are still waiting since the did not run
        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        assertFalse(passedWaitCall.await(2, TimeUnit.SECONDS));

        // Simulate callback finished
        sqsSession.setRunning(true);

        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        passedWaitCall.await();
        assertTrue(sqsSession.isCallbackActive());
    }

    /**
     * Test removing consumer
     */
    @Test
    public void testRemoveConsumer() {
        assertEquals(2, messageConsumers.size());

        /*
         * Remove the consumer
         */
        sqsSession.removeConsumer(consumer1);

        /*
         * Verify results
         */
        assertEquals(1, messageConsumers.size());
        assertEquals(consumer2, messageConsumers.iterator().next());
    }

    /**
     * Test removing producer
     */
    @Test
    public void testRemoveProducer() {
        assertEquals(2, messageProducers.size());

        /*
         * Remove the producer
         */
        sqsSession.removeProducer(producer1);

        /*
         * Verify results
         */
        assertEquals(1, messageProducers.size());
        assertEquals(producer2, messageProducers.iterator().next());
    }


    /**
     * Test create queue when session is already closed
     */
    @Test
    public void testCreateQueueWhenAlreadyClosed() {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        /*
         * Create queue
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.createQueue("Test"), "Session is closed");
    }

    /**
     * Test create queue when session is already closed
     */
    @Test
    public void testCreateQueue() throws JMSException {
        GetQueueUrlResult result = new GetQueueUrlResult().withQueueUrl(QUEUE_URL);
        when(sqsClientJMSWrapper.getQueueUrl(QUEUE_NAME))
                .thenReturn(result);

        /*
         * Create queue
         */
        Queue queue = sqsSession.createQueue(QUEUE_NAME);

        /*
         * Verify results
         */
        assertTrue(queue instanceof SQSQueueDestination);
        assertEquals(QUEUE_NAME, queue.getQueueName());
        assertEquals(QUEUE_URL, ((SQSQueueDestination) queue).getQueueUrl());
    }
    
    /**
     * Test create queue when session is already closed
     */
    @Test
    public void testCreateQueueWithOwnerAccountId() throws JMSException {
        GetQueueUrlResult result = new GetQueueUrlResult().withQueueUrl(QUEUE_URL);
        when(sqsClientJMSWrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID)).thenReturn(result);

        /*
         * Create queue
         */
        Queue queue = sqsSession.createQueue(QUEUE_NAME, OWNER_ACCOUNT_ID);

        /*
         * Verify results
         */
        assertTrue(queue instanceof SQSQueueDestination);
        assertEquals(QUEUE_NAME, queue.getQueueName());
        assertEquals(QUEUE_URL, ((SQSQueueDestination) queue).getQueueUrl());
    }

    /**
     * Test create consumer when session is already closed
     */
    @Test
    public void testCreateConsumerProducerWhenAlreadyClosed() {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        Destination destination = new Destination() {};

        /*
         * Create consumer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.createConsumer(destination), "Session is closed");

        /*
         * Create producer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.createProducer(destination), "Session is closed");

    }

    /**
     * Test create consumer when session is closing
     */
    @Test
    public void testCreateConsumerProducerWhenClosing() {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        /*
         * Create consumer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.createConsumer(destination), "Session is closed or closing");

        /*
         * Create producer
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.createProducer(destination), "Session is closed or closing");

    }

    /**
     * Test create consumer when destination is non SQSQueueDestination
     */
    @Test
    public void testCreateConsumerProducerNonSQSQueueDestination() {

        /*
         * Set up session
         */
        Destination destination = new Destination() {};

        /*
         * Create consumer
         */
        assertThrows(JMSException.class, () -> sqsSession.createConsumer(destination),
                "Actual type of Destination/Queue has to be SQSQueueDestination");

        /*
         * Create producer
         */
        assertThrows(JMSException.class, () -> sqsSession.createProducer(destination),
                "Actual type of Destination/Queue has to be SQSQueueDestination");
    }

    /**
     * Test create consumer when session is not running
     */
    @Test
    public void testCreateConsumerNotRunning() throws JMSException {

        /*
         * Set up session
         */
        SQSMessageConsumer consumer3 = mock(SQSMessageConsumer.class);
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        sqsSession.setRunning(false);
        doReturn(consumer3).when(sqsSession).createSQSMessageConsumer(destination);

        /*
         * Create consumer
         */
        MessageConsumer result = sqsSession.createConsumer(destination);

        /*
         * Verify results
         */
        verify(consumer3, never()).startPrefetch();
        assertEquals(consumer3, result);
        assertEquals(3, messageConsumers.size());
    }

    /**
     * Test create consumer when session is running
     */
    @Test
    public void testCreateConsumerRunning() throws JMSException {

        /*
         * Set up session
         */
        SQSMessageConsumer consumer3 = mock(SQSMessageConsumer.class);
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        sqsSession.setRunning(true);
        doReturn(consumer3).when(sqsSession).createSQSMessageConsumer(destination);

        /*
         * Create consumer
         */
        MessageConsumer result = sqsSession.createConsumer(destination);

        /*
         * Verify results
         */
        verify(consumer3).startPrefetch();
        assertEquals(consumer3, result);
        assertEquals(3, messageConsumers.size());
    }

    /**
     * Test create consumer with non-null message selector
     */
    @Test
    public void testCreateConsumerNonNullMessageSelector() {

        /*
         * Set up session
         */
        SQSMessageConsumer consumer3 = mock(SQSMessageConsumer.class);
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        sqsSession.setRunning(true);
        doReturn(consumer3)
                .when(sqsSession).createSQSMessageConsumer(destination);

        /*
         * Create consumer
         */
        assertThrows(JMSException.class, () -> sqsSession.createConsumer(destination, "Selector"),
                "SQSSession does not support MessageSelector. This should be null.");

        assertThrows(JMSException.class, () -> sqsSession.createConsumer(destination, "Selector", true),
                "SQSSession does not support MessageSelector. This should be null.");
    }

    /**
     * Test create consumer with null message selector
     */
    @Test
    public void testCreateConsumerWithMessageSelector() throws JMSException {

        /*
         * Set up session
         */
        SQSMessageConsumer consumer3 = mock(SQSMessageConsumer.class);
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        sqsSession.setRunning(true);
        doReturn(consumer3).when(sqsSession).createSQSMessageConsumer(destination);

        /*
         * Create consumer
         */
        sqsSession.createConsumer(destination, null);
        sqsSession.createConsumer(destination, null,true);

        /*
         * Verify results
         */
        verify(sqsSession, times(2)).createConsumer(destination);
    }

    /**
     * Test create producer when session is running
     */
    @Test
    public void testCreateProducer() throws JMSException {

        /*
         * Set up session
         */
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        /*
         * Create producer
         */
        MessageProducer result = sqsSession.createProducer(destination);

        /*
         * Verify results
         */
        assertEquals(3, messageProducers.size());
        assertEquals(destination, result.getDestination());
    }

    /**
     * Test recover
     */
    @Test
    public void testRecover() throws JMSException, InterruptedException {
        sqsSession = new SQSSession(parentSQSConnection, AcknowledgeMode.ACK_UNORDERED);
        when(parentSQSConnection.getNumberOfMessagesToPrefetch()).thenReturn(4);

        when(sqsClientJMSWrapper.getQueueUrl("queue1"))
            .thenReturn(new GetQueueUrlResult().withQueueUrl("queueUrl1"));
        when(sqsClientJMSWrapper.receiveMessage(argThat(new ReceiveRequestMatcher("queueUrl1"))))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group1", "message1", "queue1-group1-message1")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group2", "message2", "queue1-group2-message2")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group3", "message3", "queue1-group3-message3")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group1", "message4", "queue1-group1-message4")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group2", "message5", "queue1-group2-message5")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group3", "message6", "queue1-group3-message6")))
            .thenReturn(new ReceiveMessageResult());
        
        when(sqsClientJMSWrapper.getQueueUrl("queue2"))
            .thenReturn(new GetQueueUrlResult().withQueueUrl("queueUrl2"));
        when(sqsClientJMSWrapper.receiveMessage(argThat(new ReceiveRequestMatcher("queueUrl2"))))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group1", "message1", "queue2-group1-message1")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group2", "message2", "queue2-group2-message2")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group3", "message3", "queue2-group3-message3")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group1", "message4", "queue2-group1-message4")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group2", "message5", "queue2-group2-message5")))
            .thenReturn(new ReceiveMessageResult().withMessages(createFifoMessage("group3", "message6", "queue2-group3-message6")))
            .thenReturn(new ReceiveMessageResult());
    
        MessageConsumer consumer1 = sqsSession.createConsumer(sqsSession.createQueue("queue1"));
        MessageConsumer consumer2 = sqsSession.createConsumer(sqsSession.createQueue("queue2"));
        final CountDownLatch listenerRelease = new CountDownLatch(1);
        consumer2.setMessageListener(message -> {
            try {
                listenerRelease.await();
            } catch (InterruptedException ignored) {
            }
        });
        
        sqsSession.start();
        
        Message message1 = consumer1.receive();
        
        //let's give a moment for the background threads to:
        //prefetch another message for queue1
        //dispatch message to listener for queue2
        //prefetch another message for queue2
        Thread.sleep(100);
        /*
         * Recover
         */
        sqsSession.recover();
        
        //at this point we have two unacked messages:
        //queue1-group1-message1
        //queue2-group1-message1
        //and we should have 4 more messages prefetched for queue1:
        //queue1-group2-message2
        //queue1-group3-message3
        //queue1-group1-message4
        //queue1-group2-message5
        //and we should have 4 more callbacks scheduled for queue2:
        //queue2-group2-message2
        //queue2-group3-message3
        //queue2-group1-message4
        //queue2-group2-message5
        //after calling recovery, we should nack the two unacked messages and all other messages for the same queue / group, so these:
        //queue1-group1-message1
        //queue2-group1-message1
        //queue1-group1-message4
        //queue2-group1-message4
        
        ArgumentCaptor<ChangeMessageVisibilityBatchRequest> changeVisibilityCaptor = ArgumentCaptor.forClass(ChangeMessageVisibilityBatchRequest.class);
        verify(sqsClientJMSWrapper, times(2)).changeMessageVisibilityBatch(changeVisibilityCaptor.capture());

        Set<String> handles = changeVisibilityCaptor.getAllValues().stream()
                .map(ChangeMessageVisibilityBatchRequest::getEntries)
                .flatMap(Collection::stream)
                .map(ChangeMessageVisibilityBatchRequestEntry::getReceiptHandle)
                .collect(Collectors.toSet());
        
        assertEquals(4, handles.size());
        assertTrue(handles.contains("queue1-group1-message1"));
        assertTrue(handles.contains("queue1-group1-message4"));
        assertTrue(handles.contains("queue2-group1-message1"));
        assertTrue(handles.contains("queue2-group1-message4"));
        
        listenerRelease.countDown();
        
        sqsSession.close();
    }
    
    private record ReceiveRequestMatcher(String queueUrl) implements ArgumentMatcher<ReceiveMessageRequest> {

        @Override
        public boolean matches(ReceiveMessageRequest request) {
            return request != null && queueUrl.equals(request.getQueueUrl());
        }
    }

    private com.amazonaws.services.sqs.model.Message createFifoMessage(String groupId, String messageId, String receiptHandle) {
        com.amazonaws.services.sqs.model.Message message = new com.amazonaws.services.sqs.model.Message();
        message.setBody("body");
        message.setMessageId(messageId);
        message.setReceiptHandle(receiptHandle);
        Map<String, String> attributes = Map.of(
            SQSMessagingClientConstants.SEQUENCE_NUMBER, "728374687246872364",
            SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, messageId,
            SQSMessagingClientConstants.MESSAGE_GROUP_ID, groupId,
            SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "0"
        );
        message.setAttributes(attributes);
        return message;
    }

    /**
     * Test recover when session is already closed
     */
    @Test
    public void testRecoverWhenAlreadyClosed() {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        /*
         * Recover
         */
        assertThrows(IllegalStateException.class, () ->  sqsSession.recover(), "Session is closed");
    }

    /**
     * Test do close when session is already closed
     */
    @Test
    public void testDoCloseWhenAlreadyClosed() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        /*
         * Do close
         */
        sqsSession.doClose();

        /*
         * Verify results
         */
        verify(parentSQSConnection, never()).removeSession(any(SQSSession.class));
        verify(consumer1, never()).close();
        verify(consumer2, never()).close();

        verify(producer1, never()).close();
        verify(producer2, never()).close();
    }

    /**
     * Test close when session is closing
     */
    @Test
    public void testDoCloseWhenClosing() throws InterruptedException {

        /*
         * Set up session and mocks
         */
        sqsSession.setClosing(true);
        final CountDownLatch beforeDoCloseCall = new CountDownLatch(1);
        final CountDownLatch passedDoCloseCall = new CountDownLatch(1);

        /*
         * call doClose in different thread
         */
        executorService.execute(() -> {
            try {
                beforeDoCloseCall.countDown();
                sqsSession.doClose();
                passedDoCloseCall.countDown();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        // Yield execution to allow the session to wait
        assertTrue(beforeDoCloseCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the lock and ensure that we are still waiting since the did not run
        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        assertFalse(passedDoCloseCall.await(2, TimeUnit.SECONDS));

        // Simulate session closed
        sqsSession.setClosed(true);

        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        passedDoCloseCall.await();

    }

    /**
     * Test do close
     */
    @Test
    public void testDoClose() throws JMSException {
        sqsSession = new SQSSession(parentSQSConnection, AcknowledgeMode.ACK_AUTO, messageConsumers, messageProducers);
        /*
         * Do close
         */
        sqsSession.doClose();

        /*
         * Verify results
         */
        verify(parentSQSConnection).removeSession(sqsSession);
        verify(consumer1).close();
        verify(consumer2).close();

        verify(producer1).close();
        verify(producer2).close();

        assertTrue(sqsSession.isClosed());
    }

    /**
     * Test close when session is already closed
     */
    @Test
    public void testCloseWhenAlreadyClosed() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        /*
         * Close
         */
        sqsSession.close();

        /*
         * Verify results
         */
        verify(sqsSession, never()).doClose();
    }

    /**
     * Test close
     */
    @Test
    public void testClose() throws JMSException, InterruptedException {

        /*
         * Set up the latches and mocks
         */
        final CountDownLatch beforeCloseCall = new CountDownLatch(1);
        final CountDownLatch passedCloseCall = new CountDownLatch(1);

        sqsSession.setActiveConsumerInCallback(consumer1);

        // Run thread that tries to close the session while activeConsumerInCallback is set
        executorService.execute(() -> {
            beforeCloseCall.countDown();
            try {
                sqsSession.close();
                passedCloseCall.countDown();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        beforeCloseCall.await();

        // Ensure that we wait on activeConsumerInCallback
        assertFalse(passedCloseCall.await(2, TimeUnit.SECONDS));

        // Release the activeConsumerInCallback
        sqsSession.finishedCallback();

        // Ensure that the session close completed
        passedCloseCall.await();

        assertTrue(sqsSession.isClosed());
    }

    /**
     * Test close from active callback thread is rejected
     */
    @Test
    public void testCloseFromActiveCallbackThread() throws JMSException, InterruptedException {

        /*
         * Set up session
         */
        sqsSession.start();
        sqsSession.startingCallback(consumer1);

        /*
         * Verify result
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.close());
    }

    /**
     * Test create receiver
     */
    @Test
    public void testCreateReceiver() throws JMSException {
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        /*
         * Create receiver
         */
        sqsSession.createReceiver(destination);
        sqsSession.createReceiver(destination, "message selector");

        /*
         * Verify results
         */
        verify(sqsSession, times(2)).createConsumer(destination);
    }

    /**
     * Test create sender
     */
    @Test
    public void testCreateSender() throws JMSException {
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        /*
         * Create receiver
         */
        sqsSession.createSender(destination);

        /*
         * Verify results
         */
        verify(sqsSession).createProducer(destination);
    }

    /**
     * Test create message
     */
    @Test
    public void testCreateMessage() {

        /*
         * Create  message
         */
        assertThrows(JMSException.class, () -> sqsSession.createMessage(), SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /**
     * Test create byte message
     */
    @Test
    public void testCreateObjectMessage() throws JMSException {

        sqsSession.setClosed(true);

        /*
         * Create bytes message
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.createObjectMessage(), "Session is closed");

        sqsSession.setClosed(false);

        ObjectMessage msg1 = sqsSession.createObjectMessage();

        assertTrue(msg1 instanceof SQSObjectMessage);

        String myObject = "MyObject";
        ObjectMessage msg2 = sqsSession.createObjectMessage(myObject);

        assertTrue(msg2 instanceof SQSObjectMessage);
        assertEquals(myObject, msg2.getObject());
    }

    /**
     * Test create byte message
     */
    @Test
    public void testCreateTextMessage() throws JMSException {

        sqsSession.setClosed(true);

        /*
         * Create bytes message
         */
        assertThrows(IllegalStateException.class, () -> sqsSession.createTextMessage(), "Session is closed");

        sqsSession.setClosed(false);

        TextMessage msg1 = sqsSession.createTextMessage();

        assertTrue(msg1 instanceof SQSTextMessage);

        String myText = "MyText";
        TextMessage msg2 = sqsSession.createTextMessage(myText);

        assertTrue(msg2 instanceof SQSTextMessage);
        assertEquals(myText, msg2.getText());
    }
}
