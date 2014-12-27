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
import com.amazon.sqs.javamessaging.PrefetchManager;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSMessageConsumer;
import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch;
import com.amazon.sqs.javamessaging.SQSMessageProducer;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.TextMessage;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
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
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
    private SQSMessageConsumer consumer1;
    private SQSMessageConsumer consumer2;
    private SQSMessageProducer producer1;
    private SQSMessageProducer producer2;
    private AmazonSQSMessagingClientWrapper sqsClientJMSWrapper;

    @Before
    public void setup() throws JMSException {

        sqsClientJMSWrapper = mock(AmazonSQSMessagingClientWrapper.class);

        parentSQSConnection = mock(SQSConnection.class);
        when(parentSQSConnection.getWrappedAmazonSQSClient())
                .thenReturn(sqsClientJMSWrapper);

        messageConsumers = new HashSet<SQSMessageConsumer>();
        messageProducers = new HashSet<SQSMessageProducer>();

        consumer1 = mock(SQSMessageConsumer.class);
        consumer2 = mock(SQSMessageConsumer.class);
        messageConsumers.add(consumer1);
        messageConsumers.add(consumer2);

        producer1 = mock(SQSMessageProducer.class);
        producer2 = mock(SQSMessageProducer.class);
        messageProducers.add(producer1);
        messageProducers.add(producer2);

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
        try {
            sqsSession.stop();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }

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
    public void testStopNoOpIfClosing() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);

        /*
         * stop consumer
         */
        try {
            sqsSession.stop();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed or closing", ise.getMessage());
        }

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
    public void testStartNoOpIfClosing() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);

        /*
         * stop consumer
         */
        try {
            sqsSession.start();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed or closing", ise.getMessage());
        }

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
        try {
            sqsSession.start();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }

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
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (sqsSession.getStateLock()) {
                        holdStateLock.countDown();
                        mainRelease.await();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to stop the session while state lock is been held
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                beforeSessionStopCall.countDown();
                try {
                    sqsSession.stop();
                    passedSessionStopCall.countDown();
                } catch (IllegalStateException e) {
                    e.printStackTrace();
                }
            }
        });

        beforeSessionStopCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock
        assertEquals(false, passedSessionStopCall.await(2, TimeUnit.SECONDS));

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
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (sqsSession.getStateLock()) {
                        holdStateLock.countDown();
                        mainRelease.await();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to start the session while state lock is been held
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeSessionStartCall.countDown();
                    sqsSession.start();
                    passedSessionStartCall.countDown();
                } catch (IllegalStateException e) {
                    e.printStackTrace();
                }
            }
        });

        beforeSessionStartCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock
        assertEquals(false, passedSessionStartCall.await(2, TimeUnit.SECONDS));

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

        try {
            sqsSession.commit();
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.rollback();
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.unsubscribe("test");
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createTopic("topic");
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createDurableSubscriber(null, "name");
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createDurableSubscriber(null, "name", "messageSelector", false);
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createBrowser(null);
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createBrowser(null, "messageSelector");
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createTemporaryQueue();
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createTemporaryTopic();
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.getMessageListener();
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.setMessageListener(null);
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createStreamMessage();
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }

        try {
            sqsSession.createMapMessage();
            fail();
        } catch (JMSException e) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, e.getMessage());
        }
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
        MessageListener msgListener = mock(MessageListener.class);
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
            public SQSMessageConsumer getMessageConsumer() {
                return consumer1;
            }
        };

        when(msgManager.getPrefetchManager())
                .thenReturn(prefetchManager);

        sqsSession.getSqsSessionRunnable().scheduleCallBack(null, msgManager);

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to close the consumer while state lock is been held
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                beforeSessionWaitCall.countDown();
                sqsSession.waitForCallbackComplete();
                passedSessionWaitCall.countDown();

            }
        });

        beforeSessionWaitCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock this time is longer then waitForAllCallbackComplete timeoutMillis input
        assertEquals(false, passedSessionWaitCall.await(1, TimeUnit.SECONDS));

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
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                beforeWaitCall.countDown();
                sqsSession.waitForCallbackComplete();
                passedWaitCall.countDown();
            }
        });

        // Yield execution to allow the consumer to wait
        assertEquals(true, beforeWaitCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the lock and ensure that we are still waiting since the prefetch message still equal to the limit
        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        assertEquals(false, passedWaitCall.await(1, TimeUnit.SECONDS));

        // Simulate callback finished
        sqsSession.finishedCallback();
        passedWaitCall.await();
    }

    /**
     * Test finishedCallback decrease callbackCounter
     */
    @Test
    public void testFinishedCallback() throws InterruptedException, JMSException {

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
        assertEquals(false, sqsSession.isCallbackActive());
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
        assertEquals(false, sqsSession.isCallbackActive());
    }

    /**
     * Test startingCallback is a no op if closing
     */
    @Test
    public void testStartingCallbackNoOpIfClosing() throws InterruptedException {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);
        sqsSession.setActiveConsumerInCallback(null);
        
        /*
         * Starting Callback
         */
        try {
            sqsSession.startingCallback(consumer1);
            fail();
        } catch (JMSException e) {
            e.printStackTrace();
        }
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
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeWaitCall.countDown();
                    sqsSession.startingCallback(consumer1);
                    passedWaitCall.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        // Yield execution to allow the session to wait
        assertEquals(true, beforeWaitCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the lock and ensure that we are still waiting since the did not run
        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        assertEquals(false, passedWaitCall.await(2, TimeUnit.SECONDS));

        // Simulate callback finished
        sqsSession.setRunning(true);

        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        passedWaitCall.await();
        assertEquals(true, sqsSession.isCallbackActive());
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
    public void testCreateQueueWhenAlreadyClosed() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        /*
         * Create queue
         */
        try {
            sqsSession.createQueue("Test");
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }
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
        assert(queue instanceof SQSQueueDestination);
        assertEquals(QUEUE_NAME, queue.getQueueName());
        assertEquals(QUEUE_URL, ((SQSQueueDestination) queue).getQueueUrl());
    }
    
    /**
     * Test create queue when session is already closed
     */
    @Test
    public void testCreateQueueWithOwnerAccountId() throws JMSException {

        GetQueueUrlResult result = new GetQueueUrlResult().withQueueUrl(QUEUE_URL);
        when(sqsClientJMSWrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID))
                .thenReturn(result);

        /*
         * Create queue
         */
        Queue queue = sqsSession.createQueue(QUEUE_NAME, OWNER_ACCOUNT_ID);

        /*
         * Verify results
         */
        assert(queue instanceof SQSQueueDestination);
        assertEquals(QUEUE_NAME, queue.getQueueName());
        assertEquals(QUEUE_URL, ((SQSQueueDestination) queue).getQueueUrl());
    }

    /**
     * Test create consumer when session is already closed
     */
    @Test
    public void testCreateConsumerProducerWhenAlreadyClosed() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        Destination destination = new Destination() {};

        /*
         * Create consumer
         */
        try {
            sqsSession.createConsumer(destination);
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }

        /*
         * Create producer
         */
        try {
            sqsSession.createProducer(destination);
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }
    }

    /**
     * Test create consumer when session is closing
     */
    @Test
    public void testCreateConsumerProducerWhenClosing() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosing(true);
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        /*
         * Create consumer
         */
        try {
            sqsSession.createConsumer(destination);
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed or closing", ise.getMessage());
        }

        /*
         * Create producer
         */
        try {
            sqsSession.createProducer(destination);
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed or closing", ise.getMessage());
        }
    }

    /**
     * Test create consumer when destination is non SQSQueueDestination
     */
    @Test
    public void testCreateConsumerProducerNonSQSQueueDestination() throws JMSException {

        /*
         * Set up session
         */
        Destination destination = new Destination() {};

        /*
         * Create consumer
         */
        try {
            sqsSession.createConsumer(destination);
            fail();
        } catch(JMSException jmse) {
            assertEquals("Actual type of Destination/Queue has to be SQSQueueDestination", jmse.getMessage());
        }

           /*
         * Create producer
         */
        try {
            sqsSession.createProducer(destination);
            fail();
        } catch(JMSException jmse) {
            assertEquals("Actual type of Destination/Queue has to be SQSQueueDestination", jmse.getMessage());
        }
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
        doReturn(consumer3)
                .when(sqsSession).createSQSMessageConsumer(destination);

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
        doReturn(consumer3)
                .when(sqsSession).createSQSMessageConsumer(destination);

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
     * Test create consumer with non null message selector
     */
    @Test
    public void testCreateConsumerNonNullMessageSelector() throws JMSException {

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
        try {
            sqsSession.createConsumer(destination, "Selector");
            fail();
        } catch(JMSException jmse) {
            assertEquals("SQSSession does not support MessageSelector. This should be null.", jmse.getMessage());
        }

        try {
            sqsSession.createConsumer(destination, "Selector", true);
            fail();
        } catch(JMSException jmse) {
            assertEquals("SQSSession does not support MessageSelector. This should be null.", jmse.getMessage());
        }
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
        doReturn(consumer3)
                .when(sqsSession).createSQSMessageConsumer(destination);

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
    public void testRecover() throws JMSException {

        /*
         * Recover
         */
        sqsSession.recover();

        verify(consumer1).recover();
        verify(consumer2).recover();
    }

    /**
     * Test recover when session is already closed
     */
    @Test
    public void testRecoverWhenAlreadyClosed() throws JMSException {

        /*
         * Set up session
         */
        sqsSession.setClosed(true);

        /*
         * Recover
         */
        try {
            sqsSession.recover();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }
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
    public void testDoCloseWhenClosing() throws JMSException, InterruptedException {

        /*
         * Set up session and mocks
         */
        sqsSession.setClosing(true);
        final CountDownLatch beforeDoCloseCall = new CountDownLatch(1);
        final CountDownLatch passedDoCloseCall = new CountDownLatch(1);

        /*
         * call doClose in different thread
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeDoCloseCall.countDown();
                    sqsSession.doClose();
                    passedDoCloseCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        // Yield execution to allow the session to wait
        assertEquals(true, beforeDoCloseCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the lock and ensure that we are still waiting since the did not run
        synchronized (sqsSession.getStateLock()) {
            sqsSession.getStateLock().notifyAll();
        }
        assertEquals(false, passedDoCloseCall.await(2, TimeUnit.SECONDS));

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
    public void testDoClose() throws JMSException, InterruptedException {

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
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                beforeCloseCall.countDown();
                try {
                    sqsSession.close();
                    passedCloseCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        beforeCloseCall.await();

        // Ensure that we wait on activeConsumerInCallback
        assertEquals(false, passedCloseCall.await(2, TimeUnit.SECONDS));

        // Release the activeConsumerInCallback
        sqsSession.finishedCallback();

        // Ensure that the session close completed
        passedCloseCall.await();

        assertEquals(true, sqsSession.isClosed());
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
        try {
            sqsSession.close();
            fail();
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    /**
     * Test create receiver
     */
    @Test
    public void testCreateReceiver() throws JMSException, InterruptedException {
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
    public void testCreateSender() throws JMSException, InterruptedException {
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
    public void testCreateMessage() throws JMSException, InterruptedException {

        /*
         * Create  message
         */
        try {
            sqsSession.createMessage();
            fail();
        } catch(JMSException jsme) {
            assertEquals(SQSMessagingClientConstants.UNSUPPORTED_METHOD, jsme.getMessage());
        }
    }

    /**
     * Test create byte message
     */
    @Test
    public void testCreateObjectMessage() throws JMSException, InterruptedException {

        sqsSession.setClosed(true);

        /*
         * Create bytes message
         */
        try {
            sqsSession.createObjectMessage();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }

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
    public void testCreateTextMessage() throws JMSException, InterruptedException {

        sqsSession.setClosed(true);

        /*
         * Create bytes message
         */
        try {
            sqsSession.createTextMessage();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Session is closed", ise.getMessage());
        }

        sqsSession.setClosed(false);

        TextMessage msg1 = sqsSession.createTextMessage();

        assertTrue(msg1 instanceof SQSTextMessage);

        String myText = "MyText";
        TextMessage msg2 = sqsSession.createTextMessage(myText);

        assertTrue(msg2 instanceof SQSTextMessage);
        assertEquals(myText, msg2.getText());
    }
}