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
import com.amazon.sqs.javamessaging.SQSMessageConsumer;
import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.SQSSessionCallbackScheduler;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSMessage;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test concurrent operation of message listener on session and connections
 */
public class MessageListenerConcurrentOperationTest {

    private static final String QUEUE_URL = "queueUrl";
    private static final String QUEUE_NAME = "queueName";
    private static final int NUMBER_OF_MESSAGES_TO_PREFETCH = 10;

    private AmazonSQSMessagingClientWrapper amazonSQSClient;
    private SQSMessageConsumerPrefetch.MessageManager msgManager;
    private volatile SQSSession session;
    private volatile SQSConnection connection;

    /**
     * Message listener that creates a producer on the session
     */
    private MessageListener msgListenerCreatesProducer = new MessageListener() {
        @Override
        public void onMessage(Message message) {
            try {
                session.createProducer(new SQSQueueDestination(QUEUE_NAME, QUEUE_URL));
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    };

    /**
     * Message listener that creates a consumer on the session
     */
    private MessageListener msgListenerCreatesConsumer = new MessageListener() {
        @Override
        public void onMessage(Message message) {
            try {
                session.createProducer(new SQSQueueDestination(QUEUE_NAME, QUEUE_URL));
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    };

    @Before
    public void Setup() throws JMSException {

        Acknowledger acknowledger = mock(Acknowledger.class);
        NegativeAcknowledger negativeAcknowledger = mock(NegativeAcknowledger.class);
        SQSQueueDestination sqsDestination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);

        connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
        session = new SQSSession(connection, AcknowledgeMode.ACK_AUTO);
        SQSSessionCallbackScheduler sqsSessionRunnable = new SQSSessionCallbackScheduler(session, AcknowledgeMode.ACK_AUTO, acknowledger);

        SQSMessageConsumer consumer = mock(SQSMessageConsumer.class);

        SQSMessageConsumerPrefetch preftecher = new SQSMessageConsumerPrefetch(sqsSessionRunnable, acknowledger, negativeAcknowledger, sqsDestination,
                amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
        preftecher.setMessageConsumer(consumer);

        msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager.getMessage())
                .thenReturn(mock(SQSMessage.class));
        when(msgManager.getPrefetchManager())
                .thenReturn(preftecher);
    }

    /**
     * Test concurrent operation on message listener creating producer and consumer when
     * session is concurrently closed
     */
    @Test
    public void testConcurrentSessionCloseOperation() throws JMSException, InterruptedException {

        ConcurrentOperation closeSessionOperation = new ConcurrentOperation() {
            @Override
            public void setup() throws IllegalStateException {
                session.start();
            }

            @Override
            public void applyOperation() throws JMSException {
                session.close();
            }

            @Override
            public void verify() {
                assertTrue(session.isClosed());
                assertFalse(session.isRunning());
            }
        };

        // Test session close operation with create producer operation
        for (int i = 0; i < 10; ++i) {

            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesProducer, closeSessionOperation);
            connection.close();
        }

        // Test session close operation with create consumer operation
        for (int i = 0; i < 10; ++i) {

            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesConsumer, closeSessionOperation);
            connection.close();
        }
    }

    /**
     * Test concurrent operation on message listener creating producer and consumer when
     * connection is concurrently started
     */
    @Test
    public void testConcurrentConnectionStartOperation() throws JMSException, InterruptedException {

        ConcurrentOperation startConnectionOperation = new ConcurrentOperation() {
            @Override
            public void setup() throws JMSException {
                session.start();
            }

            @Override
            public void applyOperation() throws JMSException {
                connection.start();
            }

            @Override
            public void verify() {
                assertFalse(connection.isClosed());
                assertTrue(connection.isRunning());
            }
        };

        // Test connection start operation with create producer operation
        for (int i = 0; i < 10; ++i) {

            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesProducer, startConnectionOperation);
            connection.close();
        }

        // Test connection start operation with create consumer operation
        for (int i = 0; i < 10; ++i) {

            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesConsumer, startConnectionOperation);
            connection.close();
        }
    }

    /**
     * Test concurrent operation on message listener creating producer and consumer when
     * connection is concurrently closed
     */
    @Test
    public void testConcurrentConnectionCloseOperation() throws JMSException, InterruptedException {

        ConcurrentOperation closeConnectionOperation = new ConcurrentOperation() {
            @Override
            public void setup() throws JMSException {
                connection.start();
            }

            @Override
            public void applyOperation() throws JMSException {
                connection.close();
            }

            @Override
            public void verify() {
                assertTrue(connection.isClosed());
            }
        };

        // Test connection close operation with create producer operation
        for (int i = 0; i < 10; ++i) {
            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesProducer, closeConnectionOperation);
        }

        // Test connection close operation with create consumer operation
        for (int i = 0; i < 10; ++i) {
            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesConsumer, closeConnectionOperation);
        }
    }

    /**
     * Test concurrent operation on message listener creating producer and consumer when
     * connection is concurrently stopped
     */
    @Test
    public void testConcurrentConnectionStopOperation() throws JMSException, InterruptedException {

        ConcurrentOperation stopConnectionOperation = new ConcurrentOperation() {
            @Override
            public void setup() throws JMSException {
                session.start();
                connection.start();
            }

            @Override
            public void applyOperation() throws JMSException {
                connection.stop();
            }

            @Override
            public void verify() {
                assertFalse(connection.isClosed());
                assertFalse(connection.isRunning());
            }
        };


        // Test connection stop operation with create producer operation
        for (int i = 0; i < 10; ++i) {

            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesProducer, stopConnectionOperation);
            connection.close();
        }

        // Test connection stop operation with create consumer operation
        for (int i = 0; i < 10; ++i) {

            connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
            session = (SQSSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            testConcurrentExecution(msgListenerCreatesConsumer, stopConnectionOperation);
            connection.close();
        }
    }

    public void testConcurrentExecution(final MessageListener msgListener, final ConcurrentOperation operation) throws JMSException, InterruptedException {

        long start = System.currentTimeMillis();

        // Start the session
        operation.setup();

        final CyclicBarrier startBarrier = new CyclicBarrier(2);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    startBarrier.await();
                    operation.applyOperation();
                } catch (JMSException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                finishLatch.countDown();
            }
        });

        // Start the callback scheduler
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    startBarrier.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (BrokenBarrierException e) {
                    e.printStackTrace();
                }
                session.getSqsSessionRunnable().scheduleCallBack(msgListener, msgManager);
            }
        });

        t1.start();
        t2.start();

        finishLatch.await();

        operation.verify();
    }

    /**
     * Interface for the tested concurrent operation
     */
    private interface ConcurrentOperation {

        public abstract void setup() throws JMSException;

        public abstract void applyOperation() throws JMSException;

        public abstract void verify();

    }
}
