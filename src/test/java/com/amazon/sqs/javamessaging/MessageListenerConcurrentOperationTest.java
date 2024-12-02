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
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test concurrent operation of message listener on session and connections
 */
public class MessageListenerConcurrentOperationTest {

    private static final String QUEUE_URL = "queueUrl";
    private static final String QUEUE_NAME = "queueName";
    private static final int NUMBER_OF_MESSAGES_TO_PREFETCH = 10;

    private AmazonSQSMessagingClient amazonSQSClient;
    private SQSMessageConsumerPrefetch.MessageManager msgManager;
    private volatile SQSSession session;
    private volatile SQSConnection connection;

    /**
     * Message listener that creates a producer on the session
     */
    private final MessageListener msgListenerCreatesProducer = new MessageListener() {
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
    private final MessageListener msgListenerCreatesConsumer = new MessageListener() {
        @Override
        public void onMessage(Message message) {
            try {
                session.createProducer(new SQSQueueDestination(QUEUE_NAME, QUEUE_URL));
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    };

    @BeforeEach
    public void Setup() throws JMSException {
        Acknowledger acknowledger = mock(Acknowledger.class);
        NegativeAcknowledger negativeAcknowledger = mock(NegativeAcknowledger.class);
        SQSQueueDestination sqsDestination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);
        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);

        connection = new SQSConnection(amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
        session = new SQSSession(connection, AcknowledgeMode.ACK_AUTO);
        SQSSessionCallbackScheduler sqsSessionRunnable = new SQSSessionCallbackScheduler(session,
                AcknowledgeMode.ACK_AUTO, acknowledger, negativeAcknowledger);

        SQSMessageConsumer consumer = mock(SQSMessageConsumer.class);

        SQSMessageConsumerPrefetch preFetcher = new SQSMessageConsumerPrefetch(sqsSessionRunnable, acknowledger,
                negativeAcknowledger, sqsDestination, amazonSQSClient, NUMBER_OF_MESSAGES_TO_PREFETCH);
        preFetcher.setMessageConsumer(consumer);

        msgManager = new SQSMessageConsumerPrefetch.MessageManager(preFetcher, mock(SQSMessage.class));
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

    public void testConcurrentExecution(final MessageListener msgListener, final ConcurrentOperation operation)
            throws JMSException, InterruptedException {
        // Start the session
        operation.setup();

        final CyclicBarrier startBarrier = new CyclicBarrier(2);
        final CountDownLatch finishLatch = new CountDownLatch(1);

        Thread t1 = new Thread(() -> {
            try {
                startBarrier.await();
                operation.applyOperation();
            } catch (JMSException | InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            finishLatch.countDown();
        });

        // Start the callback scheduler
        Thread t2 = new Thread(() -> {
            try {
                startBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
            session.getSqsSessionRunnable().scheduleCallBacks(msgListener, Collections.singletonList(msgManager));
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

        void setup() throws JMSException;

        void applyOperation() throws JMSException;

        void verify();

    }
}
