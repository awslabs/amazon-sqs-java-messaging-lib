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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.MessageListener;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSMessageConsumer;
import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.SQSSessionCallbackScheduler;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import com.amazon.sqs.javamessaging.util.SQSMessagingClientThreadFactory;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test the SQSMessageConsumerPrefetchTest class
 */
public class SQSMessageConsumerTest {

    private static final String QUEUE_URL_1 = "QueueUrl1";
    private static final String QUEUE_URL_2 = "queueUrl2";
    private static final String QUEUE_NAME = "QueueName";

    private SQSMessageConsumer consumer;
    private SQSConnection sqsConnection;
    private SQSSession sqsSession;
    private SQSSessionCallbackScheduler sqsSessionRunnable;
    private Acknowledger acknowledger;

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
    private SQSMessageConsumerPrefetch sqsMessageConsumerPrefetch;
    private NegativeAcknowledger negativeAcknowledger;
    private SQSMessagingClientThreadFactory threadFactory;
    private SQSQueueDestination destination;

    @Before
    public void setup() throws JMSException{

        sqsConnection = mock(SQSConnection.class);

        sqsSession = spy(new SQSSession(sqsConnection, AcknowledgeMode.ACK_AUTO));//mock(SQSSession.class);
        sqsSessionRunnable = mock(SQSSessionCallbackScheduler.class);

        acknowledger = mock(Acknowledger.class);

        negativeAcknowledger = mock(NegativeAcknowledger.class);

        threadFactory = new SQSMessagingClientThreadFactory("testTask", true);

        destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL_1);

        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                                          destination, acknowledger, negativeAcknowledger, threadFactory));


        sqsMessageConsumerPrefetch = mock(SQSMessageConsumerPrefetch.class);
    }

    /**
     * Test the message selector is not supported
     */
    @Test
    public void testGetMessageSelectorNotSupported() {
        
        try {
            consumer.getMessageSelector();
            fail();
        } catch(JMSException jmse) {
            assertEquals("Unsupported Method", jmse.getMessage());
        }
    }

    /**
     * Test stop is a no op if already closed
     */
    @Test
    public void testStopNoOpIfAlreadyClosed() throws JMSException {

        /*
         * Set up consumer
         */
        consumer.close();

        /*
         * stop consumer
         */
        consumer.stopPrefetch();

        /*
         * Verify results
         */
        verifyNoMoreInteractions(sqsMessageConsumerPrefetch);
    }

    /**
     * Test close blocks on in progress callback
     */
    @Test
    public void testCloseBlocksInProgressCallback() throws InterruptedException, JMSException {

        /*
         * Set up the latches
         */
        final CountDownLatch beforeConsumerStopCall = new CountDownLatch(1);
        final CountDownLatch passedConsumerStopCall = new CountDownLatch(1);

        consumer = new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch);

        sqsSession.start();
        sqsSession.startingCallback(consumer);

        // Run another thread that tries to close the consumer while activeConsumerInCallback is set
        executorService.execute(new Runnable() {

            @Override
            public void run() {
                beforeConsumerStopCall.countDown();
                try {
                    consumer.close();
                } catch (JMSException e) {
                    fail();
                }
                passedConsumerStopCall.countDown();
            }
        });

        beforeConsumerStopCall.await();
        Thread.sleep(10);
        // Ensure that we wait on activeConsumerInCallback
        assertEquals(false, passedConsumerStopCall.await(2, TimeUnit.SECONDS));

        // Release the activeConsumerInCallback
        sqsSession.finishedCallback();

        // Ensure that the consumer close completed
        passedConsumerStopCall.await();

        assertEquals(true, consumer.closed);
    }


    /**
     * Test Start is a no op if already closed
     */
    @Test
    public void testStartNoOpIfAlreadyClosed() throws JMSException {

        /*
         * Set up consumer
         */
        consumer.close();

        /*
         * start consumer
         */
        consumer.startPrefetch();

        /*
         * Verify results
         */
        verifyNoMoreInteractions(sqsMessageConsumerPrefetch);
    }

    /**
     * Test recover
     */
    @Test
    public void testRecover() throws InterruptedException, JMSException {

        /*
         * Set up the mocks
         */
        List<SQSMessageIdentifier> unAckIdentifiers = new ArrayList<SQSMessageIdentifier>();
        unAckIdentifiers.add(new SQSMessageIdentifier(QUEUE_URL_1, "r1", "messageId1"));
        unAckIdentifiers.add(new SQSMessageIdentifier(QUEUE_URL_2, "r2", "messageId2"));

        when(acknowledger.getUnAckMessages())
                .thenReturn(unAckIdentifiers);

        /*
         * Recover the consumer
         */
        consumer.recover();

        /*
         * Verify results
         */
        verify(negativeAcknowledger).bulkAction(unAckIdentifiers, unAckIdentifiers.size());
        verify(acknowledger).forgetUnAckMessages();
    }

    /**
     * Test do close results in no op when the consumer is already closed
     */
    @Test
    public void testDoCloseNoOpWhenAlreadyClosed() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer.closed = true;

        /*
         * Do close consumer
         */
        consumer.doClose();

        /*
         * Verify results
         */
        verifyNoMoreInteractions(sqsSession);
    }


    /**
     * Test do close
     */
    @Test
    public void testDoClose() throws InterruptedException, JMSException {

        consumer = new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch);

        /*
         * Do close consumer
         */
        consumer.doClose();

        /*
         * Verify results
         */
        verify(sqsSession).removeConsumer(consumer);
        verify(sqsMessageConsumerPrefetch).close();
    }


    /**
     * Test close results in no op when the consumer is already closed
     */
    @Test
    public void testCloseNoOpWhenAlreadyClosed() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer.closed = true;

        /*
         * Close consumer
         */
        consumer.close();

        /*
         * Verify results
         */

        verify(consumer, never()).doClose();
        verify(sqsSessionRunnable, never()).setConsumerCloseAfterCallback(any(SQSMessageConsumer.class));
    }

    /**
     * Test when consumer is closed by the message listener that is running on the callback thread
     * we do not close but set a consumer close after callback
     */
    @Test
    public void testCloseCalledFromCallbackExecutionThread() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        when(sqsSession.isActiveCallbackSessionThread())
                .thenReturn(true);

        /*
         * Close consumer
         */
        consumer.close();

        /*
         * Verify results
         */
        verify(consumer, never()).doClose();
        verify(sqsSessionRunnable).setConsumerCloseAfterCallback(consumer);
    }

    /**
     * Test consumer close
     */
    @Test
    public void testClose() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        /*
         * Close consumer
         */
        consumer.close();

        /*
         * Verify results
         */
        verify(consumer).doClose();
        verify(sqsSessionRunnable, never()).setConsumerCloseAfterCallback(consumer);
    }

    /**
     * Test set message listener fails when consumer is already closed
     */
    @Test
    public void testSetMessageListenerAlreadyClosed() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        consumer.close();

        MessageListener msgListener = mock(MessageListener.class);

        /*
         * Set message listener on a consumer
         */
        try {
            consumer.setMessageListener(msgListener);
            fail();
        } catch (JMSException ex) {
            assertEquals("Consumer is closed", ex.getMessage());
        }
    }

    /**
     * Test set message listener
     */
    @Test
    public void testSetMessageListener() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        MessageListener msgListener = mock(MessageListener.class);

        /*
         * Set message listener on a consumer
         */
        consumer.setMessageListener(msgListener);

        /*
         * Verify results
         */
        verify(sqsMessageConsumerPrefetch).setMessageListener(msgListener);
    }

    /**
     * Test get message listener
     */
    @Test
    public void testGetMessageListener() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        /*
         * Get message listener on a consumer
         */
        consumer.getMessageListener();

        /*
         * Verify results
         */
        verify(sqsMessageConsumerPrefetch).getMessageListener();
    }

    /**
     * Test get message listener
     */
    @Test
    public void testGetQueue() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        assertEquals(destination, consumer.getQueue());
    }

    /**
     * Test receive
     */
    @Test
    public void testReceive() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        /*
         * Call receive
         */
        consumer.receive();

        /*
         * Verify results
         */
        verify(sqsMessageConsumerPrefetch).receive();
    }

    /**
     * Test receive with timeout
     */
    @Test
    public void testReceiveWithTimeout() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        long timeout = 10;

        /*
         * Call receive with timeout
         */
        consumer.receive(timeout);

        /*
         * Verify results
         */
        verify(sqsMessageConsumerPrefetch).receive(timeout);
    }

    /**
     * Test receive no wait
     */
    @Test
    public void testReceiveNoWait() throws InterruptedException, JMSException {

        /*
         * Set up consumer
         */
        consumer = spy(new SQSMessageConsumer(sqsConnection, sqsSession, sqsSessionRunnable,
                destination, acknowledger, negativeAcknowledger, threadFactory, sqsMessageConsumerPrefetch));

        /*
         * Call receive no wait
         */
        consumer.receiveNoWait();

        /*
         * Verify results
         */
        verify(sqsMessageConsumerPrefetch).receiveNoWait();
    }
}