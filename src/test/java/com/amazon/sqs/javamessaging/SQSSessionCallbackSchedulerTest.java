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
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.SQSSessionCallbackScheduler;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import com.amazon.sqs.javamessaging.message.SQSMessage;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSSessionCallbackSchedulerTest class
 */
public class SQSSessionCallbackSchedulerTest {

    private static final String QUEUE_URL_PREFIX = "QueueUrl";
    private static final String QUEUE_URL_1 = "QueueUrl1";
    private static final String QUEUE_URL_2 = "QueueUrl2";

    private SQSSession sqsSession;
    private NegativeAcknowledger negativeAcknowledger;
    private SQSSessionCallbackScheduler sqsSessionRunnable;
    private SQSConnection sqsConnection;
    private AmazonSQSMessagingClientWrapper sqsClient;
    private ArrayDeque<SQSSession.CallbackEntry> callbackQueue;
    private Acknowledger acknowledger;
    private SQSMessageConsumer consumer;
    
    @Before
    public void setup() {

        sqsClient = mock(AmazonSQSMessagingClientWrapper.class);

        sqsConnection = mock(SQSConnection.class);
        when(sqsConnection.getWrappedAmazonSQSClient())
                .thenReturn(sqsClient);

        sqsSession = mock(SQSSession.class);
        when(sqsSession.getParentConnection())
                .thenReturn(sqsConnection);

        negativeAcknowledger = mock(NegativeAcknowledger.class);

        callbackQueue = mock(ArrayDeque.class);

        acknowledger = mock(Acknowledger.class);
        
        consumer = mock(SQSMessageConsumer.class);

        sqsSessionRunnable = spy(new SQSSessionCallbackScheduler(sqsSession,
                AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.AUTO_ACKNOWLEDGE), acknowledger));

        sqsSessionRunnable.negativeAcknowledger = negativeAcknowledger;

        sqsSessionRunnable.callbackQueue = callbackQueue;
    }

    /**
     * Test nack queue messages when the queue is empty
     */
    @Test
    public void testNackQueueMessageWhenEmpty() throws JMSException {

        when(callbackQueue.isEmpty())
                .thenReturn(true);

        /*
         * Nack the messages
         */
        sqsSessionRunnable.nackQueuedMessages();

        /*
         * Verify results
         */
        verify(negativeAcknowledger, never()).bulkAction(anyList(), anyInt());
    }

    /**
     * Test nack queue messages does not propagate a JMS exception
     */
    @Test
    public void testNackQueueMessageAcknowledgerThrowJMSException() throws JMSException {

        MessageListener msgListener = mock(MessageListener.class);

        SQSMessage sqsMessage = mock(SQSMessage.class);
        when(sqsMessage.getReceiptHandle())
                .thenReturn("r1");
        when(sqsMessage.getSQSMessageId())
                .thenReturn("messageId1");
        when(sqsMessage.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        SQSMessageConsumerPrefetch.MessageManager msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager.getMessage())
                .thenReturn(sqsMessage);

        when(msgManager.getPrefetchManager())
                .thenReturn(mock(PrefetchManager.class));

        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(msgListener, msgManager);

        when(callbackQueue.isEmpty())
                .thenReturn(false)
                .thenReturn(true);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1);

        doThrow(new JMSException("Exception"))
                .when(negativeAcknowledger).bulkAction(anyList(), anyInt());

        /*
         * Nack the messages, no exception expected
         */
        sqsSessionRunnable.nackQueuedMessages();
    }

    /**
     * Test nack queue messages does propagate Errors
     */
    @Test
    public void testNackQueueMessageAcknowledgerThrowError() throws JMSException {

        MessageListener msgListener = mock(MessageListener.class);

        SQSMessage sqsMessage = mock(SQSMessage.class);
        when(sqsMessage.getReceiptHandle())
                .thenReturn("r2");
        when(sqsMessage.getSQSMessageId())
                .thenReturn("messageId2");
        when(sqsMessage.getQueueUrl())
                .thenReturn(QUEUE_URL_2);

        SQSMessageConsumerPrefetch.MessageManager msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager.getMessage())
                .thenReturn(sqsMessage);

        when(msgManager.getPrefetchManager())
                .thenReturn(mock(PrefetchManager.class));


        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(msgListener, msgManager);

        when(callbackQueue.isEmpty())
                .thenReturn(false)
                .thenReturn(true);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1);

        doThrow(new Error("error"))
                .when(negativeAcknowledger).bulkAction(anyList(), anyInt());

        /*
         * Nack the messages, exception expected
         */
        try {
            sqsSessionRunnable.nackQueuedMessages();
            fail();
        } catch(Error e) {
            // expected error
        }
    }

    /**
     * Test nack Queue Message
     */
    @Test
    public void testNackQueueMessage() throws JMSException {

        /*
         * Set up mocks
         */
        MessageListener msgListener = mock(MessageListener.class);

        SQSMessage sqsMessage1 = mock(SQSMessage.class);
        when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        when(sqsMessage1.getSQSMessageId())
                .thenReturn("messageId1");
        when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);

        when(msgManager1.getPrefetchManager())
                .thenReturn(mock(PrefetchManager.class));

        SQSMessage sqsMessage2 = mock(SQSMessage.class);
        when(sqsMessage2.getReceiptHandle())
                .thenReturn("r2");
        when(sqsMessage2.getSQSMessageId())
                .thenReturn("messageId2");
        when(sqsMessage2.getQueueUrl())
                .thenReturn(QUEUE_URL_2);

        SQSMessageConsumerPrefetch.MessageManager msgManager2 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager2.getMessage())
                .thenReturn(sqsMessage2);

        when(msgManager2.getPrefetchManager())
                .thenReturn(mock(PrefetchManager.class));

        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(msgListener, msgManager1);
        SQSSession.CallbackEntry entry2 = new SQSSession.CallbackEntry(msgListener, msgManager2);

        when(callbackQueue.isEmpty())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);

        List<SQSMessageIdentifier> nackMessageIdentifiers = new ArrayList<SQSMessageIdentifier>();
        nackMessageIdentifiers.add(new SQSMessageIdentifier(QUEUE_URL_1, "r1", "messageId1"));
        nackMessageIdentifiers.add(new SQSMessageIdentifier(QUEUE_URL_2, "r2", "messageId2"));

        /*
         * Nack the messages
         */
        sqsSessionRunnable.nackQueuedMessages();

        /*
         * Verify results
         */
        verify(negativeAcknowledger).bulkAction(eq(nackMessageIdentifiers), eq(2));
    }

    /**
     * Test starting callback does not propagate Interrupted Exception
     */
    @Test
    public void testStartingCallbackThrowJMSException() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        doThrow(new JMSException("closed"))
                .when(sqsSession).startingCallback(consumer);

        doNothing()
                .when(sqsSessionRunnable).nackQueuedMessages();

        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager1.getMessage())
                .thenReturn(mock(SQSMessage.class));
        when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(null, msgManager1);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1);

        /*
         * Nack the messages, exit the loop
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        verify(sqsSession).startingCallback(consumer);
        verify(sqsSessionRunnable).nackQueuedMessages();
        verify(sqsSession, never()).finishedCallback();
    }

    /**
     * Test callback run execution when call back entry message listener is empty
     */
    @Test
    public void testCallbackQueueEntryMessageListenerEmpty() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        doNothing()
        .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(any(SQSMessageConsumer.class));

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = createMessageManager(1);
        SQSMessageConsumerPrefetch.MessageManager msgManager2 = createMessageManager(2);

        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(null, msgManager1);
        SQSSession.CallbackEntry entry2 = new SQSSession.CallbackEntry(null, msgManager2);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);

        when(callbackQueue.isEmpty())
                .thenReturn(true);

        // Setup ConsumerCloseAfterCallback
        SQSMessageConsumer messageConsumer = mock(SQSMessageConsumer.class);
        sqsSessionRunnable.setConsumerCloseAfterCallback(messageConsumer);

        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        verify(sqsSession, times(2)).startingCallback(consumer);
        verify(sqsSessionRunnable).nackQueuedMessages();

        // Verify that we nack the message
        verify(negativeAcknowledger).action(QUEUE_URL_1, Collections.singletonList("r1"));
        verify(negativeAcknowledger).action(QUEUE_URL_2, Collections.singletonList("r2"));

        // Verify do close is called on set ConsumerCloseAfterCallback
        verify(messageConsumer).doClose();

        verify(sqsSession).finishedCallback();
    }

    /**
     * Test callback run execution when message ack throws a JMS exception
     */
    @Test
    public void testCallbackQueueEntryMessageAckThrowsJMSException() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        doNothing()
        .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        SQSMessage sqsMessage1 = mock(SQSMessage.class);
        when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        when(sqsMessage1.getSQSMessageId())
                .thenReturn("messageId1");
        when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);
        when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        // Throw an exception when try to acknowledge the message
        doThrow(new JMSException("Exception"))
                .when(sqsMessage1).acknowledge();

        MessageListener msgListener = mock(MessageListener.class);
        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(msgListener, msgManager1);

        SQSMessageConsumerPrefetch.MessageManager msgManager2 = createMessageManager(2);
        SQSSession.CallbackEntry entry2 = new SQSSession.CallbackEntry(msgListener, msgManager2);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);

        when(callbackQueue.isEmpty())
                .thenReturn(true);

        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        verify(sqsSession, times(2)).startingCallback(consumer);
        verify(sqsSessionRunnable).nackQueuedMessages();

        verify(sqsMessage1).acknowledge();
        // Verify that we nack the message
        verify(negativeAcknowledger).action(QUEUE_URL_1, Collections.singletonList("r1"));
        verify(negativeAcknowledger).action(QUEUE_URL_2, Collections.singletonList("r2"));

        verify(sqsSession).finishedCallback();
    }


    /**
     * Test callback run execution when message nack throws a JMS exception
     */
    @Test
    public void testCallbackQueueEntryMessageNAckThrowsJMSException() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        doNothing()
        .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        SQSMessage sqsMessage1 = mock(SQSMessage.class);
        when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        when(sqsMessage1.getSQSMessageId())
                .thenReturn("messageId1");
        when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);
        when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        // Set message listener as null to force a nack
        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(null, msgManager1);

        SQSMessageConsumerPrefetch.MessageManager msgManager2 = createMessageManager(2);
        SQSSession.CallbackEntry entry2 = new SQSSession.CallbackEntry(null, msgManager2);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);
        when(callbackQueue.isEmpty())
                .thenReturn(true);

        // Throw an exception when try to negative acknowledge the message
        doThrow(new JMSException("Exception"))
                .when(negativeAcknowledger).action(QUEUE_URL_1, Collections.singletonList("r1"));

        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        verify(sqsSession, times(2)).startingCallback(consumer);
        verify(sqsSessionRunnable).nackQueuedMessages();
        verify(negativeAcknowledger).action(QUEUE_URL_1, Collections.singletonList("r1"));
        verify(negativeAcknowledger).action(QUEUE_URL_2, Collections.singletonList("r2"));
        verify(sqsSession).finishedCallback();
    }

    /**
     * Test schedule callback
     */
    @Test
    public void testScheduleCallBack() throws JMSException, InterruptedException {

        /*
         * Set up mocks
         */
        sqsSessionRunnable.callbackQueue = new ArrayDeque<SQSSession.CallbackEntry>();

        MessageListener msgListener = mock(MessageListener.class);
        SQSMessageConsumerPrefetch.MessageManager msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.scheduleCallBack(msgListener, msgManager);

        assertEquals(1, sqsSessionRunnable.callbackQueue.size());

        SQSSession.CallbackEntry entry = sqsSessionRunnable.callbackQueue.pollFirst();

        assertEquals(msgListener, entry.getMessageListener());
        assertEquals(msgManager, entry.getMessageManager());
    }

    /**
     * Test that no auto ack messages occurs when client acknowledge is set
     */
    @Test
    public void testMessageNotAckWithClientAckMode() throws JMSException, InterruptedException {

        /**
         * Set up mocks
         */
        sqsSessionRunnable = spy(new SQSSessionCallbackScheduler(sqsSession,
                                        AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE),
                                        acknowledger));
        sqsSessionRunnable.callbackQueue = callbackQueue;

        doNothing()
                .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        SQSMessage sqsMessage1 = mock(SQSMessage.class);
        when(sqsMessage1.getReceiptHandle())
                .thenReturn("r1");
        when(sqsMessage1.getSQSMessageId())
                .thenReturn("messageId1");
        when(sqsMessage1.getQueueUrl())
                .thenReturn(QUEUE_URL_1);

        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager1.getMessage())
                .thenReturn(sqsMessage1);
        when(msgManager1.getPrefetchManager())
                .thenReturn(prefetchManager);

        MessageListener msgListener = mock(MessageListener.class);
        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(msgListener, msgManager1);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1);
        when(callbackQueue.isEmpty())
                .thenReturn(true);

        /*
         * Start the callback
         */
        sqsSessionRunnable.run();

        /*
         * Verify results
         */
        verify(sqsSession, times(2)).startingCallback(consumer);
        verify(sqsSessionRunnable).nackQueuedMessages();

        // Verify that do not ack the message
        verify(sqsMessage1, never()).acknowledge();
        verify(negativeAcknowledger, never()).action(QUEUE_URL_1, Collections.singletonList("r1"));
        verify(sqsSession).finishedCallback();
    }

    private SQSMessageConsumerPrefetch.MessageManager createMessageManager(int index) {
        SQSMessage sqsMessage = mock(SQSMessage.class);
        when(sqsMessage.getReceiptHandle())
                .thenReturn("r" + index);
        when(sqsMessage.getSQSMessageId())
                .thenReturn("messageId" + index);
        when(sqsMessage.getQueueUrl())
                .thenReturn(QUEUE_URL_PREFIX + index);

        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        when(prefetchManager.getMessageConsumer())
                .thenReturn(consumer);

        SQSMessageConsumerPrefetch.MessageManager msgManager = mock(SQSMessageConsumerPrefetch.MessageManager.class);
        when(msgManager.getMessage())
                .thenReturn(sqsMessage);
        when(msgManager.getPrefetchManager())
                .thenReturn(prefetchManager);
        return msgManager;
    }
}
