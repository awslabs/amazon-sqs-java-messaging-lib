/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazonaws.services.sqs.model.Message;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

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
                AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.AUTO_ACKNOWLEDGE), acknowledger, negativeAcknowledger));

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
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(negativeAcknowledger, times(2)).bulkAction(captor.capture(), eq(1));
        List allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = (List<SQSMessageIdentifier>)allCaptured.get(0);
        assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = (List<SQSMessageIdentifier>)allCaptured.get(1);
        assertEquals(QUEUE_URL_2, captured.get(0).getQueueUrl());
        assertEquals("r2", captured.get(0).getReceiptHandle());

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
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(negativeAcknowledger, times(2)).bulkAction(captor.capture(), eq(1));
        List allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = (List<SQSMessageIdentifier>)allCaptured.get(0);
        assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = (List<SQSMessageIdentifier>)allCaptured.get(1);
        assertEquals(QUEUE_URL_2, captured.get(0).getQueueUrl());
        assertEquals("r2", captured.get(0).getReceiptHandle());

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
        
        ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
        verify(negativeAcknowledger, times(2)).bulkAction(captor.capture(), eq(1));
        List allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = (List<SQSMessageIdentifier>)allCaptured.get(0);
        assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = (List<SQSMessageIdentifier>)allCaptured.get(1);
        assertEquals(QUEUE_URL_2, captured.get(0).getQueueUrl());
        assertEquals("r2", captured.get(0).getReceiptHandle());
        
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
        sqsSessionRunnable.scheduleCallBacks(msgListener, Collections.singletonList(msgManager));

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
                                        acknowledger, negativeAcknowledger));
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

    /**
     * Test that no auto ack messages occurs when client acknowledge is set
     */
    @Test
    public void testWhenListenerThrowsWhenAutoAckThenCallbackQueuePurgedFromMessagesWithSameQueueAndGroup() throws JMSException, InterruptedException {

        /**
         * Set up mocks
         */
        sqsSessionRunnable = spy(new SQSSessionCallbackScheduler(sqsSession,
                                        AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.AUTO_ACKNOWLEDGE),
                                        acknowledger, negativeAcknowledger));

        MessageListener messageListener = mock(MessageListener.class);
        doThrow(RuntimeException.class)
            .when(messageListener).onMessage(any(javax.jms.Message.class));
        
        List<SQSMessageConsumerPrefetch.MessageManager> messages = new ArrayList<SQSMessageConsumerPrefetch.MessageManager>();
        messages.add(createFifoMessageManager("queue1", "group1", "message1", "handle1"));
        messages.add(createFifoMessageManager("queue1", "group1", "message2", "handle2"));
        messages.add(createFifoMessageManager("queue2", "group1", "message3", "handle3"));
        messages.add(createFifoMessageManager("queue1", "group2", "message4", "handle4"));
        messages.add(createFifoMessageManager("queue1", "group1", "message5", "handle5"));
        messages.add(createFifoMessageManager("queue2", "group2", "message6", "handle6"));
        sqsSessionRunnable.scheduleCallBacks(messageListener, messages);
        
        doNothing()
            .doThrow(new JMSException("Closing"))
            .when(sqsSession).startingCallback(consumer);
        
        sqsSessionRunnable.run();
        
        ArgumentCaptor<List> messageIdentifierListCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Integer> indexOfMessageCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(negativeAcknowledger, times(3)).bulkAction(messageIdentifierListCaptor.capture(), indexOfMessageCaptor.capture());
        List<SQSMessageIdentifier> nackedMessages = messageIdentifierListCaptor.getAllValues().get(0);
        int nackedMessagesSize = indexOfMessageCaptor.getAllValues().get(0).intValue();
        
        //failing to process 'message1' should nack all messages for queue1 and group1, that is 'message1', 'message2' and 'message5'
        assertEquals(3, nackedMessagesSize);
        assertEquals("message1", nackedMessages.get(0).getSQSMessageID());
        assertEquals("message2", nackedMessages.get(1).getSQSMessageID());
        assertEquals("message5", nackedMessages.get(2).getSQSMessageID());
    }

    private SQSMessageConsumerPrefetch.MessageManager createFifoMessageManager(String queueUrl, String groupId, String messageId, String receiptHandle) throws JMSException {
        Message message = new Message();
        message.setBody("body");
        message.setMessageId(messageId);
        message.setReceiptHandle(receiptHandle);
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(SQSMessagingClientConstants.SEQUENCE_NUMBER, "728374687246872364");
        attributes.put(SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, messageId);
        attributes.put(SQSMessagingClientConstants.MESSAGE_GROUP_ID, groupId);
        attributes.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "0");
        message.setAttributes(attributes);
        SQSMessage sqsMessage = new SQSTextMessage(acknowledger, queueUrl, message);
        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        when(prefetchManager.getMessageConsumer())
            .thenReturn(consumer);
        SQSMessageConsumerPrefetch.MessageManager msgManager = new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, sqsMessage);
        return msgManager;
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
