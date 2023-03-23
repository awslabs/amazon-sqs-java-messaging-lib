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
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import jakarta.jms.JMSException;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the SQSSessionCallbackSchedulerTest class
 */
@ExtendWith(MockitoExtension.class)
public class SQSSessionCallbackSchedulerTest {

    private static final String QUEUE_URL_PREFIX = "QueueUrl";
    private static final String QUEUE_URL_1 = "QueueUrl1";
    private static final String QUEUE_URL_2 = "QueueUrl2";

    private SQSSession sqsSession;
    private NegativeAcknowledger negativeAcknowledger;
    private SQSSessionCallbackScheduler sqsSessionRunnable;
    private ArrayDeque<SQSSession.CallbackEntry> callbackQueue;
    private Acknowledger acknowledger;
    private SQSMessageConsumer consumer;

    @Captor
    ArgumentCaptor<List<SQSMessageIdentifier>> captor;

    @BeforeEach
    public void setup() {
        sqsSession = mock(SQSSession.class);
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

        SQSMessageConsumerPrefetch.MessageManager msgManager = new SQSMessageConsumerPrefetch.MessageManager(mock(PrefetchManager.class), sqsMessage);

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

        SQSMessageConsumerPrefetch.MessageManager msgManager = new SQSMessageConsumerPrefetch.MessageManager(mock(PrefetchManager.class), sqsMessage);

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
        assertThatThrownBy(() -> sqsSessionRunnable.nackQueuedMessages()).isInstanceOf(Error.class);
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

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = new SQSMessageConsumerPrefetch.MessageManager(mock(PrefetchManager.class), sqsMessage1);

        SQSMessage sqsMessage2 = mock(SQSMessage.class);
        when(sqsMessage2.getReceiptHandle())
                .thenReturn("r2");
        when(sqsMessage2.getSQSMessageId())
                .thenReturn("messageId2");
        when(sqsMessage2.getQueueUrl())
                .thenReturn(QUEUE_URL_2);

        SQSMessageConsumerPrefetch.MessageManager msgManager2 = new SQSMessageConsumerPrefetch.MessageManager(mock(PrefetchManager.class), sqsMessage2);

        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(msgListener, msgManager1);
        SQSSession.CallbackEntry entry2 = new SQSSession.CallbackEntry(msgListener, msgManager2);

        when(callbackQueue.isEmpty())
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        when(callbackQueue.pollFirst())
                .thenReturn(entry1)
                .thenReturn(entry2);

        List<SQSMessageIdentifier> nackMessageIdentifiers = List.of(
                new SQSMessageIdentifier(QUEUE_URL_1, "r1", "messageId1"),
                new SQSMessageIdentifier(QUEUE_URL_2, "r2", "messageId2"));

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

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, mock(SQSMessage.class));

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
        verify(negativeAcknowledger, times(2)).bulkAction(captor.capture(), eq(1));
        List<List<SQSMessageIdentifier>> allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = allCaptured.get(0);
        assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = allCaptured.get(1);
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

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, sqsMessage1);

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
        verify(negativeAcknowledger, times(2)).bulkAction(captor.capture(), eq(1));
        List<List<SQSMessageIdentifier>> allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = allCaptured.get(0);
        assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = allCaptured.get(1);
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

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, sqsMessage1);

        // Set message listener as null to force a nack
        SQSSession.CallbackEntry entry1 = new SQSSession.CallbackEntry(null, msgManager1);

        SQSMessageConsumerPrefetch.MessageManager msgManager2 = createMessageManager(2);
        SQSSession.CallbackEntry entry2 = new SQSSession.CallbackEntry(null, msgManager2);

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

        verify(negativeAcknowledger, times(2)).bulkAction(captor.capture(), eq(1));
        List<List<SQSMessageIdentifier>> allCaptured = captor.getAllValues();
        List<SQSMessageIdentifier> captured = allCaptured.get(0);
        assertEquals(QUEUE_URL_1, captured.get(0).getQueueUrl());
        assertEquals("r1", captured.get(0).getReceiptHandle());
        captured = allCaptured.get(1);
        assertEquals(QUEUE_URL_2, captured.get(0).getQueueUrl());
        assertEquals("r2", captured.get(0).getReceiptHandle());

        verify(sqsSession).finishedCallback();
    }

    /**
     * Test schedule callback
     */
    @Test
    public void testScheduleCallBack() {
        /*
         * Set up mocks
         */
        sqsSessionRunnable.callbackQueue = new ArrayDeque<>();

        MessageListener msgListener = mock(MessageListener.class);
        SQSMessageConsumerPrefetch.MessageManager msgManager = new SQSMessageConsumerPrefetch.MessageManager(null, null);
        /*
         * Nack the messages, exception expected
         */
        sqsSessionRunnable.scheduleCallBacks(msgListener, Collections.singletonList(msgManager));

        assertEquals(1, sqsSessionRunnable.callbackQueue.size());

        SQSSession.CallbackEntry entry = sqsSessionRunnable.callbackQueue.pollFirst();

        assertEquals(msgListener, entry.messageListener());
        assertEquals(msgManager, entry.messageManager());
    }

    /**
     * Test that no auto ack messages occurs when client acknowledge is set
     */
    @Test
    public void testMessageNotAckWithClientAckMode() throws JMSException, InterruptedException {
        /*
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

        SQSMessageConsumerPrefetch.MessageManager msgManager1 = new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, sqsMessage1);

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
    public void testWhenListenerThrowsWhenAutoAckThenCallbackQueuePurgedFromMessagesWithSameQueueAndGroup()
            throws JMSException, InterruptedException {
        /*
         * Set up mocks
         */
        sqsSessionRunnable = spy(new SQSSessionCallbackScheduler(sqsSession,
                AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(Session.AUTO_ACKNOWLEDGE),
                acknowledger, negativeAcknowledger));

        MessageListener messageListener = mock(MessageListener.class);
        doThrow(RuntimeException.class)
                .when(messageListener).onMessage(any(jakarta.jms.Message.class));

        List<SQSMessageConsumerPrefetch.MessageManager> messages = List.of(
                createFifoMessageManager("queue1", "group1", "message1", "handle1"),
                createFifoMessageManager("queue1", "group1", "message2", "handle2"),
                createFifoMessageManager("queue2", "group1", "message3", "handle3"),
                createFifoMessageManager("queue1", "group2", "message4", "handle4"),
                createFifoMessageManager("queue1", "group1", "message5", "handle5"),
                createFifoMessageManager("queue2", "group2", "message6", "handle6"));
        sqsSessionRunnable.scheduleCallBacks(messageListener, messages);

        doNothing()
                .doThrow(new JMSException("Closing"))
                .when(sqsSession).startingCallback(consumer);

        sqsSessionRunnable.run();

        ArgumentCaptor<Integer> indexOfMessageCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(negativeAcknowledger, times(3)).bulkAction(
                captor.capture(), indexOfMessageCaptor.capture());
        List<SQSMessageIdentifier> nackedMessages = captor.getAllValues().get(0);
        int nackedMessagesSize = indexOfMessageCaptor.getAllValues().get(0);

        //failing to process 'message1' should nack all messages for queue1 and group1, that is 'message1', 'message2' and 'message5'
        assertEquals(3, nackedMessagesSize);
        assertEquals("message1", nackedMessages.get(0).getSQSMessageID());
        assertEquals("message2", nackedMessages.get(1).getSQSMessageID());
        assertEquals("message5", nackedMessages.get(2).getSQSMessageID());
    }

    private SQSMessageConsumerPrefetch.MessageManager createFifoMessageManager(
            String queueUrl, String groupId, String messageId, String receiptHandle) throws JMSException {
        Map<String, String> attributes = Map.of(
                SQSMessagingClientConstants.SEQUENCE_NUMBER, "728374687246872364",
                SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID, messageId,
                SQSMessagingClientConstants.MESSAGE_GROUP_ID, groupId,
                SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "0");
        Message message = Message.builder()
                .body("body")
                .messageId(messageId)
                .receiptHandle(receiptHandle)
                .attributesWithStrings(attributes)
                .build();
        SQSMessage sqsMessage = new SQSTextMessage(acknowledger, queueUrl, message);
        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        lenient().when(prefetchManager.getMessageConsumer()).thenReturn(consumer);
        return new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, sqsMessage);
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

        return new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, sqsMessage);
    }
}
