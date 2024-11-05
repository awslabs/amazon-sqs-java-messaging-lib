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

import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch.MessageManager;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;
import jakarta.jms.JMSException;
import jakarta.jms.MessageListener;
import jakarta.jms.ObjectMessage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.utils.BinaryUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
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
public class SQSMessageConsumerPrefetchTest {

    private static final String NAMESPACE = "123456789012";
    private static final String QUEUE_NAME = "QueueName";
    private static final String QUEUE_URL = NAMESPACE + "/" + QUEUE_NAME;

    private Acknowledger acknowledger;
    private NegativeAcknowledger negativeAcknowledger;
    private SQSSessionCallbackScheduler sqsSessionRunnable;
    private SQSMessageConsumerPrefetch consumerPrefetch;
    private ExponentialBackoffStrategy backoffStrategy;

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private AmazonSQSMessagingClient amazonSQSClient;

    /**
     * Test one full prefetch operation works as expected
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testEndToEnd(int numberOfMessagesToPrefetch) throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */

        // First start the consumer prefetch
        consumerPrefetch.start();

        // Create messages return from SQS
        final int numMessages = numberOfMessagesToPrefetch > 0 ? numberOfMessagesToPrefetch : 1;
        final List<String> receipt = createReceiptHandlersList(numMessages);
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
                .thenAnswer((Answer<Boolean>) invocation -> {
                    // Ensure message queue was filled with expected messages
                    //after we return 'isClosed() == true' we will empty the prefetch queue while nacking messages
                    assertEquals(numMessages, consumerPrefetch.messageQueue.size());
                    for (MessageManager messageManager : consumerPrefetch.messageQueue) {
                        SQSMessage sqsMessage = (SQSMessage) messageManager.message();
                        assertTrue(receipt.contains(sqsMessage.getReceiptHandle()));
                    }

                    return true;
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
        verify(negativeAcknowledger).action(QUEUE_URL, new ArrayList<>());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);

        // Ensure message queue was filled with expected messages
        assertEquals(numMessages, consumerPrefetch.messageQueue.size());
        for (SQSMessageConsumerPrefetch.MessageManager messageManager : consumerPrefetch.messageQueue) {
            SQSMessage sqsMessage = (SQSMessage) messageManager.message();
            assertTrue(receipt.contains(sqsMessage.getReceiptHandle()));
        }
    }

    /**
     * Test that a get message is not called when consumer is closed while waiting for prefetch
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopWhenConsumerClosedDuringWaitForPrefetch(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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
        verify(consumerPrefetch, never()).processReceivedMessages(anyList());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Interrupt in waitForStart
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAfterInterruptWaitForStart(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doThrow(new InterruptedException("Interrupt")).when(consumerPrefetch).waitForStart();

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
        verify(consumerPrefetch, never()).processReceivedMessages(anyList());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Error in waitForStart
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAfterErrorWaitForStart(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.start();

        doThrow(new Error("error")).when(consumerPrefetch).waitForStart();

        /*
         * Run the prefetch
         */
        assertThatThrownBy(() -> consumerPrefetch.run()).isInstanceOf(RuntimeException.class);

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).waitForPrefetch();
        verify(consumerPrefetch, never()).getMessages(anyInt(), anyInt());
        verify(consumerPrefetch, never()).processReceivedMessages(anyList());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Interrupt in waitForPrefetch
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAfterInterruptWaitForPrefetch(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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
        verify(consumerPrefetch, never()).processReceivedMessages(anyList());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Error in waitForPrefetch
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAfterErrorWaitForPrefetch(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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
        assertThatThrownBy(() -> consumerPrefetch.run()).isInstanceOf(RuntimeException.class);

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();

        verify(consumerPrefetch, never()).getMessages(anyInt(), anyInt());
        verify(consumerPrefetch, never()).processReceivedMessages(anyList());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Interrupt in getMessages
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAfterInterruptGetMessages(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
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

        verify(consumerPrefetch, never()).processReceivedMessages(anyList());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test prefetch is stopped after Error in waitForPrefetch
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAfterErrorGetMessages(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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
        assertThatThrownBy(() -> consumerPrefetch.run()).isInstanceOf(RuntimeException.class);

        /*
         * Verify the results
         */

        verify(consumerPrefetch).waitForStart();
        verify(consumerPrefetch).waitForPrefetch();
        verify(consumerPrefetch).nackQueueMessages();
        verify(consumerPrefetch).getMessages(anyInt(), anyInt());

        verify(consumerPrefetch, never()).processReceivedMessages(anyList());

        // Ensure retries attempt was not increased
        assertEquals(0, consumerPrefetch.retriesAttempted);
    }

    /**
     * Test Run when consumer is closed
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testRunExitOnClose(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testSetNullMessageListener(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
        consumerPrefetch.setMessageListener(null);
        verifyNoMoreInteractions(sqsSessionRunnable);
    }

    /**
     * Test SetMessageListener when message were prefetched
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testSetMessageListener(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
        SQSMessageConsumerPrefetch.MessageManager msgManager1 = new SQSMessageConsumerPrefetch.MessageManager(null, mock(jakarta.jms.Message.class));
        SQSMessageConsumerPrefetch.MessageManager msgManager2 = new SQSMessageConsumerPrefetch.MessageManager(null, mock(jakarta.jms.Message.class));

        consumerPrefetch.messageQueue.add(msgManager1);
        consumerPrefetch.messageQueue.add(msgManager2);

        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.running = true;
        consumerPrefetch.setMessageListener(msgListener);

        assertTrue(consumerPrefetch.messageQueue.isEmpty());

        List<MessageManager> expectedList = List.of(msgManager1, msgManager2);
        verify(sqsSessionRunnable).scheduleCallBacks(msgListener, expectedList);

        verifyNoMoreInteractions(sqsSessionRunnable);
    }

    /**
     * Test getting message listener
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testGetMessageListener(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);

        assertEquals(msgListener, consumerPrefetch.getMessageListener());
    }

    /**
     * Test WaitForStart when prefetch already started
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testWaitForStartCurrentStateStart(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.running = true;
        final CountDownLatch passedWaitForStartCall = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        executorService.execute(() -> {
            try {
                consumerPrefetch.waitForStart();

                // Indicate that we no longer waiting
                passedWaitForStartCall.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        /*
         * verify result
         */
        assertTrue(passedWaitForStartCall.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForStart when preftech already closed
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testWaitForStartCurrentStateClose(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.closed = true;
        final CountDownLatch passedWaitForStartCall = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        executorService.execute(() -> {
            try {
                consumerPrefetch.waitForStart();

                // Indicate that we no longer waiting
                passedWaitForStartCall.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        /*
         * verify result
         */
        assertTrue(passedWaitForStartCall.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForStart when prefetch state is updated to started while another thread is waiting
     * for the prefetch to start
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testWaitForStartUpdateStateToStart(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        final CountDownLatch beforeWaitForStartCall = new CountDownLatch(1);
        final CountDownLatch passedWaitForStart = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        executorService.execute(() -> {
            try {
                beforeWaitForStartCall.countDown();
                consumerPrefetch.waitForStart();
                passedWaitForStart.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // Yield execution to allow the consumer to wait
        assertTrue(beforeWaitForStartCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Update the state and notify
        consumerPrefetch.start();

        /*
         * verify result
         */

        // Ensure consumer is not waiting to move to start state
        assertFalse(passedWaitForStart.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForStart when waiting thread is interrupted
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testWaitForStartInterrupted(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        final CountDownLatch beforeWaitForStartCall = new CountDownLatch(1);
        final CountDownLatch recvInterruptedExceptionLatch = new CountDownLatch(1);

        /*
         * call waitForStart in different thread
         */
        Thread t = new Thread(() -> {
            try {
                beforeWaitForStartCall.countDown();
                consumerPrefetch.waitForStart();
            } catch (InterruptedException e) {
                recvInterruptedExceptionLatch.countDown();
                e.printStackTrace();
            }
        });

        t.start();

        // Yield execution to allow the consumer to wait
        assertTrue(beforeWaitForStartCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Interrupt waiting thread
        t.interrupt();

        /*
         * verify result
         */
        assertTrue(recvInterruptedExceptionLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForPrefetch and ensure that message are not prefetch when limit has already reached
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testWaitForPrefetchLimitReached(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch + 5;
        final CountDownLatch beforeWaitForPrefetchCall = new CountDownLatch(1);
        final CountDownLatch passedWaitForPrefetch = new CountDownLatch(1);

        /*
         * call waitForPrefetch in different thread
         */
        executorService.execute(() -> {
            try {
                beforeWaitForPrefetchCall.countDown();
                consumerPrefetch.waitForPrefetch();
                passedWaitForPrefetch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // Yield execution to allow the consumer to wait
        assertTrue(beforeWaitForPrefetchCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the local and ensure that we are still waiting since the prefetch message still equal to the limit
        consumerPrefetch.notifyStateChange();
        assertFalse(passedWaitForPrefetch.await(3, TimeUnit.SECONDS));

        // Simulate messages were processes
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch - 1;

        // Release the local and ensure that we no longer waiting since the prefetch message is below the limit
        consumerPrefetch.notifyStateChange();
        assertTrue(passedWaitForPrefetch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForPrefetch when prefetch consumer is closed
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testWaitForPrefetchIsClosed(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch + 5;
        consumerPrefetch.close();

        final CountDownLatch beforeWaitForPrefetchCall = new CountDownLatch(1);
        final CountDownLatch passedWaitForPrefetch = new CountDownLatch(1);

        executorService.execute(() -> {
            try {
                beforeWaitForPrefetchCall.countDown();
                consumerPrefetch.waitForPrefetch();
                passedWaitForPrefetch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // Yield execution to allow the consumer to wait
        assertTrue(beforeWaitForPrefetchCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Validate we do not wait when the consumer is closed
        assertTrue(passedWaitForPrefetch.await(3, TimeUnit.SECONDS));
    }

    /**
     * Test WaitForPrefetch when waiting thread is interrupted
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testWaitForPrefetchInterrupted(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.messagesPrefetched = numberOfMessagesToPrefetch + 5;
        final CountDownLatch beforeWaitForPrefetchCall = new CountDownLatch(1);
        final CountDownLatch recvInterruptedExceptionLatch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                beforeWaitForPrefetchCall.countDown();
                consumerPrefetch.waitForPrefetch();
            } catch (InterruptedException e) {
                recvInterruptedExceptionLatch.countDown();
                e.printStackTrace();
            }
        });

        t.start();

        assertTrue(beforeWaitForPrefetchCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        t.interrupt();

        // Validate that we no longer waiting due to the interrupt
        assertTrue(recvInterruptedExceptionLatch.await(10, TimeUnit.SECONDS));
    }

    /**
     * Test ConvertToJMSMessage when message type is not set in the message attribute
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageNoTypeAttribute(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        Map<MessageSystemAttributeName, String> mapAttributes = Map.of(
                MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        Message message = Message.builder()
                .messageAttributes(new HashMap<>())
                .attributes(mapAttributes)
                .body("MessageBody")
                .build();

        /*
         * Convert the SQS message to JMS Message
         */
        jakarta.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSTextMessage);
        assertEquals(((SQSTextMessage) jsmMessage).getText(), "MessageBody");
    }

    /**
     * Test ConvertToJMSMessage with byte message type
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageByteTypeAttribute(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */

        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<>();
        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                .stringValue(SQSMessage.BYTE_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING)
                .build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = Map.of(
                MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        byte[] byteArray = new byte[]{1, 0, 'a', 65};

        Message message = Message.builder()
                .messageAttributes(mapMessageAttributes)
                .attributes(mapAttributes)
                .body(BinaryUtils.toBase64(byteArray))
                .build();

        /*
         * Convert the SQS message to JMS Message
         */
        jakarta.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSBytesMessage);
        for (byte b : byteArray) {
            assertEquals(b, ((SQSBytesMessage) jsmMessage).readByte());
        }
    }

    /**
     * Test ConvertToJMSMessage with byte message that contains illegal sqs message body
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageByteTypeIllegalBody(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */

        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<>();

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                .stringValue(SQSMessage.BYTE_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING)
                .build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = Map.of(
                MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

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
        assertThatThrownBy(() -> consumerPrefetch.convertToJMSMessage(message))
                .isInstanceOf(JMSException.class);
    }

    /**
     * Test ConvertToJMSMessage with an object message
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageObjectTypeAttribute(int numberOfMessagesToPrefetch)
            throws JMSException, IOException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */

        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<>();

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                .stringValue(SQSMessage.OBJECT_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING)
                .build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = Map.of(
                MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

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
        jakarta.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSObjectMessage);
        assertEquals(integer, ((SQSObjectMessage) jsmMessage).getObject());
    }

    /**
     * Test ConvertToJMSMessage with an object message that contains illegal sqs message body
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageObjectIllegalBody(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */

        Map<String, MessageAttributeValue> mapMessageAttributes = new HashMap<>();

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                .stringValue(SQSMessage.OBJECT_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING)
                .build();
        mapMessageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);

        Map<MessageSystemAttributeName, String> mapAttributes = Map.of(
                MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

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
        assertThatThrownBy(jsmMessage::getObject).isInstanceOf(JMSException.class);
    }

    /**
     * Test ConvertToJMSMessage with text message with text type attribute
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testConvertToJMSMessageTextTypeAttribute(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */

        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                .stringValue(SQSMessage.TEXT_MESSAGE_TYPE)
                .dataType(SQSMessagingClientConstants.STRING)
                .build();
        Map<String, MessageAttributeValue> mapMessageAttributes = Map.of(
                SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
        long now = System.currentTimeMillis();
        Map<String, String> mapAttributes = Map.of(
                SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1",
                SQSMessagingClientConstants.SENT_TIMESTAMP, Long.toString(now));

        // Return message attributes with message type 'TEXT'
        Message message = Message.builder()
                .messageAttributes(mapMessageAttributes)
                .attributesWithStrings(mapAttributes)
                .body("MessageBody")
                .build();

        /*
         * Convert the SQS message to JMS Message
         */
        jakarta.jms.Message jsmMessage = consumerPrefetch.convertToJMSMessage(message);

        /*
         * Verify results
         */
        assertTrue(jsmMessage instanceof SQSTextMessage);
        assertEquals(message.body(), "MessageBody");
        assertEquals(jsmMessage.getJMSTimestamp(), now);
    }

    /**
     * Test received messages when consumer prefetch has not started
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveWhenNotStarted(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveWhenClosed(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveMessagePrefetch(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.running = true;

        List<String> receiptHandlers = createReceiptHandlersList(20);

        addMessagesToQueue(receiptHandlers);

        /*
         * Call receive messages
         */
        SQSMessage msg = (SQSMessage) consumerPrefetch.receive();

        /*
         * Verify results
         */
        assertTrue(receiptHandlers.contains(msg.getReceiptHandle()));
        verify(acknowledger).notifyMessageReceived(msg);
        verify(consumerPrefetch, times(2)).notifyStateChange();

        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveNoWaitPrefetch(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and mocks
         */
        consumerPrefetch.running = true;

        List<String> receiptHandlers = createReceiptHandlersList(20);

        addMessagesToQueue(receiptHandlers);

        /*
         * Call receive messages
         */
        SQSMessage msg = (SQSMessage) consumerPrefetch.receiveNoWait();

        /*
         * Verify results
         */
        assertTrue(receiptHandlers.contains(msg.getReceiptHandle()));
        verify(acknowledger).notifyMessageReceived(msg);
        verify(consumerPrefetch, times(2)).notifyStateChange();

        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages call wait for messages and exists when consumer prefterch is closed
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveMessageEmptyThenClosed(int numberOfMessagesToPrefetch) throws InterruptedException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up consumer prefetch and latches
         */
        consumerPrefetch.running = true;
        final CountDownLatch beforeReceiveCall = new CountDownLatch(1);
        final CountDownLatch passedReceiveCall = new CountDownLatch(1);
        final AtomicBoolean noMessageReturned = new AtomicBoolean(false);

        /*
         * Call receive messages
         */
        executorService.execute(() -> {
            try {
                beforeReceiveCall.countDown();
                jakarta.jms.Message msg = consumerPrefetch.receive(0);
                if (msg == null) {
                    noMessageReturned.set(true);
                }
                passedReceiveCall.countDown();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        });

        assertTrue(beforeReceiveCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Update the state and notify
        consumerPrefetch.close();

        // Wait till receive execution finishes
        assertTrue(passedReceiveCall.await(10, TimeUnit.SECONDS));

        // Validate that after session is closed receive returns null
        assertTrue(noMessageReturned.get());

        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages wait when no message are prefetch and return newly added message
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveMessageEmptyThenAddMessage(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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
        executorService.execute(() -> {
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
        });

        assertTrue(beforeReceiveCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Add message to the internal queue
        List<String> receiptHandlers = new ArrayList<>();
        receiptHandlers.add(receiptHandle);
        addMessagesToQueue(receiptHandlers);
        consumerPrefetch.notifyStateChange();

        // Wait till receive execution finishes
        assertTrue(passedReceiveCall.await(10, TimeUnit.SECONDS));

        // Validate that after adding a single message it was received correctly
        assertTrue(messageReceived.get());

        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages with timeout
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveMessageTimeout(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
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
        assertTrue(waitTime <= measuredTime, String.format(
                "Expected wait time = %1$s ms and has to be less than or equal to measured time = %2$s ms",
                waitTime, measuredTime));

        // Ensure the messagesRequested counter is reset correctly
        assertEquals(0, consumerPrefetch.messagesRequested);
    }

    /**
     * Test received messages with timeout
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testReceiveNoWaitEmpty(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testProcessReceivedMessagesEmptyInput(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
        consumerPrefetch.processReceivedMessages(List.of());
        verifyNoMoreInteractions(sqsSessionRunnable);
    }

    /**
     * Test process received messages
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testProcessReceivedMessages(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
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
            SQSMessage msg = (SQSMessage) msgManager.message();
            assertTrue(receiptHandlers.contains(msg.getReceiptHandle()));
        }

        verify(negativeAcknowledger).action(QUEUE_URL, List.of());
    }


    /**
     * Test process messages when message listener is set
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testProcessReceivedMessagesWithMessageListener(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up mocks and consumer
         */
        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);

        // Create messages
        List<String> receiptHandlers = createReceiptHandlersList(3);

        List<Message> messages = createSQSServiceMessages(receiptHandlers);

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */
        ArgumentCaptor<List<MessageManager>> captor = ArgumentCaptor.forClass(List.class);
        verify(sqsSessionRunnable, times(1)).scheduleCallBacks(eq(msgListener), captor.capture());
        assertEquals(3, captor.getValue().size());

        // Ensure no messages were added to the queue
        assertEquals(0, consumerPrefetch.messageQueue.size());
        assertEquals(3, consumerPrefetch.messagesPrefetched);

        verify(negativeAcknowledger).action(QUEUE_URL, List.of());
    }

    /**
     * Test process messages when message listener is set
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testProcessReceivedMessagesThrowsException(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up mocks and consumer
         */
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

        verify(negativeAcknowledger).action(QUEUE_URL, List.of("r1", "r2"));
    }

    /**
     * Test process messages when message listener is set
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testProcessReceivedMessagesNegativeAcknowledgerThrowJMSException(int numberOfMessagesToPrefetch)
            throws JMSException {
        init(numberOfMessagesToPrefetch);
        /*
         * Set up mocks and consumer
         */
        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);

        doThrow(new JMSException("Exception"))
                .when(negativeAcknowledger).action(eq(QUEUE_URL), anyList());

        // Create messages
        List<String> receiptHandlers = createReceiptHandlersList(3);
        List<Message> messages = createSQSServiceMessages(receiptHandlers);

        /*
         * Process messages
         */
        consumerPrefetch.processReceivedMessages(messages);

        /*
         * Verify results
         */
        ArgumentCaptor<List<MessageManager>> captor = ArgumentCaptor.forClass(List.class);
        verify(sqsSessionRunnable, times(1)).scheduleCallBacks(eq(msgListener), captor.capture());
        assertEquals(3, captor.getValue().size());

        // Ensure no messages were added to the queue
        assertEquals(0, consumerPrefetch.messageQueue.size());
        assertEquals(3, consumerPrefetch.messagesPrefetched);

        verify(negativeAcknowledger).action(QUEUE_URL, List.of());
    }

    /**
     * Test Get Messages
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testGetMessages(int numberOfMessagesToPrefetch) throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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

        List<Message> messages = IntStream.range(1, 6)
                .mapToObj(a -> Message.builder().receiptHandle("r" + a).build())
                .toList();

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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testGetMessagesIllegalPrefetchSize(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
        int negativeSize = -10;
        assertThatThrownBy(() -> consumerPrefetch.getMessages(negativeSize, 0))
                .isInstanceOf(AssertionError.class);

        assertThatThrownBy(() -> consumerPrefetch.getMessages(0, 0))
                .isInstanceOf(AssertionError.class);
    }

    /**
     * Test Get Messages throws JMS exception
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testGetMessagesJMSException(int numberOfMessagesToPrefetch) throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testGetMessagesInterruptDuringBackoff(int numberOfMessagesToPrefetch)
            throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
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
        Thread t = new Thread(() -> {
            try {
                beforeGetMessagesCall.countDown();
                consumerPrefetch.getMessagesWithBackoff(prefetchBatchSize);
            } catch (InterruptedException e) {
                recvInterruptedExceptionLatch.countDown();
                e.printStackTrace();
            }
        });
        t.start();

        assertTrue(beforeGetMessagesCall.await(5, TimeUnit.SECONDS));
        Thread.sleep(10);

        /*
         * Interrupt the getMessage execution
         */
        t.interrupt();

        assertTrue(recvInterruptedExceptionLatch.await(5, TimeUnit.SECONDS));
    }

    /**
     * Test Get Messages throws error
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testGetMessagesError(int numberOfMessagesToPrefetch) throws JMSException {
        init(numberOfMessagesToPrefetch);
        int retriesAttempted = 3;
        int prefetchBatchSize = 5;
        consumerPrefetch.retriesAttempted = retriesAttempted;

        when(amazonSQSClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenThrow(new Error());

        assertThatThrownBy(() -> consumerPrefetch.getMessages(prefetchBatchSize, 0))
                .isInstanceOf(Error.class);
    }

    /**
     * Test start when consumer prefetch is already closed
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStartAlreadyClosed(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStartAlreadyStarted(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStart(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAlreadyClosed(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStopAlreadyStarted(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testStop(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testCloseAlreadyClosed(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testClose(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
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
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testRequestedMessageTracking(int numberOfMessagesToPrefetch) throws InterruptedException, JMSException {
        init(numberOfMessagesToPrefetch);
        int concurrentReceives = 3;
        int receiveBatchSize = Math.min(SQSMessagingClientConstants.MAX_BATCH,
                Math.max(concurrentReceives, numberOfMessagesToPrefetch));

        // Create messages return from SQS
        List<String> receipt = createReceiptHandlersList(receiveBatchSize);
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
        doAnswer((Answer<Object>) invocation -> {
            invocation.callRealMethod();
            allReceivesWaiting.countDown();
            return null;
        }).when(consumerPrefetch).requestMessage();

        // Close the prefetcher immediately after completing one loop
        final List<Future<jakarta.jms.Message>> receivedMessageFutures = new ArrayList<>();
        doAnswer((Answer<Object>) invocation -> {
            invocation.callRealMethod();
            for (Future<jakarta.jms.Message> messageFuture : receivedMessageFutures) {
                assertNotNull(messageFuture.get());
            }
            consumerPrefetch.close();
            return null;
        }).when(consumerPrefetch).processReceivedMessages(anyList());

        // Set running to true first so that the receive calls don't terminate early
        consumerPrefetch.running = true;

        ExecutorService receiveExecutor = Executors.newFixedThreadPool(concurrentReceives);
        for (int i = 0; i < concurrentReceives; i++) {
            receivedMessageFutures.add(receiveExecutor.submit(() -> consumerPrefetch.receive()));
        }

        // Wait to make sure the received calls have gotten far enough to
        // wait on the message queue
        allReceivesWaiting.await();

        assertEquals(concurrentReceives, consumerPrefetch.messagesRequested);

        consumerPrefetch.run();
    }

    /**
     * Test SetMessageListener before starting prefetch
     * Setting MessageListener before starting prefetch would not mark
     * the message listener as ready. Therefore start() method should
     * do this work in order to get pre-fetch going even when
     * number of messages to pre-fetch is set to 0.
     */
    @ParameterizedTest
    @MethodSource("prefetchParameters")
    public void testSetMessageListenerBeforeStart(int numberOfMessagesToPrefetch) {
        init(numberOfMessagesToPrefetch);
        MessageListener msgListener = mock(MessageListener.class);
        consumerPrefetch.setMessageListener(msgListener);
        consumerPrefetch.start();
        assertEquals(1, consumerPrefetch.messagesRequested);
    }

    /*
     * Utility functions
     */

    private void addMessagesToQueue(List<String> receiptHandlers) throws JMSException {
        Map<MessageSystemAttributeName, String> mapAttributes = Map.of(
                MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        for (String receiptHandler : receiptHandlers) {
            Message message = Message.builder()
                    .receiptHandle(receiptHandler)
                    .attributes(mapAttributes)
                    .build();
            jakarta.jms.Message m1 = consumerPrefetch.convertToJMSMessage(message);
            SQSMessageConsumerPrefetch.MessageManager msgManager = new SQSMessageConsumerPrefetch.MessageManager(null, m1);

            consumerPrefetch.messageQueue.add(msgManager);
        }
    }

    private List<Message> createSQSServiceMessages(List<String> receiptHandlers) {
        Map<MessageSystemAttributeName, String> mapAttributes = Map.of(
                MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

        return receiptHandlers.stream().map(
                receiptHandler -> Message.builder().receiptHandle(receiptHandler).attributes(mapAttributes).build()
        ).toList();
    }

    private ReceiveMessageResponse createReceiveMessageResult(List<String> receiptList) {
        List<Message> messages = createSQSServiceMessages(receiptList);
        return ReceiveMessageResponse.builder().messages(messages).build();
    }

    private List<String> createReceiptHandlersList(int count) {
        return IntStream.range(0, count).mapToObj(a -> "r" + a).toList();
    }

    void init(int numberOfMessagesToPrefetch) {
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

    private static Stream<Arguments> prefetchParameters() {
        return Stream.of(
                Arguments.of(0),
                Arguments.of(1),
                Arguments.of(5),
                Arguments.of(10),
                Arguments.of(15)
        );
    }
}
