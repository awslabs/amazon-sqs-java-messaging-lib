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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.MessageListener;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * Used internally to prefetch messages to internal buffer on a background
 * thread for better <code>receive</code> turn around times.
 * <P>
 * Each message consumer creates one prefetch thread.
 * <P>
 * This runs until the message consumer is closed and in-progress SQS
 * <code>receiveMessage</code> call returns.
 * <P>
 * Uses SQS <code>receiveMessage</code> with long-poll wait time of 20 seconds.
 * <P>
 * Add re-tries on top of <code>AmazonSQSClient</code> re-tries on SQS calls.
 */
public class SQSMessageConsumerPrefetch implements Runnable, PrefetchManager {

    private static final Log LOG = LogFactory.getLog(SQSMessageConsumerPrefetch.class);

    protected static final int WAIT_TIME_SECONDS = 20;

    protected static final String ALL = "All";

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;

    private final String queueUrl;

    /**
     * Maximum number messages, which MessageConsumer can prefetch
     */
    private final int numberOfMessagesToPrefetch;

    private final SQSQueueDestination sqsDestination;

    /**
     * Internal buffer of Messages. The size of queue is MIN_BATCH by default
     * and it can be changed by user.
     */
    protected final ArrayDeque<MessageManager> messageQueue;

    private final Acknowledger acknowledger;

    private final NegativeAcknowledger negativeAcknowledger;

    private volatile MessageListener messageListener;

    private SQSMessageConsumer messageConsumer;

    private final SQSSessionCallbackScheduler sqsSessionRunnable;

    /**
     * Counter on how many messages are prefetched into internal messageQueue.
     */
    protected int messagesPrefetched = 0;

    /**
     * States of the prefetch thread
     */
    protected volatile boolean closed = false;

    protected volatile boolean running = false;

    /** Controls the number of retry attempts to the SQS */
    protected int retriesAttempted = 0;

    private final Object stateLock = new Object();

    /**
     * AWS SQS SDK with default backup strategy already re-tries 3 times
     * exponentially. This backoff is on top of that to let the prefetch thread
     * backoff after SDK completes re-tries with a max delay of 2 seconds and
     * 25ms delayInterval.
     */
    protected ExponentialBackoffStrategy backoffStrategy = new ExponentialBackoffStrategy(25,25,2000);
    
    SQSMessageConsumerPrefetch(SQSSessionCallbackScheduler sqsSessionRunnable, Acknowledger acknowledger,
                               NegativeAcknowledger negativeAcknowledger, SQSQueueDestination sqsDestination,
                               AmazonSQSMessagingClientWrapper amazonSQSClient, int numberOfMessagesToPrefetch) {
        this.amazonSQSClient = amazonSQSClient;
        this.numberOfMessagesToPrefetch = numberOfMessagesToPrefetch;

        this.acknowledger = acknowledger;
        this.negativeAcknowledger = negativeAcknowledger;
        queueUrl = sqsDestination.getQueueUrl();
        this.sqsDestination = sqsDestination;
        this.sqsSessionRunnable = sqsSessionRunnable;
        messageQueue = new ArrayDeque<MessageManager>(numberOfMessagesToPrefetch);
    }

    MessageListener getMessageListener() {
        return messageListener;
    }
    
    void setMessageConsumer(SQSMessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }
    
    @Override
    public SQSMessageConsumer getMessageConsumer() {
        return messageConsumer;
    }
     
    /**
     * Sets the message listener.
     * <P>
     * If message listener is set, the existing messages on the internal buffer
     * will be pushed to session callback scheduler.
     * <P>
     * If message lister is set to null, then the messages on the internal
     * buffer of session callback scheduler will be negative acknowledged, which
     * will be handled by the session callback scheduler thread itself.
     */
    protected void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
        if (messageListener == null || isClosed()) {
            return;
        }
        synchronized (stateLock) {
            if (!running || isClosed()) {
                return;
            }
            while (!messageQueue.isEmpty()) {
                sqsSessionRunnable.scheduleCallBack(messageListener, messageQueue.pollFirst());
            }
        }
    }
    
    /**
     * Runs until the message consumer is closed and in-progress SQS
     * <code>receiveMessage</code> call returns.
     * <P>
     * This blocks if configured number of prefetched messages are already
     * received or connection has not started yet.
     * <P>
     * After consumer is closed, all the messages inside internal buffer will
     * be negatively acknowledged.
     */
    @Override
    public void run() {
        while (true) {
            int prefetchBatchSize;
            boolean nackQueueMessages = false;
            List<Message> messages = null;
            try {
                if (isClosed()) {
                    break;
                }
                
                synchronized (stateLock) {
                    waitForStart();
                    waitForPrefetch();
                    prefetchBatchSize = Math.min(
                            (numberOfMessagesToPrefetch - messagesPrefetched), SQSMessagingClientConstants.MAX_BATCH);
                }

                if (!isClosed()) {
                    messages = getMessages(prefetchBatchSize);
                }

                if (messages != null && !messages.isEmpty()) {
                    processReceivedMessages(messages);
                }
            } catch (InterruptedException e) {
                nackQueueMessages = true;
                break;
            } catch (Throwable e) {
                LOG.error("Unexpected exception when prefetch messages:", e);
                nackQueueMessages = true;
                throw e;
            } finally {
                if (isClosed() || nackQueueMessages) {
                    nackQueueMessages();
                }
            }
        }
    }
    
    /**
     * Call <code>receiveMessage</code> with long-poll wait time of 20 seconds
     * with available prefetch batch size and potential re-tries.
     */
    protected List<Message> getMessages(int prefetchBatchSize) throws InterruptedException {

        assert prefetchBatchSize > 0;

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
                                                              .withMaxNumberOfMessages(prefetchBatchSize)
                                                              .withAttributeNames(ALL)
                                                              .withMessageAttributeNames(ALL)
                                                              .withWaitTimeSeconds(WAIT_TIME_SECONDS);
        List<Message> messages = null;
        try {
            ReceiveMessageResult receivedMessageResult = amazonSQSClient.receiveMessage(receiveMessageRequest);
            messages = receivedMessageResult.getMessages();
            retriesAttempted = 0;
        } catch (JMSException e) {
            LOG.warn("Encountered exception during receive in ConsumerPrefetch thread", e);
            try {
                sleep(backoffStrategy.delayBeforeNextRetry(retriesAttempted++));
            } catch (InterruptedException ex) {
                LOG.warn("Interrupted while retrying on receive", ex);
                throw ex;
            }
        }
        return messages;
    }
    
    /**
     * Converts the received message to JMS message, and pushes to messages to
     * either callback scheduler for asynchronous message delivery or to
     * internal buffers for synchronous message delivery. Messages that was not
     * converted to JMS message will be immediately negative acknowledged.
     */
    protected void processReceivedMessages(List<Message> messages) {
        List<String> nackMessages = new ArrayList<String>();
        for (Message message : messages) {
            try {
                javax.jms.Message jmsMessage = convertToJMSMessage(message);

                if (messageListener != null) {
                    sqsSessionRunnable.scheduleCallBack(messageListener, new MessageManager(this, jmsMessage));
                    synchronized (stateLock) {
                        messagesPrefetched++;                        
                        notifyStateChange();
                    }
                } else {
                    synchronized (stateLock) {
                        messageQueue.addLast(new MessageManager(this, jmsMessage));
                        messagesPrefetched++;
                        notifyStateChange();
                    }
                }
            } catch (JMSException e) {
                nackMessages.add(message.getReceiptHandle());
            }
        }

        // Nack any messages that cannot be serialized to JMSMessage.
        try {
            negativeAcknowledger.action(queueUrl, nackMessages);
        } catch (JMSException e) {
            LOG.warn("Caught exception while nacking received messages", e);
        }
    }

    protected void waitForPrefetch() throws InterruptedException {
        synchronized (stateLock) {
            while (messagesPrefetched >= numberOfMessagesToPrefetch && !isClosed()) {
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting on prefetch", e);
                    /** For interruption, we do not nack the messages */
                    throw e;
                }
            }
        }
    }

    /**
     * Convert the return SQS message into JMS message
     * @param message SQS message to convert
     * @return Converted JMS message
     * @throws JMSException
     */
    protected javax.jms.Message convertToJMSMessage(Message message) throws JMSException {
        MessageAttributeValue messageTypeAttribute = message.getMessageAttributes().get(
                SQSMessage.JMS_SQS_MESSAGE_TYPE);
        javax.jms.Message jmsMessage = null;
        if (messageTypeAttribute == null) {
            jmsMessage = new SQSTextMessage(acknowledger, queueUrl, message);
        } else {
            String messageType = messageTypeAttribute.getStringValue();
            if (SQSMessage.BYTE_MESSAGE_TYPE.equals(messageType)) {
                try {
                    jmsMessage = new SQSBytesMessage(acknowledger, queueUrl, message);
                } catch (JMSException e) {
                    LOG.warn("MessageReceiptHandle - " + message.getReceiptHandle() +
                             "cannot be serialized to BytesMessage", e);
                    throw e;
                }
            } else if (SQSMessage.OBJECT_MESSAGE_TYPE.equals(messageType)) {
                jmsMessage = new SQSObjectMessage(acknowledger, queueUrl, message);
            } else if (SQSMessage.TEXT_MESSAGE_TYPE.equals(messageType)) {
                jmsMessage = new SQSTextMessage(acknowledger, queueUrl, message);
            } else {
                throw new JMSException("Not a supported JMS message type");
            }
        }
        jmsMessage.setJMSDestination(sqsDestination);
        return jmsMessage;
    }

    protected void nackQueueMessages() {
        // Also nack messages already in the messageQueue
        synchronized (stateLock) {
            try {
                negativeAcknowledger.bulkAction(messageQueue, queueUrl);
            } catch (JMSException e) {
                LOG.warn("Caught exception while nacking queued messages", e);
            } finally {
                notifyStateChange();
            }
        }
    }

    protected void waitForStart() throws InterruptedException {
        synchronized (stateLock) {
            while (!running && !isClosed()) {
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting on consumer start", e);
                    throw e;
                }
            }
        }
    }
        
    @Override
    public void messageDispatched() {
        synchronized (stateLock) {
            messagesPrefetched--;
            if (messagesPrefetched < numberOfMessagesToPrefetch) {
                notifyStateChange();
            }
        }
    }

    public static class MessageManager {

        private final PrefetchManager prefetchManager;

        private final javax.jms.Message message;

        public MessageManager(PrefetchManager prefetchManager, javax.jms.Message message) {
            this.prefetchManager = prefetchManager;
            this.message = message;
        }

        public PrefetchManager getPrefetchManager() {
            return prefetchManager;
        }

        public javax.jms.Message getMessage() {
            return message;
        }
    }

    javax.jms.Message receive() throws JMSException {
        return receive(0);
    }
    
    javax.jms.Message receive(long timeout) throws JMSException {
        if (cannotDeliver()) {
            return null;
        }

        if (timeout < 0) {
            timeout = 0;
        }
        
        MessageManager messageManager;
        synchronized (stateLock) {
            // If message exists in queue poll.
            if (!messageQueue.isEmpty()) {
                messageManager = messageQueue.pollFirst();
            } else {
                long startTime = System.currentTimeMillis();

                long waitTime = 0;
                while (messageQueue.isEmpty() && !isClosed() &&
                        (timeout == 0 || (waitTime = getWaitTime(timeout, startTime)) > 0)) {
                    try {
                        stateLock.wait(waitTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
                if (messageQueue.isEmpty() || isClosed()) {
                    return null;
                }
                messageManager = messageQueue.pollFirst();
            }
        }
        return messageHandler(messageManager);
    }

    private long getWaitTime(long timeout, long startTime) {
        return timeout - (System.currentTimeMillis() - startTime);
    }

    protected void notifyStateChange() {
        synchronized (stateLock) {
            stateLock.notifyAll();
        }
    }

    javax.jms.Message receiveNoWait() throws JMSException {
        if (cannotDeliver()) {
            return null;
        }
        
        MessageManager messageManager;
        synchronized (stateLock) {
            messageManager = messageQueue.pollFirst();
        }
        return messageHandler(messageManager);
    }

    void start() {
        if (isClosed() || running) {
            return;
        }
        synchronized (stateLock) {
            running = true;
            notifyStateChange();
        }
    }

    void stop() {
        if (isClosed() || !running) {
            return;
        }
        synchronized (stateLock) {
            running = false;
            notifyStateChange();
        }
    }

    void close() {
        if (isClosed()) {
            return;
        }
        synchronized (stateLock) {
            closed = true;
            notifyStateChange();
            messageListener = null;
        }
    }
    
    /**
     * Helper that notifies PrefetchThread that message is dispatched and AutoAcknowledge
     */
    private javax.jms.Message messageHandler(MessageManager messageManager) throws JMSException {
        if (messageManager == null) {
            return null;
        }        
        javax.jms.Message message = messageManager.getMessage();
        
        // Notify PrefetchThread that message is dispatched
        this.messageDispatched();
        acknowledger.notifyMessageReceived((SQSMessage) message);
        return message;
    }
    
    private boolean cannotDeliver() throws JMSException {
        if (isClosed() || !running) {
            return true;
        }
        if (messageListener != null) {
            throw new JMSException("Cannot receive messages synchronously after a message listener is set");
        }
        return false;
    }
    
    /**
     * Sleeps for the configured time.
     */
    protected void sleep(long sleepTimeMillis) throws InterruptedException {
        try {
            Thread.sleep(sleepTimeMillis);
        } catch (InterruptedException e) {
            throw e;
        }
    }
    
    protected boolean isClosed() {
        return closed;
    }
}
