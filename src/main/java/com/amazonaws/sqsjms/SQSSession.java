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
package com.amazonaws.sqsjms;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.jms.IllegalStateException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.sqsjms.SQSMessageConsumerPrefetch.MessageManager;
import com.amazonaws.sqsjms.acknowledge.AcknowledgeMode;
import com.amazonaws.sqsjms.acknowledge.Acknowledger;
import com.amazonaws.sqsjms.acknowledge.NegativeAcknowledger;
import com.amazonaws.sqsjms.util.SQSJMSClientThreadFactory;

public class SQSSession implements Session, QueueSession {
    private static final Log LOG = LogFactory.getLog(SQSSession.class);

    static final String SESSION_EXECUTOR_NAME = "SessionCallBackScheduler";
    static final SQSJMSClientThreadFactory SESSION_THREAD_FACTORY = new SQSJMSClientThreadFactory(
            SESSION_EXECUTOR_NAME, false, true);

    static final String CONSUMER_PREFETCH_EXECUTER_NAME = "ConsumerPrefetch";
    static final SQSJMSClientThreadFactory CONSUMER_PREFETCH_THREAD_FACTORY = new SQSJMSClientThreadFactory(
            CONSUMER_PREFETCH_EXECUTER_NAME, true);

    /**
     * Non standard acknowledge mode. This is a variation of CLIENT_ACKNOWLEDGE
     * where Clients need to remember to call acknowledge on message. Difference
     * is that calling acknowledge on a message only acknowledge the message
     * being called.
     */
    public static final int UNORDERED_ACKNOWLEDGE = 100;

    /**
     * True if Session is closed.
     */
    private volatile boolean closed = false;

    /**
     * False if Session is stopped.
     */
    volatile boolean running = false;

    private volatile boolean closing = false;

    private final AmazonSQSClientJMSWrapper amazonSQSClient;
    private final SQSConnection parentSQSConnection;

    /**
     * AcknowledgeMode of this Session.
     */
    private final AcknowledgeMode acknowledgeMode;

    /**
     * Acknowledger of this Session.
     */
    private final Acknowledger acknowledger;

    /**
     * Set of MessageProducer under this Session
     */
    private final Set<SQSMessageProducer> messageProducers;

    /**
     * Set of MessageConsumer under this Session
     */
    private final Set<SQSMessageConsumer> messageConsumers;

    private final SQSSessionCallbackScheduler sqsSessionRunnable;

    /**
     * Executor service for running MessageListener.
     */
    private final ExecutorService executor;

    private final Object stateLock = new Object();
    
    /**
     * Used to determine if the caller thread is the session callback thread.
     * Guarded by stateLock
     */
    private Thread activeCallbackSessionThread;

    /**
     * Used to determine the active consumer, whose is dispatching the message
     * on the callback. Guarded by stateLock
     */
    private SQSMessageConsumer activeConsumerInCallback = null;

    SQSSession(SQSConnection parentSQSConnection, AcknowledgeMode acknowledgeMode) throws JMSException{
        this(parentSQSConnection, acknowledgeMode,
                Collections.newSetFromMap(new ConcurrentHashMap<SQSMessageConsumer, Boolean>()),
                Collections.newSetFromMap(new ConcurrentHashMap<SQSMessageProducer, Boolean>()));
    }

    SQSSession(SQSConnection parentSQSConnection, AcknowledgeMode acknowledgeMode,
               Set<SQSMessageConsumer> messageConsumers,
               Set<SQSMessageProducer> messageProducers) throws JMSException{
        this.parentSQSConnection = parentSQSConnection;
        this.amazonSQSClient = parentSQSConnection.getWrappedAmazonSQSClient();
        this.acknowledgeMode = acknowledgeMode;
        this.acknowledger = this.acknowledgeMode.createAcknowledger(amazonSQSClient, this);
        this.sqsSessionRunnable = new SQSSessionCallbackScheduler(this, acknowledgeMode, acknowledger);
        this.executor = Executors.newSingleThreadExecutor(SESSION_THREAD_FACTORY);
        this.messageConsumers = messageConsumers;
        this.messageProducers = messageProducers;

        executor.execute(sqsSessionRunnable);
    }
    
    SQSConnection getParentConnection() {
        return parentSQSConnection;
    }
    
    /**
     * True if the current thread is the callback thread
     * @return
     */
    boolean isActiveCallbackSessionThread() {
        synchronized (stateLock) {
            return activeCallbackSessionThread == Thread.currentThread();
        }
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return (QueueReceiver) createConsumer(queue);
    }

    /**
     * createReceiver does not support messageSelector.
     * It will drop anything in messageSelector.
     */
    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return createReceiver(queue);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return (QueueSender) createProducer(queue);
    }

    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkClosed();
        return new SQSBytesMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        /**
         * According to JMS specification, a message can be sent with only
         * headers without any payload, SQS does not support messages with empty
         * payload
         */
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkClosed();
        return new SQSObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        checkClosed();
        return new SQSObjectMessage(object);
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkClosed();
        return new SQSTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        checkClosed();
        return new SQSTextMessage(text);
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return acknowledgeMode.getOriginalAcknowledgeMode();
    }

    @Override
    public void close() throws JMSException {

        if (closed) {
            return;
        }
        
        /**
         * A MessageListener must not attempt to close its own Session as
         * this would lead to deadlock
         */
        if (isActiveCallbackSessionThread()) {
            throw new IllegalStateException(
                    "MessageListener must not attempt to close its own Session to prevent potential deadlock issues");
        }
        
        doClose();
    }

    void doClose() throws JMSException {
        boolean shouldClose = false;
        synchronized (stateLock) {
            if (!closing) {
                shouldClose = true;
                closing = true;
            }
        }
        if (closed) {
            return;
        }

        if (shouldClose) {
            try {
                parentSQSConnection.removeSession(this);

                for (MessageProducer messageProducer : messageProducers) {
                    messageProducer.close();
                }
                for (MessageConsumer messageConsumer : messageConsumers) {
                    messageConsumer.close();
                }
                
                try {
                    if (executor != null) {
                        LOG.info("Shutting down " + SESSION_EXECUTOR_NAME + " executor");

                        /** Shut down executor. */
                        executor.shutdown();
                        
                        waitForCallbackComplete();
                        
                        sqsSessionRunnable.close();

                        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {


                            LOG.warn("Can't terminate executor service " + SESSION_EXECUTOR_NAME + " after " +
                                     10 + " seconds, some running threads will be shutdown immediately");
                            executor.shutdownNow();
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while closing the session.", e);
                }
                
                /** Nack the messages that were delivered but not acked */
                recover();
            } finally {
                synchronized (stateLock) {
                    closed = true;
                    stateLock.notifyAll();
                }
            }
        }/** Blocks until closing of the session completes */
        else {
            synchronized (stateLock) {
                while (!closed) {
                    try {
                        stateLock.wait();
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted while waiting the session to close.", e);
                    }
                }
            }
        }
    }

    /*
     * TODO
     * 1. Stop message delivery
     * 2. Mark all messages that might have been delivered but not acknowledged as "redelivered"
     * 3. Restart the delivery sequence including all unacknowledged messages that had been previously delivered.
     * Redelivered messages do not have to be delivered in exactly their original delivery order.
     */
    @Override
    public void recover() throws JMSException {
        checkClosed();
        for (SQSMessageConsumer messageConsumer : messageConsumers) {
            messageConsumer.recover();
        }
    }

    @Override
    public void run() {
    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkClosed();
        if (destination != null && !(destination instanceof SQSDestination)) {
            throw new JMSException("Actual type of Destination/Queue has to be SQSDestination");
        }
        SQSMessageProducer messageProducer;
        synchronized (stateLock) {
            checkClosing();
            messageProducer = new SQSMessageProducer(amazonSQSClient, this, (SQSDestination) destination);
            messageProducers.add(messageProducer);
        }
        return messageProducer;
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        checkClosed();
        if (!(destination instanceof SQSDestination)) {
            throw new JMSException("Actual type of Destination/Queue has to be SQSDestination");
        }
        SQSMessageConsumer messageConsumer;
        synchronized (stateLock) {
            checkClosing();
            messageConsumer = createSQSMessageConsumer((SQSDestination) destination);
            messageConsumers.add(messageConsumer);
            if( running ) {
                messageConsumer.startPrefetch();
            }
        }
        return messageConsumer;
    }

    SQSMessageConsumer createSQSMessageConsumer(SQSDestination destination) {
        return new SQSMessageConsumer(
                parentSQSConnection, this, sqsSessionRunnable, (SQSDestination) destination,
                acknowledger,  new NegativeAcknowledger(amazonSQSClient),
                CONSUMER_PREFETCH_THREAD_FACTORY);
    }

    /**
     * createConsumer does not support messageSelector.
     * It will drop any argument in messageSelector.
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (messageSelector != null) {
            throw new JMSException("SQSSession does not support MessageSelector. This should be null.");
        }
        return createConsumer(destination);
    }

    /**
     * createConsumer does not support messageSelector and NoLocal.
     * It will drop any argument in messageSelector and NoLocal.
     */
    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean NoLocal) throws JMSException {
        if (messageSelector != null) {
            throw new JMSException("SQSSession does not support MessageSelector. This should be null.");
        }
        return createConsumer(destination);
    }

    /**
     * This does not create SQS Queue. This method is only to create JMS Queue Object.
     * Make sure the queue exists corresponding to the queueName.
     */
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkClosed();
        return new SQSDestination(queueName, amazonSQSClient.getQueueUrl(queueName).getQueueUrl());
    }

    /**
     * This is used in MessageConsumer. When MessageConsumer is closed
     * it will remove itself from list of Sessions.
     */
    void removeConsumer(SQSMessageConsumer consumer) {
        messageConsumers.remove(consumer);
    }

    /**
     * This is used in MessageProducer. When MessageProducer is closed
     * it will remove itself from list of Sessions.
     */
    void removeProducer(SQSMessageProducer producer) {
        messageProducers.remove(producer);    
    }
    
    void startingCallback(SQSMessageConsumer consumer) throws InterruptedException, JMSException {
        if (closed) {
            return;
        }
        synchronized (stateLock) {
            if (activeConsumerInCallback != null) {
                throw new IllegalStateException("Callback already in progress");
            }
            assert activeCallbackSessionThread == null;

            while (!running && !closing) {
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while waiting on session start", e);
                    throw e;
                }
            }
            checkClosing();
            activeConsumerInCallback = consumer;
            activeCallbackSessionThread = Thread.currentThread();
        }
    }
    
    void finishedCallback() throws JMSException {
        synchronized (stateLock) {
            if (activeConsumerInCallback == null) {
                throw new IllegalStateException("Callback not in progress");
            }
            activeConsumerInCallback = null;
            activeCallbackSessionThread = null;
            stateLock.notifyAll();
        }
    }
    
    void waitForConsumerCallbackToComplete(SQSMessageConsumer consumer) throws InterruptedException {
        synchronized (stateLock) {
            while (activeConsumerInCallback == consumer) {
                stateLock.wait();
            }
        }
    }
    
    void waitForCallbackComplete() throws InterruptedException {
        synchronized (stateLock) {
            while (activeConsumerInCallback != null) {
                stateLock.wait();
            }
        }
    }

    /** SQS does not support transacted. Transacted will always be false. */
    @Override
    public boolean getTransacted() throws JMSException {
        return false;
    }

    /** This method is not supported. This method is related to transaction which SQS doesn't support */
    @Override
    public void commit() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. This method is related to transaction which SQS doesn't support */
    @Override
    public void rollback() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. This method is related to Topic which SQS doesn't support */
    @Override
    public void unsubscribe(String name) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public MessageListener getMessageListener() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public MapMessage createMapMessage() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    static class CallbackEntry {
        private final MessageListener messageListener;
        private final MessageManager messageManager;
        
        CallbackEntry(MessageListener messageListener, MessageManager messageManager) {
            this.messageListener = messageListener;
            this.messageManager = messageManager;
        }
        
        public MessageListener getMessageListener() {
            return messageListener;
        }
        
        public MessageManager getMessageManager() {
            return messageManager;
        }
    }
    
    /**
     * Check current closed state of Session.
     */
    public void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Session is closed");
        }
    }
    
    /**
     * Check current closed state of Session.
     */
    public void checkClosing() throws IllegalStateException {
        if (closing) {
            throw new IllegalStateException("Session is closed or closing");
        }
    }
        
    void start() throws IllegalStateException {
        checkClosed();
        synchronized (stateLock) {
            checkClosing();
            running = true;            
            for (SQSMessageConsumer messageConsumer : messageConsumers) {
                messageConsumer.startPrefetch();
            }
            stateLock.notifyAll();
        }
    }
    
    void stop() throws IllegalStateException {
        checkClosed();
        synchronized (stateLock) {
            checkClosing();
            running = false;
            for (SQSMessageConsumer messageConsumer : messageConsumers) {
                messageConsumer.stopPrefetch();
            }
            try {
                waitForCallbackComplete();
            } catch (InterruptedException e) {
                LOG.error("Interrupted while waiting on session stop", e);
            }
            stateLock.notifyAll();
        }
    }

    /*
     * Unit Tests Utility Functions
     */

    boolean isCallbackActive() {
        return activeConsumerInCallback != null;
    }
    
    void setActiveConsumerInCallback(SQSMessageConsumer consumer) {
        activeConsumerInCallback = consumer;
    }
    
    Object getStateLock() {
        return stateLock;
    }

    boolean isClosed() {
        return closed;
    }
    
    boolean isClosing() {
        return closing;
    }

    void setClosed(boolean closed) {
        this.closed = closed;
    }

    public void setClosing(boolean closing) {
        this.closing = closing;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

}
