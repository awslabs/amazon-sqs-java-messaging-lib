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

import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch.MessageManager;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.util.SQSMessagingClientThreadFactory;

/**
 * A session serves several purposes:
 * <ul>
 * <li>It is a factory for its message producers and consumers.</li>
 * <li>It provides a way to create Queue objects for those clients that need to
 * dynamically manipulate provider-specific destination names.</li>
 * <li>It retains messages it consumes until they have been acknowledged.</li>
 * <li>It serializes execution of message listeners registered with its message
 * consumers.</li>
 * </ul>
 * <P>
 * Not safe for concurrent use.
 * <P>
 * This session object does not support:
 * <ul>
 * <li>(Temporary)Topic</li>
 * <li>Temporary Queue</li>
 * <li>Browser</li>
 * <li>MapMessage</li>
 * <li>StreamMessage</li>
 * <li>MessageSelector</li>
 * <li>Transactions</li>
 * </ul>
 */
public class SQSSession implements Session, QueueSession {
    private static final Log LOG = LogFactory.getLog(SQSSession.class);
    
    private static final int SESSION_EXECUTOR_GRACEFUL_SHUTDOWN_TIME = 10;
    
    static final String SESSION_EXECUTOR_NAME = "SessionCallBackScheduler";

    /** Used to create session callback scheduler threads */
    static final SQSMessagingClientThreadFactory SESSION_THREAD_FACTORY = new SQSMessagingClientThreadFactory(
            SESSION_EXECUTOR_NAME, false, true);

    static final String CONSUMER_PREFETCH_EXECUTER_NAME = "ConsumerPrefetch";

    /** Used to create consumer prefetcher threads */
    static final SQSMessagingClientThreadFactory CONSUMER_PREFETCH_THREAD_FACTORY = new SQSMessagingClientThreadFactory(
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
    
    /**
     * True if Session is closed or close is in-progress.
     */
    private volatile boolean closing = false;

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;
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
    
    /**
     * Thread that is responsible to guarantee serial execution of message
     * delivery on message listeners
     */
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
     * @return True if the current thread is the callback thread
     */
    boolean isActiveCallbackSessionThread() {
        synchronized (stateLock) {
            return activeCallbackSessionThread == Thread.currentThread();
        }
    }
    
    /**
     * Creates a <code>QueueReceiver</code> for the specified queue.
     * @param queue
     *            a queue receiver
     * @return new message consumer
     * @throws JMSException
     *             If session is closed
     */
    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return (QueueReceiver) createConsumer(queue);
    }

    /**
     * Creates a <code>QueueReceiver</code> for the specified queue. Does not
     * support messageSelector. It will drop anything in messageSelector.
     * 
     * @param queue
     *            a queue destination
     * @param messageSelector           
     * @return new message receiver
     * @throws JMSException
     *             If session is closed
     */
    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return createReceiver(queue);
    }
    
    /**
     * Creates a <code>QueueSender</code> for the specified queue.
     * 
     * @param queue
     *            a queue destination 
     * @return new message sender
     * @throws JMSException
     *             If session is closed
     */
    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return (QueueSender) createProducer(queue);
    }
    
    /**
     * Creates a <code>BytesMessage</code>.
     * 
     * @return new <code>BytesMessage</code>
     * @throws JMSException
     *             If session is closed or internal error
     */
    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        checkClosed();
        return new SQSBytesMessage();
    }
   
    /**
     * According to JMS specification, a message can be sent with only headers
     * without any payload, SQS does not support messages with empty payload. so
     * this method is not supported
     */
    @Override
    public Message createMessage() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }
    
    /**
     * Creates a <code>ObjectMessage</code>.
     * 
     * @return new <code>ObjectMessage</code>
     * @throws JMSException
     *             If session is closed or internal error
     */
    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        checkClosed();
        return new SQSObjectMessage();
    }
    
    /**
     * Creates an initialized <code>ObjectMessage</code>.
     * 
     * @param object
     *            The initialized <code>ObjectMessage</code>
     * @return new <code>ObjectMessage</code>
     * @throws JMSException
     *             If session is closed or internal error
     */
    @Override
    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        checkClosed();
        return new SQSObjectMessage(object);
    }
    
    /**
     * Creates a <code>TextMessage</code>.
     * 
     * @return new <code>TextMessage</code>
     * @throws JMSException
     *             If session is closed or internal error
     */
    @Override
    public TextMessage createTextMessage() throws JMSException {
        checkClosed();
        return new SQSTextMessage();
    }
    
    /**
     * Creates an initialized <code>TextMessage</code>.
     * 
     * @param text
     *            The initialized <code>TextMessage</code>
     * @return new <code>TextMessage</code>
     * @throws JMSException
     *             If session is closed or internal error
     */
    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        checkClosed();
        return new SQSTextMessage(text);
    }

    /**
     * Returns the acknowledge mode of the session. The acknowledge mode is set
     * at the time that the session is created.
     * 
     * @return acknowledge mode
     */
    @Override
    public int getAcknowledgeMode() throws JMSException {
        return acknowledgeMode.getOriginalAcknowledgeMode();
    }
    
    /**
     * Closes the session.
     * <P>
     * This will not return until all the message consumers and producers close
     * internally, which blocks until receives and/or message listeners in
     * progress have completed. A blocked message consumer receive call returns
     * null when this session is closed.
     * <P>
     * Since consumer prefetch threads use SQS long-poll feature with 20 seconds
     * timeout, closing each consumer prefetch thread can take up to 20 seconds,
     * which in-turn will impact the time on session close.
     * <P>
     * This method is safe for concurrent use.
     * <P>
     * A message listener must not attempt to close its own session; otherwise
     * throws a IllegalStateException.
     * <P>
     * Invoking any other session method on a closed session must throw a
     * <code>IllegalStateException</code>.
     * 
     * @throws IllegalStateException
     *             If called by a message listener on its own
     *             <code>Session</code>.
     * @throws JMSException
     *             On internal error.
     */
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
            stateLock.notifyAll();
        }
        
        if (closed) {
            return;
        }

        if (shouldClose) {
            try {
                parentSQSConnection.removeSession(this);

                for (SQSMessageConsumer messageConsumer : messageConsumers) {
                    messageConsumer.close();
                    /** Nack the messages that were delivered but not acked */
                    messageConsumer.recover();
                }
                
                try {
                    if (executor != null) {
                        LOG.info("Shutting down " + SESSION_EXECUTOR_NAME + " executor");

                        /** Shut down executor. */
                        executor.shutdown();
                        
                        waitForCallbackComplete();
                        
                        sqsSessionRunnable.close();
                        
                        for (MessageProducer messageProducer : messageProducers) {
                            messageProducer.close();
                        }

                        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {

                            LOG.warn("Can't terminate executor service " + SESSION_EXECUTOR_NAME + " after " +
                                     SESSION_EXECUTOR_GRACEFUL_SHUTDOWN_TIME +
                                     " seconds, some running threads will be shutdown immediately");
                            executor.shutdownNow();
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.error("Interrupted while closing the session.", e);
                }
               
            } finally {
                synchronized (stateLock) {
                    closed = true;
                    running = false;
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


    /**
     * Negative acknowledges all the messages on the session that is delivered
     * but not acknowledged.
     * 
     * @throws JMSException
     *             If session is closed or on internal error.
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
    
    /**
     * Creates a <code>MessageProducer</code> for the specified destination.
     * Only queue destinations are supported at this time.
     * 
     * @param destination
     *            a queue destination
     * @return new message producer
     * @throws JMSException
     *             If session is closed or queue destination is not used
     */
    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        checkClosed();
        if (destination != null && !(destination instanceof SQSQueueDestination)) {
            throw new JMSException("Actual type of Destination/Queue has to be SQSQueueDestination");
        }
        SQSMessageProducer messageProducer;
        synchronized (stateLock) {
            checkClosing();
            messageProducer = new SQSMessageProducer(amazonSQSClient, this, (SQSQueueDestination) destination);
            messageProducers.add(messageProducer);
        }
        return messageProducer;
    }
    
    /**
     * Creates a <code>MessageConsumer</code> for the specified destination.
     * Only queue destinations are supported at this time.
     * 
     * @param destination
     *            a queue destination
     * @return new message consumer
     * @throws JMSException
     *             If session is closed or queue destination is not used
     */
    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        checkClosed();
        if (!(destination instanceof SQSQueueDestination)) {
            throw new JMSException("Actual type of Destination/Queue has to be SQSQueueDestination");
        }
        SQSMessageConsumer messageConsumer;
        synchronized (stateLock) {
            checkClosing();
            messageConsumer = createSQSMessageConsumer((SQSQueueDestination) destination);
            messageConsumers.add(messageConsumer);
            if( running ) {
                messageConsumer.startPrefetch();
            }
        }
        return messageConsumer;
    }

    SQSMessageConsumer createSQSMessageConsumer(SQSQueueDestination destination) {
        return new SQSMessageConsumer(
                parentSQSConnection, this, sqsSessionRunnable, (SQSQueueDestination) destination,
                acknowledger,  new NegativeAcknowledger(amazonSQSClient),
                CONSUMER_PREFETCH_THREAD_FACTORY);
    }

    /**
     *
     * Creates a <code>MessageConsumer</code> for the specified destination.
     * Only queue destinations are supported at this time.
     * It will ignore any argument in messageSelector.
     * 
     * @param destination
     *            a queue destination
     * @param messageSelector           
     * @return new message consumer
     * @throws JMSException
     *             If session is closed or queue destination is not used
     */

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        if (messageSelector != null) {
            throw new JMSException("SQSSession does not support MessageSelector. This should be null.");
        }
        return createConsumer(destination);
    }
    
    /**
     * Creates a <code>MessageConsumer</code> for the specified destination.
     * Only queue destinations are supported at this time. It will ignore any
     * argument in messageSelector and NoLocal.
     * 
     * @param destination
     *            a queue destination
     * @param messageSelector
     * @param NoLocal
     * @return new message consumer
     * @throws JMSException
     *             If session is closed or queue destination is not used
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
     * @param queueName
     * @return a queue destination
     * @throws JMSException
     *         If session is closed or invalid queue is provided
     */
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        checkClosed();
        return new SQSQueueDestination(queueName, amazonSQSClient.getQueueUrl(queueName).getQueueUrl());
    }
    
    /**
     * This does not create SQS Queue. This method is only to create JMS Queue
     * Object. Make sure the queue exists corresponding to the queueName and
     * ownerAccountId.
     * 
     * @param queueName
     * @param ownerAccountId
     *            the account id, which originally created the queue on SQS
     * @return a queue destination
     * @throws JMSException
     *             If session is closed or invalid queue is provided
     */
    public Queue createQueue(String queueName, String ownerAccountId) throws JMSException {
        checkClosed();
        return new SQSQueueDestination(
                queueName, amazonSQSClient.getQueueUrl(queueName, ownerAccountId).getQueueUrl());
    }

    /**
     * This is used in MessageConsumer. When MessageConsumer is closed
     * it will remove itself from list of consumers.
     */
    void removeConsumer(SQSMessageConsumer consumer) {
        messageConsumers.remove(consumer);
    }

    /**
     * This is used in MessageProducer. When MessageProducer is closed
     * it will remove itself from list of producers.
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
                    LOG.warn("Interrupted while waiting on session start. Continue to wait...", e);
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
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    LOG.warn(
                            "Interrupted while waiting the active consumer in callback to complete. Continue to wait...",
                            e);
                }
            }
        }
    }
    
    void waitForCallbackComplete() {
        synchronized (stateLock) {
            while (activeConsumerInCallback != null) {
                try {
                    stateLock.wait();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting on session callback completion. Continue to wait...", e);
                }
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
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. This method is related to transaction which SQS doesn't support */
    @Override
    public void rollback() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. This method is related to Topic which SQS doesn't support */
    @Override
    public void unsubscribe(String name) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name, String messageSelector, boolean noLocal) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public MessageListener getMessageListener() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public MapMessage createMapMessage() throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
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
     * Check if session is closed.
     */
    public void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Session is closed");
        }
    }
    
    /**
     * Check if session is closed or closing.
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
            waitForCallbackComplete();

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

    void setClosing(boolean closing) {
        this.closing = closing;
    }

    void setRunning(boolean running) {
        this.running = running;
    }

    boolean isRunning() {
        return running;
    }

    SQSSessionCallbackScheduler getSqsSessionRunnable() {
        return sqsSessionRunnable;
    }
}
