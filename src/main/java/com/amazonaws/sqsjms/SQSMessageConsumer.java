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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import com.amazonaws.sqsjms.acknowledge.Acknowledger;
import com.amazonaws.sqsjms.acknowledge.NegativeAcknowledger;
import com.amazonaws.sqsjms.acknowledge.SQSMessageIdentifier;

public class SQSMessageConsumer implements MessageConsumer, QueueReceiver {
    private static final Log LOG = LogFactory.getLog(SQSMessageConsumer.class);
    public static final int PREFETCH_EXECUTOR_GRACEFUL_SHUTDOWN_TIME = 60;

    protected volatile boolean closed = false;
    
    private final AmazonSQSClientJMSWrapper amazonSQSClient;
    private final SQSDestination sqsDestination;
    private final Acknowledger acknowledger;
    private final SQSSession parentSQSSession;

    private final NegativeAcknowledger negativeAcknowledger;

    private final SQSSessionCallbackScheduler sqsSessionRunnable; 

    /**
     * Executor for prefetch thread.
     */
    private final ExecutorService prefetchExecutor;
    
    /**
     * Prefetch Runnable. This include keeping internal message buffer filled and call MessageListener if set.
     */
    private final SQSMessageConsumerPrefetch sqsMessageConsumerPrefetch;
    
    private final Object callBackSynchronizer;
    
    SQSMessageConsumer(SQSConnection parentSQSConnection, SQSSession parentSQSSession,
                       SQSSessionCallbackScheduler sqsSessionRunnable, SQSDestination destination,
                       Acknowledger acknowledger, NegativeAcknowledger negativeAcknowledger, ThreadFactory threadFactory) {
        this(parentSQSConnection, parentSQSSession,
                sqsSessionRunnable, destination,
                acknowledger, negativeAcknowledger, threadFactory,
                new SQSMessageConsumerPrefetch(sqsSessionRunnable, acknowledger, negativeAcknowledger, destination,
                                               parentSQSConnection.getWrappedAmazonSQSClient(),
                                               parentSQSConnection.getNumberOfMessagesToPrefetch()));

    }

    SQSMessageConsumer(SQSConnection parentSQSConnection, SQSSession parentSQSSession,
                       SQSSessionCallbackScheduler sqsSessionRunnable, SQSDestination destination,
                       Acknowledger acknowledger, NegativeAcknowledger negativeAcknowledger, ThreadFactory threadFactory,
                       SQSMessageConsumerPrefetch sqsMessageConsumerPrefetch) {
        this.amazonSQSClient = parentSQSConnection.getWrappedAmazonSQSClient();
        this.parentSQSSession = parentSQSSession;
        this.sqsDestination = destination;
        this.acknowledger = acknowledger;
        this.sqsSessionRunnable = sqsSessionRunnable;
        this.sqsMessageConsumerPrefetch = sqsMessageConsumerPrefetch;
        this.negativeAcknowledger = negativeAcknowledger;

        prefetchExecutor = Executors.newSingleThreadExecutor(threadFactory);
        prefetchExecutor.execute(sqsMessageConsumerPrefetch);

        callBackSynchronizer = sqsSessionRunnable.getSynchronizer();
    }



    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) sqsDestination;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return sqsMessageConsumerPrefetch.getMessageListener();
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        checkClosed();
        this.sqsMessageConsumerPrefetch.setMessageListener(listener);
    }

    /**
     * This call blocks indefinitely until a message is produced or until this message consumer is closed.
     * When ConnectionState is stopped receive is paused.
     */
    @Override
    public Message receive() throws JMSException {
        return sqsMessageConsumerPrefetch.receive();
    }

    /**
     * This call blocks until a message arrives, the timeout expires, or this message consumer is closed.
     * A timeout of zero never expires, and the call blocks indefinitely.
     */
    @Override
    public Message receive(long timeout) throws JMSException {
        return sqsMessageConsumerPrefetch.receive(timeout);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return sqsMessageConsumerPrefetch.receiveNoWait();
    }
    

    @Override
    public void close() throws JMSException {
        if (closed) {
            return;
        }
        synchronized (callBackSynchronizer) {
            if (Thread.currentThread() == sqsSessionRunnable.getCurrentThread()) {
                sqsSessionRunnable.setConsumerCloseAfterCallback(this);
                return;
            }

            doClose();
            callBackSynchronizer.notifyAll();
        }
    }
    
    void doClose() {
        if (closed) {
            return;
        }

        sqsMessageConsumerPrefetch.close();

        parentSQSSession.removeConsumer(this);

        try {
            LOG.info("Shutting down " + SQSSession.CONSUMER_PREFETCH_EXECUTER_NAME + " executor");

            /** Shut down executor. */
            prefetchExecutor.shutdown();

            if (!prefetchExecutor.awaitTermination(PREFETCH_EXECUTOR_GRACEFUL_SHUTDOWN_TIME, TimeUnit.SECONDS)) {

                LOG.warn("Can't terminate executor service " +
                         SQSSession.CONSUMER_PREFETCH_EXECUTER_NAME + " after " + 60 +
                         " seconds, some running threads will be shutdown immediately");
                prefetchExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Interrupted while closing the consumer.", e);
        }
        closed = true;
    }
    

    /**
     * Set the message visibility as zero for the list of messages which are not
     * acknowledged and delete them from the list of unacknowledged messages.
     */
    void recover() throws JMSException {
        List<SQSMessageIdentifier> unAckedMessages = acknowledger.getUnAckMessages();
        negativeAcknowledger.bulkAction(unAckedMessages, unAckedMessages.size());

        acknowledger.forgetUnAckMessages();
    }

    /** This method is not supported. */
    @Override
    public String getMessageSelector() throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }
    
    /** This call blocks until message listener in progress have completed. */
    protected void stop() {
        if (!closed) {
            synchronized (callBackSynchronizer) {
                sqsMessageConsumerPrefetch.stop();
                callBackSynchronizer.notifyAll();
            }
        }
    }

    protected void start() {
        if (!closed) {
            synchronized (callBackSynchronizer) {
                sqsMessageConsumerPrefetch.start();
                callBackSynchronizer.notifyAll();
            }
        }
    }
       
    private void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
    }
}
