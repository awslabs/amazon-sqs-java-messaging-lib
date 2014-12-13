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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.sqsjms.SQSMessageConsumerPrefetch.MessageManager;
import com.amazonaws.sqsjms.SQSSession.CallbackEntry;
import com.amazonaws.sqsjms.acknowledge.AcknowledgeMode;
import com.amazonaws.sqsjms.acknowledge.Acknowledger;
import com.amazonaws.sqsjms.acknowledge.NegativeAcknowledger;
import com.amazonaws.sqsjms.acknowledge.SQSMessageIdentifier;

public class SQSSessionCallbackScheduler implements Runnable {
    private static final Log LOG = LogFactory.getLog(SQSSessionCallbackScheduler.class);
    
    private final AtomicLong changeMessageVisibilityIdGenerator = new AtomicLong();

    protected ArrayDeque<CallbackEntry> callbackQueue;

    private AcknowledgeMode acknowledgeMode;

    private SQSSession session;
    
    protected NegativeAcknowledger negativeAcknowledger;
    
    private final Acknowledger acknowledger;
    
    private volatile boolean closed;
    
    private volatile Thread currentThread;
    
    private volatile SQSMessageConsumer consumerCloseAfterCallback;
    
    private final Object synchronizer = new Object();

    SQSSessionCallbackScheduler(SQSSession session, AcknowledgeMode acknowledgeMode, Acknowledger acknowledger) {
        this.session = session;
        this.acknowledgeMode = acknowledgeMode;
        callbackQueue = new ArrayDeque<CallbackEntry>();
        negativeAcknowledger = new NegativeAcknowledger(
                this.session.getParentConnection().getWrappedAmazonSQSClient(), changeMessageVisibilityIdGenerator);
        this.acknowledger = acknowledger;
    }

    void close() {
        synchronized (synchronizer) {
            closed = true;
            synchronizer.notifyAll();
        }
    }
    
    @Override
    public void run() {
        currentThread = Thread.currentThread();

        while (true) {
            try {
                session.startingCallback(); // this takes care of start and stop
            } catch (InterruptedException e) {
                break;
            } finally {
                if (closed) {
                    nackQueuedMessages();
                    break;
                }
            }

            try {
                CallbackEntry callbackEntry;
                synchronized (callbackQueue) {
                    callbackEntry = callbackQueue.pollFirst();
                }
                if (callbackEntry != null) {
                    MessageListener messageListener = callbackEntry.getMessageListener();
                    MessageManager messageManager = callbackEntry.getMessageManager();
                    SQSMessage message = (SQSMessage) messageManager.getMessage();
                    /**
                     * Notifying consumer prefetch thread so that it can
                     * continue to prefetch
                     */
                    messageManager.getPrefetchManager().messageDispatched();
                    int ackMode= acknowledgeMode.getOriginalAcknowledgeMode();
                    synchronized (synchronizer) {
                        boolean tryNack = false;
                        try {
                            if (messageListener != null) {
                                if (ackMode != Session.AUTO_ACKNOWLEDGE) {
                                    acknowledger.notifyMessageReceived(message);
                                }
                                messageListener.onMessage(message);
                                try {
                                    if (ackMode == Session.AUTO_ACKNOWLEDGE) {
                                        message.acknowledge();
                                    }
                                } catch (JMSException ex) {
                                    LOG.warn(
                                            "Unable to ack the messageId " +
                                                    message.getSQSMessageId(), ex);
                                    tryNack = true;
                                }
                            } else {
                                /**
                                 * Nack the message if message listener becomes
                                 * null after scheduled for delivery
                                 */
                                tryNack = true;
                            }
                        } catch (Throwable ex) {
                            LOG.error(
                                    "Caught exception while processing the messageId " +
                                            message.getSQSMessageId(), ex);
                            tryNack = true;
                        } finally {
                            if (tryNack) {
                                try {
                                    negativeAcknowledger.action(
                                            message.getQueueUrl(),
                                            Collections.singletonList(message.getReceiptHandle()));
                                } catch (JMSException ex) {
                                    LOG.info(
                                            "Unable to nack the messageId " +
                                                    message.getSQSMessageId());
                                }
                            }
                        }
                        
                        /**
                         * The consumer close is delegated to the session thread
                         * if consumer close is called by its message listener's
                         * onMessage method on its own consumer.
                         */
                        if (consumerCloseAfterCallback != null) {
                            consumerCloseAfterCallback.doClose();
                            consumerCloseAfterCallback = null;
                        }
                        synchronizer.notifyAll();
                    }
                    
                } 
            } finally {
                session.finishedCallback();
            }
        }

        callbackQueue.clear();
    }
    
    public Thread getCurrentThread() {
        return currentThread;
    }
    
    public void setConsumerCloseAfterCallback(SQSMessageConsumer messageConsumer) {
        consumerCloseAfterCallback = messageConsumer;
    }
    
    public Object getSynchronizer() {
        return synchronizer;
    }
    
    void scheduleCallBack(MessageListener messageListener, MessageManager messageManager) {
        CallbackEntry callbackEntry = new CallbackEntry(messageListener, messageManager);
        
        synchronized (callbackQueue) {
            callbackQueue.push(callbackEntry);
        }
    }
            
    protected void nackQueuedMessages() {
        synchronized (callbackQueue) {
            try {
                List<SQSMessageIdentifier> nackMessageIdentifiers = new ArrayList<SQSMessageIdentifier>();
                while (!callbackQueue.isEmpty()) {
                    SQSMessage nackMessage = (SQSMessage) callbackQueue.pollFirst().getMessageManager().getMessage();
                    nackMessageIdentifiers.add(new SQSMessageIdentifier(
                            nackMessage.getQueueUrl(), nackMessage.getReceiptHandle(),
                            nackMessage.getSQSMessageId()));
                }

                if (!nackMessageIdentifiers.isEmpty()) {
                    negativeAcknowledger.bulkAction(nackMessageIdentifiers, nackMessageIdentifiers.size());
                }
            } catch (JMSException e) {
                LOG.warn(
                        "Caught exception while nacking the remaining messages on session callback queue", e);
            }
        }
    }   
}