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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch.MessageManager;
import com.amazon.sqs.javamessaging.SQSSession.CallbackEntry;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import com.amazon.sqs.javamessaging.message.SQSMessage;

/**
 * Used internally to guarantee serial execution of message processing on
 * consumer message listeners.
 */
public class SQSSessionCallbackScheduler implements Runnable {
    private static final Log LOG = LogFactory.getLog(SQSSessionCallbackScheduler.class);
    
    protected ArrayDeque<CallbackEntry> callbackQueue;

    private AcknowledgeMode acknowledgeMode;

    private SQSSession session;
    
    private NegativeAcknowledger negativeAcknowledger;
    
    private final Acknowledger acknowledger;
    
    /**
     * Only set from the callback thread to transfer the ownership of closing
     * the consumer. The message listener's onMessage method can call
     * <code>close</code> on its own consumer. After message consumer
     * <code>close</code> returns the onMessage method should be allowed to
     * complete normally and this thread will be responsible to finish the
     * <code>close</code> on message consumer.
     */
    private SQSMessageConsumer consumerCloseAfterCallback;
    
    private volatile boolean closed = false;
        
    SQSSessionCallbackScheduler(SQSSession session, AcknowledgeMode acknowledgeMode, Acknowledger acknowledger, NegativeAcknowledger negativeAcknowledger) {
        this.session = session;
        this.acknowledgeMode = acknowledgeMode;
        this.acknowledger = acknowledger;
        this.negativeAcknowledger = negativeAcknowledger;
        callbackQueue = new ArrayDeque<CallbackEntry>();
    }
    
    /**
     * Used in case no consumers have started, and session needs to terminate
     * the thread
     */
    void close() {
        closed = true;
        /** Wake-up the thread in case it was blocked on empty queue */
        synchronized (callbackQueue) {
            callbackQueue.notify();
        }
    }
    
    @Override
    public void run() {
        CallbackEntry callbackEntry = null;
        try {            
            while (true) {
                try {
                    if (closed) {
                        break;
                    }
                    synchronized (callbackQueue) {
                        callbackEntry = callbackQueue.pollFirst();
                        if (callbackEntry == null) {
                            try {
                                callbackQueue.wait();
                            } catch (InterruptedException e) {
                                /**
                                 * Will be retried on the next loop, and
                                 * break if the callback scheduler is closed.
                                 */
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("wait on empty callback queue interrupted: " + e.getMessage());
                                }
                            }
                            continue;
                        }
                    }

                    MessageListener messageListener = callbackEntry.getMessageListener();
                    MessageManager messageManager = callbackEntry.getMessageManager();
                    SQSMessage message = (SQSMessage) messageManager.getMessage();    
                    SQSMessageConsumer messageConsumer = messageManager.getPrefetchManager().getMessageConsumer();
                    if (messageConsumer.isClosed()) {
                        nackReceivedMessage(message);
                        continue;
                    }
                    
                    try {
                        // this takes care of start and stop
                        session.startingCallback(messageConsumer);
                    } catch (JMSException e) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Not running callback: " + e.getMessage());
                        }
                        break;
                    }

                    try {
                        /**
                         * Notifying consumer prefetch thread so that it can
                         * continue to prefetch
                         */
                        messageManager.getPrefetchManager().messageDispatched();
                        int ackMode = acknowledgeMode.getOriginalAcknowledgeMode();
                        boolean tryNack = true;
                        try {
                            if (messageListener != null) {
                                if (ackMode != Session.AUTO_ACKNOWLEDGE) {
                                    acknowledger.notifyMessageReceived(message);
                                }
                                boolean callbackFailed = false;
                                try {
                                    messageListener.onMessage(message);
                                } catch (Throwable ex) {
                                    LOG.info("Exception thrown from onMessage callback for message " +
                                             message.getSQSMessageId(), ex);
                                    callbackFailed = true;
                                } finally {
                                    if (!callbackFailed) {
                                        if (ackMode == Session.AUTO_ACKNOWLEDGE) {
                                            message.acknowledge();
                                        }
                                        tryNack = false;
                                    }
                                }
                            }
                        } catch (JMSException ex) {
                            LOG.warn(
                                    "Unable to complete message dispatch for the message " +
                                            message.getSQSMessageId(), ex);
                        } finally {
                            if (tryNack) {
                                nackReceivedMessage(message);
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
                    } finally {
                        session.finishedCallback();
                        
                        // Let the prefetch manager know we're available to
                        // process another message (if there is a still a listener attached).
                        messageManager.getPrefetchManager().messageListenerReady();
                    }
                } catch (Throwable ex) {
                    LOG.error("Unexpected exception thrown during the run of the scheduled callback", ex);
                }
            }
        } finally {
            if (callbackEntry != null) {
                nackReceivedMessage((SQSMessage) callbackEntry.getMessageManager().getMessage());
            }
            nackQueuedMessages();
        }
    }
    
    void setConsumerCloseAfterCallback(SQSMessageConsumer messageConsumer) {
        consumerCloseAfterCallback = messageConsumer;
    }
    
    void scheduleCallBacks(MessageListener messageListener, List<MessageManager> messageManagers) {
        synchronized (callbackQueue) {
            try {
                for (MessageManager messageManager : messageManagers) {
                    CallbackEntry callbackEntry = new CallbackEntry(messageListener, messageManager);
                    callbackQueue.addLast(callbackEntry);
                }
            } finally {
                callbackQueue.notify();
            }
        }
    }
    
    void nackQueuedMessages() {
        synchronized (callbackQueue) {
            try {
                List<SQSMessageIdentifier> nackMessageIdentifiers = new ArrayList<SQSMessageIdentifier>();
                while (!callbackQueue.isEmpty()) {
                    SQSMessage nackMessage = (SQSMessage) callbackQueue.pollFirst().getMessageManager().getMessage();
                    nackMessageIdentifiers.add(SQSMessageIdentifier.fromSQSMessage(nackMessage));
                }

                if (!nackMessageIdentifiers.isEmpty()) {
                    negativeAcknowledger.bulkAction(nackMessageIdentifiers, nackMessageIdentifiers.size());
                }
            } catch (JMSException e) {
                LOG.warn("Caught exception while nacking the remaining messages on session callback queue", e);
            }
        }
    }

    private void nackReceivedMessage(SQSMessage message) {
        try {
            SQSMessageIdentifier messageIdentifier = SQSMessageIdentifier.fromSQSMessage(message);
            List<SQSMessageIdentifier> nackMessageIdentifiers = new ArrayList<SQSMessageIdentifier>();
            nackMessageIdentifiers.add(messageIdentifier);
            
            //failing to process a message with a specific group id means we have to nack all the pending messages with the same group id
            //to prevent processing messages out of order
            if (messageIdentifier.getGroupId() != null) {
                //prepare a map of single queueUrl with single group to purge
                Map<String, Set<String>> queueToGroupsMapping = Collections.singletonMap(messageIdentifier.getQueueUrl(), Collections.singleton(messageIdentifier.getGroupId()));
                nackMessageIdentifiers.addAll(purgeScheduledCallbacksForQueuesAndGroups(queueToGroupsMapping));
            }
            
            negativeAcknowledger.bulkAction(nackMessageIdentifiers, nackMessageIdentifiers.size());
        } catch (JMSException e) {
            LOG.warn("Unable to nack the message " + message.getSQSMessageId(), e);
        }
    }

    List<SQSMessageIdentifier> purgeScheduledCallbacksForQueuesAndGroups(Map<String, Set<String>> queueToGroupsMapping) throws JMSException {
        List<SQSMessageIdentifier> purgedCallbacks = new ArrayList<SQSMessageIdentifier>();
        synchronized (callbackQueue) {
            //let's walk over the callback queue
            Iterator<CallbackEntry> callbackIterator = callbackQueue.iterator();
            while (callbackIterator.hasNext()) {
                CallbackEntry callbackEntry = callbackIterator.next();
                SQSMessageIdentifier pendingCallbackIdentifier = SQSMessageIdentifier.fromSQSMessage((SQSMessage) callbackEntry.getMessageManager().getMessage());
                
                //is the callback entry for one of the affected queues?
                Set<String> affectedGroupsInQueue = queueToGroupsMapping.get(pendingCallbackIdentifier.getQueueUrl());
                if (affectedGroupsInQueue != null) {
                    
                    //is the callback entry for one of the affected group ids?
                    if (affectedGroupsInQueue.contains(pendingCallbackIdentifier.getGroupId())) {
                        //we will purge this callback
                        purgedCallbacks.add(pendingCallbackIdentifier);
                        //remove from callback queue
                        callbackIterator.remove();
                        //notify prefetcher for that message that we are done with it and it can prefetch more messages
                        callbackEntry.getMessageManager().getPrefetchManager().messageDispatched();
                    }
                }
            }
        }
        return purgedCallbacks;
    }
}