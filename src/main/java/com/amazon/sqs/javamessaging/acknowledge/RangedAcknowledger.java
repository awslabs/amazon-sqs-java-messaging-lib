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
package com.amazon.sqs.javamessaging.acknowledge;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClient;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import jakarta.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Used to acknowledge group of messages. Acknowledging a consumed message
 * acknowledges all messages that the session has consumed before and including
 * that message.
 * <P>
 * A big backlog of consumed messages can cause memory pressure, as well as an
 * increase on the probability of duplicates.
 * <P>
 * This class is not safe for concurrent use.
 */
public class RangedAcknowledger extends BulkSQSOperation implements Acknowledger {
    private static final Logger LOG = LoggerFactory.getLogger(RangedAcknowledger.class);
    
    private final AmazonSQSMessagingClient amazonSQSClient;

    private final SQSSession session;
    
    private final Queue<SQSMessageIdentifier> unAckMessages;

    public RangedAcknowledger(AmazonSQSMessagingClient amazonSQSClient, SQSSession session) {
        this.amazonSQSClient = amazonSQSClient;
        this.session = session;
        this.unAckMessages  = new LinkedList<>();
    }
    
    /**
     * Acknowledges all the consumed messages as well as the previously consumed
     * messages on the session via calling <code>deleteMessageBatch</code> until
     * all the messages are deleted.
     */
    @Override
    public void acknowledge(SQSMessage message) throws JMSException {
        session.checkClosed();

        SQSMessageIdentifier ackMessage = SQSMessageIdentifier.fromSQSMessage(message);

        int indexOfMessage = indexOf(ackMessage);

        /*
         * In case the message has already been deleted, warn user about it and
         * return. If not then it should continue with acknowledging all
         * the messages received before that
         */
        if (indexOfMessage == -1) {
            LOG.warn("SQSMessageID: {} with SQSMessageReceiptHandle: {} does not exist.", message.getSQSMessageId(),
                    message.getReceiptHandle());
        } else {
            bulkAction(getUnAckMessages(), indexOfMessage);
        }
    }

    /**
     * Return the index of message if the message is in queue. Return -1 if
     * message does not exist in queue.
     */
    private int indexOf(SQSMessageIdentifier findMessage) {
        int i = 0;
        for (SQSMessageIdentifier sqsMessageIdentifier : unAckMessages) {
            i++;
            if (sqsMessageIdentifier.equals(findMessage)) {
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Updates the internal queue for the consumed but not acknowledged
     * messages if the message was not already on queue.
     */
    @Override
    public void notifyMessageReceived(SQSMessage message) throws JMSException {
        SQSMessageIdentifier messageIdentifier = SQSMessageIdentifier.fromSQSMessage(message);
        if (!unAckMessages.contains(messageIdentifier)) {
            unAckMessages.add(messageIdentifier);
        }
    } 
    
    /**
     * Returns the list of all consumed but not acknowledged messages.
     */
    @Override
    public List<SQSMessageIdentifier> getUnAckMessages() {
        return new ArrayList<>(unAckMessages);
    }
    
    /**
     * Clears the list of not acknowledged messages.
     */
    @Override
    public void forgetUnAckMessages() {
        unAckMessages.clear();
    }
    
    /**
     * Acknowledges up to 10 messages via calling
     * <code>deleteMessageBatch</code>.
     */
    @Override
    public void action(String queueUrl, List<String> receiptHandles) throws JMSException {
        if (receiptHandles == null || receiptHandles.isEmpty()) {
            return;
        }

        List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = new ArrayList<>();
        int batchId = 0;
        for (String receiptHandle : receiptHandles) {
            // Remove the message from queue of unAckMessages
            unAckMessages.poll();
            
            DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder()
            		.id( Integer.toString(batchId))
            		.receiptHandle(receiptHandle)
            		.build();
            deleteMessageBatchRequestEntries.add(entry);
            batchId++;
        }
        
        DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest
        		.builder()
        		.queueUrl(queueUrl)
        		.entries(deleteMessageBatchRequestEntries)
        		.build();
        /*
         * TODO: If one of the batch calls fail, then the remaining messages on
         * the batch will not be deleted, and will be visible and delivered as
         * duplicate after visibility timeout expires.
         */
        amazonSQSClient.deleteMessageBatch(deleteMessageBatchRequest);
    }
}
