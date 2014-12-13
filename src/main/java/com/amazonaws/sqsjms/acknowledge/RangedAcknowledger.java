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
package com.amazonaws.sqsjms.acknowledge;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.sqsjms.AmazonSQSClientJMSWrapper;
import com.amazonaws.sqsjms.BulkSQSOperation;
import com.amazonaws.sqsjms.SQSMessage;
import com.amazonaws.sqsjms.SQSSession;

public class RangedAcknowledger extends BulkSQSOperation implements Acknowledger {
    private static final Log LOG = LogFactory.getLog(RangedAcknowledger.class);
    
    protected static final AtomicLong batchIdGenerator = new AtomicLong();

    private final AmazonSQSClientJMSWrapper amazonSQSClient;

    private final SQSSession session;
    
    private final Queue<SQSMessageIdentifier> unAckMessages;

    public RangedAcknowledger(AmazonSQSClientJMSWrapper amazonSQSClient, SQSSession session) {
        this.amazonSQSClient = amazonSQSClient;
        this.session = session;
        this.unAckMessages  = new LinkedList<SQSMessageIdentifier>();
    }

    @Override
    public void acknowledge(SQSMessage message) throws JMSException {      
        session.checkClosed();
        /*
         * In case the message has already been deleted, warn user about it and return.
         * If not then then it should continue with acknowledging all the
         * messages received before that
         */
        SQSMessageIdentifier ackMessage = new SQSMessageIdentifier(message.getQueueUrl(), message.getReceiptHandle(), message.getSQSMessageId());

        int indexOfMessage = indexOf(ackMessage);

        if (indexOfMessage == -1) {
            LOG.warn("SQSMessageID: " + message.getSQSMessageId() + " with SQSMessageReceiptHandle: " + message.getReceiptHandle()
                    + " does not exist.");
        } else {
            bulkAction(getUnAckMessages(), indexOfMessage);
        }
    }

    /**
     * Return the index of message if the message in queue.
     * method will return -1 if message does not exist in Queue.
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

    @Override
    public void notifyMessageReceived(SQSMessage message) throws JMSException {
        SQSMessageIdentifier messageIdentifier = new SQSMessageIdentifier(message.getQueueUrl(), message.getReceiptHandle(), message.getSQSMessageId());
        if (!unAckMessages.contains(messageIdentifier)) {
            unAckMessages.add(messageIdentifier);
        }
    }

    @Override
    public List<SQSMessageIdentifier> getUnAckMessages() {
        return new ArrayList<SQSMessageIdentifier>(unAckMessages);
    }

    @Override
    public void forgetUnAckMessages() {
        unAckMessages.clear();
    }

    @Override
    public void action(String queueUrl, List<String> receiptHandles) throws JMSException {
        if (receiptHandles == null || receiptHandles.isEmpty()) {
            return;
        }
        
        List<DeleteMessageBatchRequestEntry> deleteMessageBatchRequestEntries = new ArrayList<DeleteMessageBatchRequestEntry>();
        for (String receiptHandle : receiptHandles) {
            // Remove the message from Queue of unAckMessages
            unAckMessages.poll();
            
            DeleteMessageBatchRequestEntry entry = new DeleteMessageBatchRequestEntry(Long.toString(batchIdGenerator
                    .incrementAndGet()), receiptHandle);
            deleteMessageBatchRequestEntries.add(entry);
        }

        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest(queueUrl,
                deleteMessageBatchRequestEntries);
        amazonSQSClient.deleteMessageBatch(deleteMessageBatchRequest);
    }
}
