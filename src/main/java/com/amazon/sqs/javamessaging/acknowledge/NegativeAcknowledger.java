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
package com.amazon.sqs.javamessaging.acknowledge;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSMessagingClientConstants;
import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch.MessageManager;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;

/**
 * Used to negative acknowledge of group of messages.
 * <P>
 * Negative acknowledge resets the visibility timeout of a message, so that the
 * message can be immediately available to consume. This is mostly used on
 * <code>recover</code> and <code>close</code> methods.
 * <P>
 * Negative acknowledge can potentially cause duplicate deliveries.
 */
public class NegativeAcknowledger extends BulkSQSOperation {

    private static final AtomicLong DEFAULT_BATCH_ID_GENERATOR = new AtomicLong();

    private static final int NACK_TIMEOUT = 0;

    private final AtomicLong batchIdGenerator;

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;

    public NegativeAcknowledger(AmazonSQSMessagingClientWrapper amazonSQSClient, AtomicLong batchIdGenerator) {
        this.amazonSQSClient = amazonSQSClient;
        this.batchIdGenerator = batchIdGenerator;
    }

    public NegativeAcknowledger(AmazonSQSMessagingClientWrapper amazonSQSClient) {
        this.amazonSQSClient = amazonSQSClient;
        this.batchIdGenerator = DEFAULT_BATCH_ID_GENERATOR;
    }
    
    /**
     * Bulk action for negative acknowledge on the list of messages of a
     * specific queue.
     * 
     * @param messageQueue
     *            Container for the list of message managers.
     * @param queueUrl
     *            The queueUrl of the messages, which they received from.
     * @throws JMSException
     *             If <code>action</code> throws.
     */
    public void bulkAction(ArrayDeque<MessageManager> messageQueue, String queueUrl) throws JMSException {
        List<String> receiptHandles = new ArrayList<String>();
        while (!messageQueue.isEmpty()) {
            receiptHandles.add(((SQSMessage) (messageQueue.pollFirst().getMessage())).getReceiptHandle());

            // If there is more than 10 stop can call action
            if (receiptHandles.size() == SQSMessagingClientConstants.MAX_BATCH) {
                action(queueUrl, receiptHandles);
                receiptHandles.clear();
            }
        }
        action(queueUrl, receiptHandles);
    }
    
    /**
     * Action call block for negative acknowledge for the list of receipt
     * handles. This action can be applied on multiple messages for the same
     * queue.
     * 
     * @param queueUrl
     *            The queueUrl of the queue, which the receipt handles belong.
     * @param receiptHandles
     *            The list of handles, which is be used to negative acknowledge
     *            the messages via using
     *            <code>changeMessageVisibilityBatch</code>.
     * @throws JMSException
     *             If <code>changeMessageVisibilityBatch</code> throws.
     */
    @Override
    public void action(String queueUrl, List<String> receiptHandles) throws JMSException {

        if (receiptHandles == null || receiptHandles.isEmpty()) {
            return;
        }

        List<ChangeMessageVisibilityBatchRequestEntry> nackEntries = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>(
                receiptHandles.size());
        for (String messageReceiptHandle : receiptHandles) {
            ChangeMessageVisibilityBatchRequestEntry changeMessageVisibilityBatchRequestEntry = new ChangeMessageVisibilityBatchRequestEntry(
                    batchIdGenerator.getAndIncrement() + "", messageReceiptHandle).withVisibilityTimeout(NACK_TIMEOUT);
            nackEntries.add(changeMessageVisibilityBatchRequestEntry);
        }
        amazonSQSClient.changeMessageVisibilityBatch(new ChangeMessageVisibilityBatchRequest(
                queueUrl, nackEntries));
    }

}

