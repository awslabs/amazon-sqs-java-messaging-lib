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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.JMSException;

import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.sqsjms.AmazonSQSClientJMSWrapper;
import com.amazonaws.sqsjms.BulkSQSOperation;
import com.amazonaws.sqsjms.SQSJMSClientConstants;
import com.amazonaws.sqsjms.SQSMessage;
import com.amazonaws.sqsjms.SQSMessageConsumerPrefetch.MessageManager;

public class NegativeAcknowledger extends BulkSQSOperation {

    private static final AtomicLong DEFAULT_BATCH_ID_GENERATOR = new AtomicLong();

    private static final int NACK_TIMEOUT = 0;

    private final AtomicLong batchIdGenerator;

    private final AmazonSQSClientJMSWrapper amazonSQSClient;

    public NegativeAcknowledger(AmazonSQSClientJMSWrapper amazonSQSClient, AtomicLong batchIdGenerator) {
        this.amazonSQSClient = amazonSQSClient;
        this.batchIdGenerator = batchIdGenerator;
    }

    public NegativeAcknowledger(AmazonSQSClientJMSWrapper amazonSQSClient) {
        this.amazonSQSClient = amazonSQSClient;
        this.batchIdGenerator = DEFAULT_BATCH_ID_GENERATOR;
    }

    public void bulkAction(ArrayDeque<MessageManager> messageQueue, String queueUrl) throws JMSException {
        List<String> receiptHandles = new ArrayList<String>();
        while (!messageQueue.isEmpty()) {
            receiptHandles.add(((SQSMessage) (messageQueue.pollFirst().getMessage())).getReceiptHandle());

            // If you have more than 10 stop
            if (receiptHandles.size() == SQSJMSClientConstants.MAX_BATCH) {
                action(queueUrl, receiptHandles);
                receiptHandles.clear();
            }
        }
        action(queueUrl, receiptHandles);
    }

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

