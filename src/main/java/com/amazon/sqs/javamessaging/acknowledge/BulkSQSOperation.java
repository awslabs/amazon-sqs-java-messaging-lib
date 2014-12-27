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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.SQSMessagingClientConstants;

/**
 * This is used by different acknowledgers that requires partitioning of the
 * list, and execute actions on the partitions
 */
public abstract class BulkSQSOperation {
    
    /**
     * Bulk action on list of message identifiers up to the provided index
     * 
     * @param messageIdentifierList
     *            Container for the list of message identifiers
     * @param indexOfMessage
     *            The action will apply to all messages up to this index
     * @throws JMSException
     *             if <code>action</code> throws
     */
    public void bulkAction(List<SQSMessageIdentifier> messageIdentifierList, int indexOfMessage)
            throws JMSException {

        assert indexOfMessage > 0;
        assert indexOfMessage <= messageIdentifierList.size();

        Map<String, List<String>> receiptHandleWithSameQueueUrl = new HashMap<String, List<String>>();
        
        // Add all messages up to and including requested message into Map.
        // Map contains key as queueUrl and value as list receiptHandles from
        // that queueUrl.

        for (int i = 0; i < indexOfMessage; i++) {
            SQSMessageIdentifier messageIdentifier = messageIdentifierList.get(i);
            String queueUrl = messageIdentifier.getQueueUrl();
            List<String> receiptHandles = receiptHandleWithSameQueueUrl.get(queueUrl);
            // if value of queueUrl is null create new list.
            if (receiptHandles == null) {
                receiptHandles = new ArrayList<String>();
                receiptHandleWithSameQueueUrl.put(queueUrl, receiptHandles);
            }
            // add receiptHandle to the list.
            receiptHandles.add(messageIdentifier.getReceiptHandle());
            // Once there are 10 messages in messageBatch, apply the batch action
            if (receiptHandles.size() == SQSMessagingClientConstants.MAX_BATCH) {
                action(queueUrl, receiptHandles);
                receiptHandles.clear();
            }
        }

        // Flush rest of messages in map.
        for (Entry<String, List<String>> entry : receiptHandleWithSameQueueUrl.entrySet()) {
            action(entry.getKey(), entry.getValue());
        }
    }
         
    /**
     * Action call block. This action can be applied on multiple messages for
     * the same queue.
     * 
     * @param queueUrl
     *            queueUrl of the queue, which the receipt handles belong
     * @param receiptHandles
     *            the list of handles, which is be used to (negative)acknowledge
     *            the messages.
     * @throws JMSException
     */
    public abstract void action(String queueUrl, List<String> receiptHandles) throws JMSException;
        
}
