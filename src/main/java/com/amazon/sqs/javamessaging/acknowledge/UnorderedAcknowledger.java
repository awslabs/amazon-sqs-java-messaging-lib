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

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

/**
 * Used to acknowledge messages in any order one at a time.
 * <P>
 * This class is not safe for concurrent use.
 */
public class UnorderedAcknowledger implements Acknowledger {

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;
    
    private final SQSSession session;
    
    // key is the receipt handle of the message and value is the message
    // identifier
    private final Map<String, SQSMessageIdentifier> unAckMessages;

    public UnorderedAcknowledger (AmazonSQSMessagingClientWrapper amazonSQSClient, SQSSession session) {
        this.amazonSQSClient = amazonSQSClient;
        this.session = session;
        this.unAckMessages  = new HashMap<String, SQSMessageIdentifier>();
    }
    
    /**
     * Acknowledges the consumed message via calling <code>deleteMessage</code>.
     */
    @Override
    public void acknowledge(SQSMessage message) throws JMSException {
        session.checkClosed();
        amazonSQSClient.deleteMessage(new DeleteMessageRequest(
                message.getQueueUrl(), message.getReceiptHandle()));
        unAckMessages.remove(message.getReceiptHandle());
    }
    
    /**
     * Updates the internal data structure for the consumed but not acknowledged
     * message.
     */
    @Override
    public void notifyMessageReceived(SQSMessage message) throws JMSException {
        SQSMessageIdentifier messageIdentifier = new SQSMessageIdentifier(
                message.getQueueUrl(), message.getReceiptHandle(), message.getSQSMessageId());
        unAckMessages.put(message.getReceiptHandle(), messageIdentifier);
    }
    
    /**
     * Returns the list of all consumed but not acknowledged messages.
     */
    @Override
    public List<SQSMessageIdentifier> getUnAckMessages() {
        return new ArrayList<SQSMessageIdentifier>(unAckMessages.values());
    }
    
    /**
     * Clears the list of not acknowledged messages.
     */
    @Override
    public void forgetUnAckMessages() {
        unAckMessages.clear();
    }
   
}
