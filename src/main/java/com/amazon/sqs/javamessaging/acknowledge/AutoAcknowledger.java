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

import java.util.Collections;
import java.util.List;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

/**
 * Used by session to automatically acknowledge a client's receipt of a message
 * either when the session has successfully returned from a call to receive or
 * when the message listener the session has called to process the message
 * successfully returns.
 */
public class AutoAcknowledger implements Acknowledger {

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;
    private final SQSSession session;

    public AutoAcknowledger(AmazonSQSMessagingClientWrapper amazonSQSClient, SQSSession session) {
        this.amazonSQSClient = amazonSQSClient;
        this.session = session;
    }
    
    /** Acknowledges the consumed message via calling <code>deleteMessage</code> */
    @Override
    public void acknowledge(SQSMessage message) throws JMSException {
        session.checkClosed();
        amazonSQSClient.deleteMessage(new DeleteMessageRequest(
                message.getQueueUrl(), message.getReceiptHandle()));
    }
    
    /**
     * When notify message is received, it will acknowledge the message.
     */
    @Override
    public void notifyMessageReceived(SQSMessage message) throws JMSException {
        acknowledge(message);
    }
    
    /**
     * AutoAcknowledge doesn't need to do anything in this method. Return an
     * empty list.
     */
    @Override
    public List<SQSMessageIdentifier> getUnAckMessages() {
        return Collections.<SQSMessageIdentifier>emptyList(); 
    }
    
    /** AutoAcknowledge doesn't need to do anything in this method. */
    @Override
    public void forgetUnAckMessages() {
    }
 
}
