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

import java.util.Collections;
import java.util.List;

import javax.jms.JMSException;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.sqsjms.AmazonSQSClientJMSWrapper;
import com.amazonaws.sqsjms.SQSMessage;
import com.amazonaws.sqsjms.SQSSession;

public class AutoAcknowledger implements Acknowledger {

    private final AmazonSQSClientJMSWrapper amazonSQSClient;
    private final SQSSession session;

    public AutoAcknowledger(AmazonSQSClientJMSWrapper amazonSQSClient, SQSSession session) {
        this.amazonSQSClient = amazonSQSClient;
        this.session = session;
    }

    @Override
    public void acknowledge(SQSMessage message) throws JMSException {
        session.checkClosed();
        amazonSQSClient.deleteMessage(new DeleteMessageRequest(
                message.getQueueUrl(), message.getReceiptHandle()));
    }

    @Override
    public void notifyMessageReceived(SQSMessage message) throws JMSException {
        // When notify message is received AutoAck mode will ack the message. 
        // Ack the message while close is still running.
        acknowledge(message);
    }

    @Override
    public List<SQSMessageIdentifier> getUnAckMessages() {
        // AutoAcknowledge doesn't need to do anything in this method.
        // Return an empty list so we don't have to check for null.
        return Collections.<SQSMessageIdentifier>emptyList(); 
    }

    @Override
    public void forgetUnAckMessages() {
        // AutoAcknowledge doesn't need to do anything in this method.
    }
 
}
