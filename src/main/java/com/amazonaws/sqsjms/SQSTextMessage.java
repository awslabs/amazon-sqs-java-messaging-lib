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

import javax.jms.JMSException;
import javax.jms.TextMessage;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.sqsjms.acknowledge.Acknowledger;

public class SQSTextMessage extends SQSMessage implements TextMessage {

    /**
     * Text of the message. Assume this is safe from SQS invalid characters.
     */
    private String text;

    /**
     * Convert received SQSMessage into TextMessage.
     */
    SQSTextMessage(Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException{
        super(acknowledger, queueUrl, sqsMessage);
        this.text = sqsMessage.getBody();
    }
    
    /**
     * Create new empty TextMessage to send.
     */
    SQSTextMessage() throws JMSException {
        super();
    }

    /**
     * Create new TextMessage with payload to send.
     */
    SQSTextMessage(String payload) throws JMSException {
        super();
        this.text = payload;
    }

    @Override
    public void setText(String string) throws JMSException {
        checkBodyWritePermissions();
        this.text = string;
    }

    @Override
    public String getText() throws JMSException {
        return text;
    }

    @Override
    public void clearBody() throws JMSException {
        text = null;
        setBodyWritePermissions(true);
    }
}
