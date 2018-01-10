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

import java.util.List;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.message.SQSMessage;

public interface Acknowledger {

    /**
     * Generic Acknowledge method. This method will delete message(s) in SQS Queue.
     * 
     * @param message
     *            message to acknowledge.
     * @throws JMSException
     */
    public void acknowledge(SQSMessage message) throws JMSException;

    /**
     * Used when receiving messages. Depending on acknowledge mode this will
     * help create list of message backlog.
     * 
     * @param message
     *            notify acknowledger message is received
     * @throws JMSException
     */
    public void notifyMessageReceived(SQSMessage message) throws JMSException;

    /**
     * Used in negative acknowledge. Gets all delivered but not acknowledged
     * messages.
     */
    public List<SQSMessageIdentifier> getUnAckMessages();

    /**
     * Deletes all not acknowledged delivered messages.
     */
    public void forgetUnAckMessages();

}