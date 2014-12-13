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

import javax.jms.JMSException;

import com.amazonaws.sqsjms.AmazonSQSClientJMSWrapper;
import com.amazonaws.sqsjms.SQSSession;

/**
 * This enum specifies the different possible modes of acknowledgment under
 * CLIENT_ACKNOWLEDGE Mode.
 * ACK_AUTO In this mode, every time the user receives a message it is acknowledged.
 * This is implemented by consumer and does not need any further processing.
 * In ACK_UNORDERED mode, messages can be acknowledged in any order.
 * In ACK_RANGE mode, all messages received before that message including that
 * one are acknowledged.
 */
public enum AcknowledgeMode {
    ACK_AUTO, ACK_UNORDERED, ACK_RANGE;

    private int originalAcknowledgeMode;

    public AcknowledgeMode withOriginalAcknowledgeMode(int originalAcknowledgeMode) {
        this.originalAcknowledgeMode = originalAcknowledgeMode;
        return this;
    }
    
    public int getOriginalAcknowledgeMode() {
        return originalAcknowledgeMode;
    }

    public Acknowledger createAcknowledger(AmazonSQSClientJMSWrapper amazonSQSClient, SQSSession parentSQSSession) throws JMSException {
        switch (this) {
        case ACK_AUTO:
            return new AutoAcknowledger(amazonSQSClient, parentSQSSession);
        case ACK_RANGE:
            return new RangedAcknowledger(amazonSQSClient, parentSQSSession);
        case ACK_UNORDERED:
            return new UnorderedAcknowledger(amazonSQSClient, parentSQSSession);
        default:
            throw new JMSException(this + " - AcknowledgeMode does not exist");
        }
    }
}
