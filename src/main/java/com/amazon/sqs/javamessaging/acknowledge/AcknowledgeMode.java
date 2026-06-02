/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClient;
import com.amazon.sqs.javamessaging.SQSSession;

/**
 * <p>
 * Specifies the different possible modes of acknowledgment:
 * <ul>
 * <li>In ACK_AUTO mode, every time the user receives a message, it is
 * acknowledged automatically. This is implemented by consumer and does not need
 * any further processing.</li>
 * <li>In ACK_RANGE mode, all messages received before that message including
 * that one are acknowledged.</li>
 * <li>In ACK_UNORDERED mode, messages can be acknowledged in any order one at a
 * time. Acknowledging the consumed message does not affect other message
 * acknowledges.</li>
 * </ul>
 */
public enum AcknowledgeMode {
    ACK_AUTO, ACK_UNORDERED, ACK_RANGE;

    private int originalAcknowledgeMode;

    /**
     * Sets the acknowledgment mode.
     */
    public AcknowledgeMode withOriginalAcknowledgeMode(int originalAcknowledgeMode) {
        this.originalAcknowledgeMode = originalAcknowledgeMode;
        return this;
    }

    /**
     * Returns the acknowledgment mode.
     */
    public int getOriginalAcknowledgeMode() {
        return originalAcknowledgeMode;
    }

    /**
     * Creates the acknowledger associated with the session, which will be used
     * to acknowledge the delivered messages on consumers of the session.
     * 
     * @param amazonSQSClient
     *            the SQS client to delete messages
     * @param parentSQSSession
     *            the associated session for the acknowledger
     */
    public Acknowledger createAcknowledger(AmazonSQSMessagingClient amazonSQSClient, SQSSession parentSQSSession) {
        return switch (this) {
            case ACK_AUTO -> new AutoAcknowledger(amazonSQSClient, parentSQSSession);
            case ACK_RANGE -> new RangedAcknowledger(amazonSQSClient, parentSQSSession);
            case ACK_UNORDERED -> new UnorderedAcknowledger(amazonSQSClient, parentSQSSession);
        };
    }
}
