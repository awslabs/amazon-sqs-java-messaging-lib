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

import com.amazon.sqs.javamessaging.message.SQSMessage;
import jakarta.jms.JMSException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Identifies an SQS message, when (negative)acknowledging the message
 */
@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Getter
public class SQSMessageIdentifier {

    // The queueUrl where the message was sent or received from
    @ToString.Include
    @EqualsAndHashCode.Include
    private final String queueUrl;

    // The receipt handle returned after the delivery of the message from SQS
    @ToString.Include
    @EqualsAndHashCode.Include
    private final String receiptHandle;

    // The SQS message id assigned on send.
    @ToString.Include
    @EqualsAndHashCode.Include
    private final String sqsMessageId;
    
    // The group id to which the message belongs
    private String groupId;

    public SQSMessageIdentifier(String queueUrl, String receiptHandle, String sqsMessageId) {
        this(queueUrl, receiptHandle, sqsMessageId, null);
    }
    
    public SQSMessageIdentifier(String queueUrl, String receiptHandle, String sqsMessageId, String groupId) {
        this.queueUrl = queueUrl;
        this.receiptHandle = receiptHandle;
        this.sqsMessageId = sqsMessageId;
        this.groupId = groupId;
        if (this.groupId != null && this.groupId.isEmpty()) {
            this.groupId = null;
        }
    }
    
    public static SQSMessageIdentifier fromSQSMessage(SQSMessage sqsMessage) throws JMSException {
        return new SQSMessageIdentifier(sqsMessage.getQueueUrl(), sqsMessage.getReceiptHandle(), sqsMessage.getSQSMessageId(), sqsMessage.getSQSMessageGroupId());
    }
}
