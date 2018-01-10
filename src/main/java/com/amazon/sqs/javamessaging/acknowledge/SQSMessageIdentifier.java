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

/**
 * Identifies an SQS message, when (negative)acknowledging the message
 */
public class SQSMessageIdentifier {

    // The queueUrl where the message was sent or received from
    private String queueUrl;

    // The receipt handle returned after the delivery of the message from SQS
    private String receiptHandle;

    // The SQS message id assigned on send.
    private String sqsMessageId;

    public SQSMessageIdentifier(String queueUrl, String receiptHandle, String sqsMessageId) {
        this.queueUrl = queueUrl;
        this.receiptHandle = receiptHandle;
        this.sqsMessageId = sqsMessageId;
    }

    /**
     * Returns the queueUrl where the message was sent or received from.
     * 
     * @return queueUrl
     */
    public String getQueueUrl() {
        return this.queueUrl;
    }

    /**
     * Returns the receipt handle returned after the delivery of the message
     * from SQS.
     * 
     * @return receiptHandle
     */
    public String getReceiptHandle() {
        return this.receiptHandle;
    }

    /**
     * Returns the SQS message id assigned on send.
     * 
     * @return sqsMessageId
     */
    public String getSQSMessageID() {
        return this.sqsMessageId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((queueUrl == null) ? 0 : queueUrl.hashCode());
        result = prime * result + ((receiptHandle == null) ? 0 : receiptHandle.hashCode());
        result = prime * result + ((sqsMessageId == null) ? 0 : sqsMessageId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SQSMessageIdentifier other = (SQSMessageIdentifier) obj;
        if (queueUrl == null) {
            if (other.queueUrl != null)
                return false;
        } else if (!queueUrl.equals(other.queueUrl))
            return false;
        if (receiptHandle == null) {
            if (other.receiptHandle != null)
                return false;
        } else if (!receiptHandle.equals(other.receiptHandle))
            return false;
        if (sqsMessageId == null) {
            if (other.sqsMessageId != null)
                return false;
        } else if (!sqsMessageId.equals(other.sqsMessageId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SQSMessageIdentifier [queueUrl=" + queueUrl + ", receiptHandle=" + receiptHandle +
               ", sqsMessageId=" + sqsMessageId + "]";
    }
}
