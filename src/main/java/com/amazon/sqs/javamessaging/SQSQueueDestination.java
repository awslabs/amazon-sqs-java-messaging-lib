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
package com.amazon.sqs.javamessaging;

import jakarta.jms.Destination;
import jakarta.jms.Queue;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A SQSQueueDestination object encapsulates a queue name and SQS specific queue
 * URL. This is the way a client specifies the identity of a queue to JMS API
 * methods.
 */
@Getter
@ToString(exclude = {"isFifo"})
@EqualsAndHashCode
public class SQSQueueDestination implements Destination, Queue {
    
    private final String queueName;

    private final String queueUrl;

    private final boolean isFifo;

    SQSQueueDestination(String queueName, String queueUrl) {
        this.queueName = queueName;
        this.queueUrl = queueUrl;
        this.isFifo = this.queueName.endsWith(".fifo");
    }
}
