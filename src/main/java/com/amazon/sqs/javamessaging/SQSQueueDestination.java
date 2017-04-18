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
package com.amazon.sqs.javamessaging;

import javax.jms.Destination;
import javax.jms.JMSException;

import javax.jms.Queue;

/**
 * A SQSQueueDestination object encapsulates a queue name and SQS specific queue
 * URL. This is the way a client specifies the identity of a queue to JMS API
 * methods.
 */
public class SQSQueueDestination implements Destination, Queue {
    
    private final String queueName;
    
    private final String queueUrl;
    
    public SQSQueueDestination(String queueName, String queueUrl) {
        this.queueName = queueName;
        this.queueUrl = queueUrl;
    }
    
    /**
     * Returns the name of this queue. 
     * 
     * @return queueName
     */
    @Override
    public String getQueueName() throws JMSException {
        return this.queueName;
    }
    
    /**
     * Returns the queueUrl of this queue. 
     * 
     * @return queueUrl
     */
    public String getQueueUrl() {
        return queueUrl;
    }

    @Override
    public String toString() {
        return "SQSDestination [queueName=" + queueName + ", queueUrl=" + queueUrl + "]";
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((queueName == null) ? 0 : queueName.hashCode());
        result = prime * result + ((queueUrl == null) ? 0 : queueUrl.hashCode());
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
        SQSQueueDestination other = (SQSQueueDestination) obj;
        if (queueName == null) {
            if (other.queueName != null)
                return false;
        } else if (!queueName.equals(other.queueName))
            return false;
        if (queueUrl == null) {
            if (other.queueUrl != null)
                return false;
        } else if (!queueUrl.equals(other.queueUrl))
            return false;
        return true;
    }
}
