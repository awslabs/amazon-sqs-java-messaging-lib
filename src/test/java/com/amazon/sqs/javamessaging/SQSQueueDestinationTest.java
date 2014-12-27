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

import org.junit.Test;

import com.amazon.sqs.javamessaging.SQSQueueDestination;

import static org.junit.Assert.assertEquals;

/**
 * Test the SQSDestinationTest class
 */
public class SQSQueueDestinationTest {

    public static final String QUEUE_NAME = "QueueName";
    public static final String QUEUE_URL = "QueueUrl";

    /**
     * Test SQSDestination property
     */
    @Test
    public void testProperty() throws Exception {
        SQSQueueDestination destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        assertEquals(QUEUE_NAME, destination.getQueueName());
        assertEquals(QUEUE_URL, destination.getQueueUrl());
    }

} 
