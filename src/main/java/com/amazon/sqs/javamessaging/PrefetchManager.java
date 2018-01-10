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

/**
 * This interface is helper to notify when the prefetchThread should be resuming
 * messages.
 */
public interface PrefetchManager {

    /**
     * Notify the prefetchThread that the message is dispatched from
     * messageQueue when user calls for receive or message listener onMessage is
     * called.
     */
    public void messageDispatched();

    /**
     * This is used to determine the state of the consumer, when the message
     * listener scheduler is processing the messages.
     * 
     * @return The message consumer, which owns the prefetchThread
     */
    public SQSMessageConsumer getMessageConsumer();
}
