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
package com.amazon.sqs.javamessaging.util;

/**
 * Simple exponential back-off strategy, that is used for re-tries on SQS
 * interactions.
 */
public class ExponentialBackoffStrategy {

    private long delayInterval;
    private long initialDelay;
    private long maxDelay;

    public ExponentialBackoffStrategy(long delayInterval, long initialDelay, long maxDelay) {
        this.delayInterval = delayInterval;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
    }
    
    /**
     * Returns the delay before the next attempt.
     * 
     * @param retriesAttempted
     * @return The delay before the next attempt.
     */
    public long delayBeforeNextRetry(int retriesAttempted) {
        if (retriesAttempted < 1) {
            return initialDelay;
        }


        if (retriesAttempted > 63) {
            return maxDelay;
        }

        long multiplier = ((long)1 << (retriesAttempted - 1));
        if (multiplier > Long.MAX_VALUE / delayInterval) {
            return maxDelay;
        }

        long delay = multiplier * delayInterval;
        delay = Math.min(delay, maxDelay);
        return delay;
    }
}
