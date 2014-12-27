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

import com.amazon.sqs.javamessaging.util.ExponentialBackoffStrategy;

import org.junit.Test;
import org.junit.Before;

import static org.junit.Assert.assertEquals;

/**
 * Test the ExponentialBackoffStrategy class
 */
public class ExponentialBackoffStrategyTest {

    private static final int DELAY_INTERVAL = 10;
    private static final int INITAL_DELAY = 20;
    private static final int MAX_DELAY = 300;

    /**
     * test delay with illegal value of retry attempts
     */
    @Test
    public void testDelayBeforeNextRetryNonPositiveRetryAttempt() throws Exception {

        ExponentialBackoffStrategy backoff = new ExponentialBackoffStrategy(DELAY_INTERVAL, INITAL_DELAY, MAX_DELAY);

        assertEquals(INITAL_DELAY, backoff.delayBeforeNextRetry(0));
        assertEquals(INITAL_DELAY, backoff.delayBeforeNextRetry(-10));
    }

    /**
     * test first delay result in delay initial value
     */
    @Test
    public void testDelayBeforeNextRetryFirstAttempt() throws Exception {
        ExponentialBackoffStrategy backoff = new ExponentialBackoffStrategy(DELAY_INTERVAL, INITAL_DELAY, MAX_DELAY);

        assertEquals(DELAY_INTERVAL, backoff.delayBeforeNextRetry(1));
    }

    /**
     * test delay from 2 to 1000
     */
    @Test
    public void testDelayBeforeNextRetry() throws Exception {
        ExponentialBackoffStrategy backoff = new ExponentialBackoffStrategy(DELAY_INTERVAL, INITAL_DELAY, MAX_DELAY);

        assertEquals(20, backoff.delayBeforeNextRetry(2));
        assertEquals(40, backoff.delayBeforeNextRetry(3));
        assertEquals(80, backoff.delayBeforeNextRetry(4));
        assertEquals(160, backoff.delayBeforeNextRetry(5));
        for (int i = 6; i < 1000; i++) {
            assertEquals(MAX_DELAY, backoff.delayBeforeNextRetry(i));
        }
    }
}
