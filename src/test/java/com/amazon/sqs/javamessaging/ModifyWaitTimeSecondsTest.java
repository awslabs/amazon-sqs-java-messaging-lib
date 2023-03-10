/*
 * Copyright 2010-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ModifyWaitTimeSecondsTest {
    @DisplayName("Should be able to modify SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS via Reflection")
    @Test
    public void waitTimeSecondsShouldBeModifiableViaReflection() throws NoSuchFieldException, IllegalAccessException {
        Field wait_time_seconds = SQSMessageConsumerPrefetch.class.getDeclaredField("WAIT_TIME_SECONDS");
        wait_time_seconds.setAccessible(true);
        wait_time_seconds.setInt(null,5);
        assertEquals(5, SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS);
    }
}