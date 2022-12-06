package com.amazon.sqs.javamessaging;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

class ModifyWaitTimeSecondsTest {
    @DisplayName("Should be able to modify SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS via Reflection")
    @Test
    void WaitTimeSecondsShouldBeModifiableViaReflection() throws NoSuchFieldException, IllegalAccessException {
        Field wait_time_seconds = SQSMessageConsumerPrefetch.class.getDeclaredField("WAIT_TIME_SECONDS");
        wait_time_seconds.setAccessible(true);
        wait_time_seconds.setInt(null,5);
        assertEquals(5,SQSMessageConsumerPrefetch.WAIT_TIME_SECONDS);
    }
}