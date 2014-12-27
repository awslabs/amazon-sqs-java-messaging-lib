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
package com.amazon.sqs.javamessaging.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.jms.JMSException;

import org.junit.Test;

import com.amazon.sqs.javamessaging.message.SQSTextMessage;

/**
 * Test the SQSTextMessageTest class
 */
public class SQSTextMessageTest {

    @Test
    public void testSetText() throws JMSException {
        String expectedPayload = "testing set payload";
        SQSTextMessage sqsTextMessage = new SQSTextMessage();
        sqsTextMessage.setText(expectedPayload);
        String actualPayload = sqsTextMessage.getText();
        assertEquals(expectedPayload, actualPayload);
    }

    /**
     * Test create message with text
     */
    @Test
    public void testCreateMessageWithText() throws JMSException {
        String expectedPayload = "testing message with payload";
        SQSTextMessage sqsTextMessage = new SQSTextMessage(expectedPayload);

        String actualPayload = sqsTextMessage.getText();
        assertEquals(expectedPayload, actualPayload);
    }

    /**
     * Test create message and setting text
     */
    @Test
    public void testClearBody() throws JMSException {
        String expectedPayload = "testing set payload";
        SQSTextMessage sqsTextMessage = new SQSTextMessage();
        sqsTextMessage.setText(expectedPayload);
        assertEquals(expectedPayload, sqsTextMessage.getText());

        sqsTextMessage.clearBody();
        assertNull(sqsTextMessage.getText());
    }
}