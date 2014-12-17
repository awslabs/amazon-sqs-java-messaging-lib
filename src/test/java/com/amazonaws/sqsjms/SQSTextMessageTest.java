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
package com.amazonaws.sqsjms;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.jms.JMSException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class SQSTextMessageTest {

    private SQSSession mockSQSSession;

    @Before
    public void setup() throws JMSException {
        mockSQSSession = mock(SQSSession.class);
    }

    @Test
    public void testSetText() throws JMSException {
        String expectedPayload = "testing set payload";
        when(mockSQSSession.createTextMessage()).thenReturn(new SQSTextMessage());
        SQSTextMessage sqsTextMessage = (SQSTextMessage) mockSQSSession.createTextMessage();
        sqsTextMessage.setText(expectedPayload);
        String actualPayload = sqsTextMessage.getText();
        Assert.assertEquals(expectedPayload, actualPayload);
    }

    @Test
    public void testCreateMessageWithText() throws JMSException {
        String expectedPayload = "testing message with payload";
        when(mockSQSSession.createTextMessage(expectedPayload)).thenReturn(new SQSTextMessage(expectedPayload));
        SQSTextMessage sqsTextMessage = (SQSTextMessage) mockSQSSession.createTextMessage(expectedPayload);

        String actualPayload = sqsTextMessage.getText();
        Assert.assertEquals(expectedPayload, actualPayload);
    }
}