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

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.AutoAcknowledger;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

import javax.jms.IllegalStateException;
import java.util.Collections;
import org.junit.Test;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the AutoAcknowledger class
 */
public class AutoAcknowledgerTest {

    private static final String QUEUE_URL = "QueueUrl";
    private static final String RECEIPT_HANDLE = "ReceiptHandle";

    private AutoAcknowledger acknowledger;
    private AmazonSQSMessagingClientWrapper amazonSQSClient;
    private SQSSession session;

    @Before
    public void before() throws Exception {
        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);
        session = mock(SQSSession.class);
        acknowledger = (AutoAcknowledger) spy(AcknowledgeMode.ACK_AUTO.createAcknowledger(amazonSQSClient, session));
    }

    /**
     * Test acknowledging message with auto acknowledger
     */
    @Test
    public void testAcknowledge() throws Exception {

        /*
         * Set up message mock
         */
        SQSMessage message = mock(SQSMessage.class);
        when(message.getQueueUrl())
                .thenReturn(QUEUE_URL);
        when(message.getReceiptHandle())
                .thenReturn(RECEIPT_HANDLE);

        /*
         * Use the acknowledger to ack the message
         */
        acknowledger.acknowledge(message);

        /*
         * Verify results
         */
        ArgumentCaptor<DeleteMessageRequest> argumentCaptor = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(amazonSQSClient).deleteMessage(argumentCaptor.capture());
        assertEquals(1, argumentCaptor.getAllValues().size());

        DeleteMessageRequest input = argumentCaptor.getAllValues().get(0);
        assertEquals(QUEUE_URL, input.getQueueUrl());
        assertEquals(RECEIPT_HANDLE, input.getReceiptHandle());
    }

    /**
     * Test attempt to acknowledge when the session is already closed
     */
    @Test
    public void testAcknowledgeWhenSessionClosed() throws Exception {

        /*
         * Set up mocks
         */
        doThrow(new IllegalStateException("ise"))
                .when(session).checkClosed();

        SQSMessage message = mock(SQSMessage.class);
        when(message.getQueueUrl())
                .thenReturn(QUEUE_URL);
        when(message.getReceiptHandle())
                .thenReturn(RECEIPT_HANDLE);

        /*
         * Use the acknowledger to ack the message
         */
        try {
            acknowledger.acknowledge(message);
            fail();
        } catch (IllegalStateException ise) {
            // Expected exception
        }
    }

    /**
     * Test notify message received
     */
    @Test
    public void testNotifyMessageReceived() throws Exception {

        SQSMessage message = mock(SQSMessage.class);
        acknowledger.notifyMessageReceived(message);
        verify(acknowledger).acknowledge(message);
    }

    /**
     * Test get UnAckMessages
     */
    @Test
    public void testGetUnAckMessages() throws Exception {

        assertEquals(Collections.<SQSMessageIdentifier>emptyList(), acknowledger.getUnAckMessages());
    }
}
