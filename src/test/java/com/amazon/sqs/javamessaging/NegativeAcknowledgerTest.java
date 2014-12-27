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
import com.amazon.sqs.javamessaging.PrefetchManager;
import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.NegativeAcknowledger;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;

import javax.jms.JMSException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test the NegativeAcknowledger class
 */
public class NegativeAcknowledgerTest extends AcknowledgerCommon {

    private static final String QUEUE_URL = "queueUrl";

    private NegativeAcknowledger negativeAcknowledger;

    @Before
    public void setupRanded() throws JMSException {
        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);
        negativeAcknowledger = spy(new NegativeAcknowledger(amazonSQSClient));
    }

    /**
     * Test NegativeAcknowledger bulk action
     */
    @Test
    public void testNackBulkAction() throws JMSException {

        /*
         * Set up the message queue
         */
        List<String> receiptHandles = new ArrayList<String>();
        receiptHandles.add("r0");
        receiptHandles.add("r1");
        receiptHandles.add("r2");

        ArrayDeque<SQSMessageConsumerPrefetch.MessageManager> messageQueue = addSQSMessageToQueue(3);

        /*
         * Nack the messages in bulk actions
         */
        negativeAcknowledger.bulkAction(messageQueue, QUEUE_URL);

        /*
         * Verify results
         */
        verify(negativeAcknowledger).action(QUEUE_URL, receiptHandles);
    }

    /**
     * Test NegativeAcknowledger bulk action with a large batch
     */
    @Test
    public void testNackBulkActionLargeBatch() throws JMSException {

        /*
         * Set up the message queue
         */
        ArrayDeque<SQSMessageConsumerPrefetch.MessageManager> messageQueue = addSQSMessageToQueue(13);

        /*
         * Nack the messages in bulk actions
         */
        negativeAcknowledger.bulkAction(messageQueue, QUEUE_URL);

        /*
         * Verify results
         */
        verify(negativeAcknowledger, times(2)).action(eq(QUEUE_URL), anyList());
    }

    /**
     * Test NegativeAcknowledger action
     */
    @Test
    public void testAction() throws JMSException {


        List<String> receiptHandles = new ArrayList<String>();
        receiptHandles.add("r0");
        receiptHandles.add("r1");
        receiptHandles.add("r2");

        negativeAcknowledger.action(QUEUE_URL, receiptHandles);

        ArgumentCaptor<ChangeMessageVisibilityBatchRequest> argumentCaptor =
                ArgumentCaptor.forClass(ChangeMessageVisibilityBatchRequest.class);
        verify(amazonSQSClient).changeMessageVisibilityBatch(argumentCaptor.capture());

        assertEquals(1, argumentCaptor.getAllValues().size());

        assertEquals(QUEUE_URL, argumentCaptor.getAllValues().get(0).getQueueUrl());
        List<ChangeMessageVisibilityBatchRequestEntry> captureList =  argumentCaptor.getAllValues().get(0).getEntries();
        assertEquals(receiptHandles.size(), captureList.size());

        for (ChangeMessageVisibilityBatchRequestEntry item : captureList) {
            receiptHandles.contains(item.getReceiptHandle());
        }
    }

    /**
     * Test NegativeAcknowledger action  withe empty receipt handles
     */
    @Test
    public void testActionEmptyReceiptHandles() throws JMSException {

        negativeAcknowledger.action(QUEUE_URL, null);

        negativeAcknowledger.action(QUEUE_URL, Collections.EMPTY_LIST);

        verify(amazonSQSClient, never()).changeMessageVisibilityBatch(any(ChangeMessageVisibilityBatchRequest.class));
    }

    /**
     * Utility function
     */
    private ArrayDeque<SQSMessageConsumerPrefetch.MessageManager> addSQSMessageToQueue(int count) {

        ArrayDeque<SQSMessageConsumerPrefetch.MessageManager> messageQueue =
                new ArrayDeque<SQSMessageConsumerPrefetch.MessageManager>();

        PrefetchManager prefetchManager = mock(PrefetchManager.class);
        for (int i = 0; i < count; ++i) {
            SQSMessage msg = mock(SQSMessage.class);
            when(msg.getReceiptHandle()).thenReturn("r" + i);
            messageQueue.add(new SQSMessageConsumerPrefetch.MessageManager(prefetchManager, msg));
        }

        return messageQueue;
    }
}
