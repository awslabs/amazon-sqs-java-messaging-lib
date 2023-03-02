/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.sqs.javamessaging.acknowledge.BulkSQSOperation;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Test the BulkSQSOperation class
 */
public class BulkSQSOperationTest {

    private static final String QUEUE_URL = "queueUrl";
    private static final String MESSAGE_ID_PREFIX = "sqsMessageId";
    private static final String RECEIPT_HANDLE_PREFIX = "receiptHandle";

    private BulkSQSOperation bulkAction;

    @BeforeEach
    public void before() {
        bulkAction = spy(new BulkSQSOperation() {
            @Override
            public void action(String queueUrl, List<String> receiptHandles) {
            }
        });
    }

    /**
     * Test illegal index value cases
     */
    @Test
    public void testBulkActionIllegalIndexValue() {
        List<SQSMessageIdentifier> messageIdentifierList = List.of(
                new SQSMessageIdentifier(QUEUE_URL, "receiptHandle1", "sqsMessageId1"),
                new SQSMessageIdentifier(QUEUE_URL, "receiptHandle2", "sqsMessageId2"),
                new SQSMessageIdentifier(QUEUE_URL, "receiptHandle3", "sqsMessageId3"));

        int negativeSize = -10;
        assertThatThrownBy(() -> bulkAction.bulkAction(messageIdentifierList, negativeSize))
                .isInstanceOf(AssertionError.class);

        assertThatThrownBy(() -> bulkAction.bulkAction(messageIdentifierList, 0))
                .isInstanceOf(AssertionError.class);

        assertThatThrownBy(() -> bulkAction.bulkAction(messageIdentifierList, messageIdentifierList.size() + 1))
                .isInstanceOf(AssertionError.class);
    }

    /**
     * Test message flushed if number of message is below max batch size
     */
    @Test
    public void testBulkActionBelowBatchSize() throws Exception {
        List<SQSMessageIdentifier> messageIdentifierList = new ArrayList<>();

        int numMessagesFromQueue = (SQSMessagingClientConstants.MAX_BATCH - 2) / 2;

        // Create message from the first queue
        int i = 0;
        List<String> receiptHandles1 = new ArrayList<>();
        for (; i < numMessagesFromQueue; ++i) {
            messageIdentifierList.add(new SQSMessageIdentifier(QUEUE_URL + 1,
                    RECEIPT_HANDLE_PREFIX + i, MESSAGE_ID_PREFIX + i));
            receiptHandles1.add(RECEIPT_HANDLE_PREFIX + i);
        }

        // Create message from the second queue
        List<String> receiptHandles2 = new ArrayList<>();
        for (; i < numMessagesFromQueue * 2; ++i) {
            messageIdentifierList.add(new SQSMessageIdentifier(QUEUE_URL + 2,
                    RECEIPT_HANDLE_PREFIX + i, MESSAGE_ID_PREFIX + i));
            receiptHandles2.add(RECEIPT_HANDLE_PREFIX + i);
        }

        bulkAction.bulkAction(messageIdentifierList, messageIdentifierList.size());

        verify(bulkAction).action(QUEUE_URL + 1, receiptHandles1);
        verify(bulkAction).action(QUEUE_URL + 2, receiptHandles2);
    }


    /**
     * Test message are send if number of message from a single queue is above max batch size
     */
    @Test
    public void testBulkActionAboveBatchSize() throws Exception {
        List<SQSMessageIdentifier> messageIdentifierList = new ArrayList<>();

        int numMessagesFromQueue = SQSMessagingClientConstants.MAX_BATCH * 2 + 3;

        // Create messages from the first batch
        int i = 0;
        List<String> firstBatchReceiptHandles = new ArrayList<>();
        for (; i < SQSMessagingClientConstants.MAX_BATCH; ++i) {
            messageIdentifierList.add(new SQSMessageIdentifier(QUEUE_URL + 1,
                    RECEIPT_HANDLE_PREFIX + i, MESSAGE_ID_PREFIX + i));
            firstBatchReceiptHandles.add(RECEIPT_HANDLE_PREFIX + i);
        }

        // Create messages from the second batch
        List<String> secondBatchReceiptHandles = new ArrayList<>();
        for (; i < SQSMessagingClientConstants.MAX_BATCH * 2; ++i) {
            messageIdentifierList.add(new SQSMessageIdentifier(QUEUE_URL + 1,
                    RECEIPT_HANDLE_PREFIX + i, MESSAGE_ID_PREFIX + i));
            secondBatchReceiptHandles.add(RECEIPT_HANDLE_PREFIX + i);
        }

        // Create messages from the third batch
        List<String> thirdBatchReceiptHandles = new ArrayList<>();
        for (; i < numMessagesFromQueue; ++i) {
            messageIdentifierList.add(new SQSMessageIdentifier(QUEUE_URL + 1,
                    RECEIPT_HANDLE_PREFIX + i, MESSAGE_ID_PREFIX + i));
            thirdBatchReceiptHandles.add(RECEIPT_HANDLE_PREFIX + i);
        }

        // Create messages from a different queue
        List<String> receiptHandles2 = new ArrayList<>();
        for (i = 0; i < SQSMessagingClientConstants.MAX_BATCH / 2; ++i) {
            messageIdentifierList.add(new SQSMessageIdentifier(QUEUE_URL + 2,
                    RECEIPT_HANDLE_PREFIX + i, MESSAGE_ID_PREFIX + i));
            receiptHandles2.add(RECEIPT_HANDLE_PREFIX + i);
        }

        final List<List<String>> receiptHandlesList = new ArrayList<>();
        final List<String> queueUrlList = new ArrayList<>();
        bulkAction = new BulkSQSOperation() {
            @Override
            public void action(String queueUrl, List<String> receiptHandles) {
                receiptHandlesList.add(new ArrayList<>(receiptHandles));
                queueUrlList.add(queueUrl);
            }
        };

        bulkAction.bulkAction(messageIdentifierList, messageIdentifierList.size());

        assertEquals(firstBatchReceiptHandles, receiptHandlesList.get(0));
        assertEquals(QUEUE_URL + 1, queueUrlList.get(0));

        assertEquals(secondBatchReceiptHandles, receiptHandlesList.get(1));
        assertEquals(QUEUE_URL + 1, queueUrlList.get(1));

        assertEquals(thirdBatchReceiptHandles, receiptHandlesList.get(2));
        assertEquals(QUEUE_URL + 1, queueUrlList.get(2));

        assertEquals(receiptHandles2, receiptHandlesList.get(3));
        assertEquals(QUEUE_URL + 2, queueUrlList.get(3));
    }
}
