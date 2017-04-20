/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageIdentifier;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;


/**
 * Test the RangedAcknowledger class
 */
public class RangedAcknowledgerTest extends AcknowledgerCommon {

    @Before
    public void setupRanded() throws JMSException {
        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);
        acknowledger = AcknowledgeMode.ACK_RANGE.createAcknowledger(amazonSQSClient, mock(SQSSession.class));
    }

    /**
     * Test forget un-acknowledge messages
     */
    @Test
    public void testForget() throws JMSException {
        int populateMessageSize = 33;
        populateMessage(populateMessageSize);

        acknowledger.forgetUnAckMessages();
        Assert.assertEquals(0, acknowledger.getUnAckMessages().size());
    }

    /**
     * Test acknowledge all un-acknowledge messages
     */
    @Test
    public void testFullAck() throws JMSException {
        int populateMessageSize = 34;
        populateMessage(populateMessageSize);
        int ackMessage = 33;
        
        testAcknowledge(populateMessageSize, ackMessage);
        
        Assert.assertEquals(0, acknowledger.getUnAckMessages().size());
    }

    /**
     * Test acknowledge 25 first un-acknowledge messages
     */
    @Test
    public void testOneAck() throws JMSException {
        int populateMessageSize = 34;
        populateMessage(populateMessageSize);
        int ackMessage = 25;

        testAcknowledge(populateMessageSize, ackMessage);

        ArgumentCaptor<DeleteMessageBatchRequest> argumentCaptor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(amazonSQSClient, times(5)).deleteMessageBatch(argumentCaptor.capture());
        
        //key is the queue url
        //value is the sequence of sizes of expected batches
        Map<String, List<Integer>> expectedCalls = new HashMap<String, List<Integer>>();
        List<Integer> queue0Calls = new ArrayList<Integer>();
        queue0Calls.add(10);
        queue0Calls.add(1);
        expectedCalls.put(baseQueueUrl + 0, queue0Calls);
        List<Integer> queue1Calls = new ArrayList<Integer>();
        queue1Calls.add(10);
        queue1Calls.add(1);
        expectedCalls.put(baseQueueUrl + 1, queue1Calls);
        List<Integer> queue2Calls = new ArrayList<Integer>();
        queue2Calls.add(4);
        expectedCalls.put(baseQueueUrl + 2, queue2Calls);
        
        for (DeleteMessageBatchRequest request : argumentCaptor.getAllValues()) {
            String queueUrl = request.getQueueUrl();
            List<Integer> expectedSequence = expectedCalls.get(queueUrl);
            assertNotNull(expectedSequence);
            assertTrue(expectedSequence.size() > 0);
            assertEquals(expectedSequence.get(0).intValue(), request.getEntries().size());
            expectedSequence.remove(0);
            if (expectedSequence.isEmpty()) {
                expectedCalls.remove(queueUrl);
            }
        }
        
        assertTrue(expectedCalls.isEmpty());
    }

    /**
     * Test two acknowledge calls
     */
    @Test
    public void testTwoAck() throws JMSException {
        int populateMessageSize = 44;
        populateMessage(populateMessageSize);
        int firstAckMessage = 10;

        testAcknowledge(populateMessageSize, firstAckMessage);

        int secondAckMessage = 20;
        testAcknowledge(populateMessageSize, secondAckMessage);
    }

    /**
     * Utility function to acknowledge the first <code>indexOfMessageToAck<code/> un-acknowledge messages
     * @param populateMessageSize current un-acknowledge messages size
     * @param indexOfMessageToAck index of message to acknowledge
     */
    public void testAcknowledge(int populateMessageSize, int indexOfMessageToAck) throws JMSException {
        SQSMessage messageToAck = populatedMessages.get(indexOfMessageToAck);
        messageToAck.acknowledge();

        for (int i = 0; i < indexOfMessageToAck; i++) {
            SQSMessage message = populatedMessages.get(i);
            SQSMessageIdentifier sqsMessageIdentifier = new SQSMessageIdentifier(message.getQueueUrl(), message.getReceiptHandle(),
                    message.getSQSMessageId());
            Assert.assertFalse(acknowledger.getUnAckMessages().contains(sqsMessageIdentifier));
        }

        Assert.assertEquals((populateMessageSize - indexOfMessageToAck - 1), acknowledger.getUnAckMessages().size());
    }
}