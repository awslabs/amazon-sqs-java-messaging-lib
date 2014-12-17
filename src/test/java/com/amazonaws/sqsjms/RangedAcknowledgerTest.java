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

import javax.jms.JMSException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.sqsjms.AcknowledgerCommon;
import com.amazonaws.sqsjms.AmazonSQSClientJMSWrapper;

import com.amazonaws.sqsjms.SQSMessage;
import com.amazonaws.sqsjms.SQSSession;
import com.amazonaws.sqsjms.acknowledge.AcknowledgeMode;
import com.amazonaws.sqsjms.acknowledge.SQSMessageIdentifier;

public class RangedAcknowledgerTest extends AcknowledgerCommon {

    @Before
    public void setupRanded() throws JMSException {
        amazonSQSClient = mock(AmazonSQSClientJMSWrapper.class);
        acknowledger = AcknowledgeMode.ACK_RANGE.createAcknowledger(amazonSQSClient, mock(SQSSession.class));
    }

    @Test
    public void testForget() throws JMSException {
        int populateMessageSize = 33;
        populateMessage(populateMessageSize);

        acknowledger.forgetUnAckMessages();
        Assert.assertEquals(0, acknowledger.getUnAckMessages().size());
    }
    
    @Test
    public void testFullAck() throws JMSException {
        int populateMessageSize = 34;
        populateMessage(populateMessageSize);
        int ackMessage = 33;
        
        testAcknowledge(populateMessageSize, ackMessage);
        
        Assert.assertEquals(0, acknowledger.getUnAckMessages().size());
    }

    @Test
    public void testOneAck() throws JMSException {
        int populateMessageSize = 34;
        populateMessage(populateMessageSize);
        int ackMessage = 25;

        testAcknowledge(populateMessageSize, ackMessage);
    }

    @Test
    public void testTwoAck() throws JMSException {
        int populateMessageSize = 44;
        populateMessage(populateMessageSize);
        int firstAckMessage = 10;

        testAcknowledge(populateMessageSize, firstAckMessage);

        int secondAckMessage = 20;
        testAcknowledge(populateMessageSize, secondAckMessage);
    }

    @Test
    public void testMessageDoesNotExist() throws JMSException {
        int populateMessageSize = 32;
        populateMessage(populateMessageSize);
        int messagesToAck = 20;

        testAcknowledge(populateMessageSize, messagesToAck);
        
        SQSMessage message = populatedMessages.get(10);
        message.acknowledge();

    }

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