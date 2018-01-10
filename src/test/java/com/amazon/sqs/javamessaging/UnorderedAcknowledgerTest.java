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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import javax.jms.JMSException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Test the UnorderedAcknowledgerTest class
 */
public class UnorderedAcknowledgerTest extends AcknowledgerCommon {

    @Before
    public void setupUnordered() throws JMSException {
        amazonSQSClient = mock(AmazonSQSMessagingClientWrapper.class);
        acknowledger = AcknowledgeMode.ACK_UNORDERED.createAcknowledger(amazonSQSClient, mock(SQSSession.class));
    }

    /**
     * Test forgetUnAckMessages
     */
    @Test
    public void testForgetUnAckMessages() throws JMSException {
        int populateMessageSize = 30;
        populateMessage(populateMessageSize);
        
        acknowledger.forgetUnAckMessages();
        assertEquals(0, acknowledger.getUnAckMessages().size());
    }

    /**
     * Test acknowledge does not impact messages that were not specifically acknowledge
     */
    @Test
    public void testAcknowledge() throws JMSException {
        int populateMessageSize = 37;
        populateMessage(populateMessageSize);
        int counter = 0;

        List<SQSMessage> populatedMessagesCopy =  new ArrayList<SQSMessage>(populatedMessages);
        while (!populatedMessagesCopy.isEmpty()) {

            int rand = new Random().nextInt(populatedMessagesCopy.size());
            SQSMessage message = populatedMessagesCopy.remove(rand);
            message.acknowledge();
            assertEquals(populateMessageSize - (++counter), acknowledger.getUnAckMessages().size());
        }
        assertEquals(0, acknowledger.getUnAckMessages().size());

        ArgumentCaptor<DeleteMessageRequest> argumentCaptor = ArgumentCaptor.forClass(DeleteMessageRequest.class);
        verify(amazonSQSClient, times(populateMessageSize)).deleteMessage(argumentCaptor.capture());

        for (SQSMessage msg : populatedMessages) {
            DeleteMessageRequest deleteRequest = new DeleteMessageRequest()
                    .withQueueUrl(msg.getQueueUrl())
                    .withReceiptHandle(msg.getReceiptHandle());
            assertTrue(argumentCaptor.getAllValues().contains(deleteRequest));
        }
    }
}