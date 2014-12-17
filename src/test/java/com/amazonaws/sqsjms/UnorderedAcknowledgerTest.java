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

public class UnorderedAcknowledgerTest extends AcknowledgerCommon {

    @Before
    public void setupUnordered() throws JMSException {
        //super.setup();
        amazonSQSClient = mock(AmazonSQSClientJMSWrapper.class);
        acknowledger = AcknowledgeMode.ACK_UNORDERED.createAcknowledger(amazonSQSClient, mock(SQSSession.class));
    }

    @Test
    public void testForgetUnAckMessages() throws JMSException {
        int populateMessageSize = 30;
        populateMessage(populateMessageSize);
        
        acknowledger.forgetUnAckMessages();
        Assert.assertEquals(0, acknowledger.getUnAckMessages().size());
    }

    @Test
    public void testAck() throws JMSException {
        int populateMessageSize = 37;
        populateMessage(populateMessageSize);
        for (SQSMessage message : populatedMessages) {
            message.acknowledge();
        }
        Assert.assertEquals(0, acknowledger.getUnAckMessages().size());
    }
}