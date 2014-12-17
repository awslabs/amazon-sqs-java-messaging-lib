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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.util.Base64;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.sqsjms.SQSJMSClientConstants;

public class SQSBytesMessageTest {
    
    private SQSSession mockSQSSession;
    
    private final static Map<String, String> MESSAGE_SYSTEM_ATTRIBUTES;
    static {
        MESSAGE_SYSTEM_ATTRIBUTES = new HashMap<String,String>();
        MESSAGE_SYSTEM_ATTRIBUTES.put(SQSJMSClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");
    }

    @Before 
    public void setUp() {
        mockSQSSession = mock(SQSSession.class);
    }

    @Test
    public void testReadWrite() throws JMSException {
        when(mockSQSSession.createBytesMessage()).thenReturn(new SQSBytesMessage());
        SQSBytesMessage msg = (SQSBytesMessage) mockSQSSession.createBytesMessage();
        
        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        byte byteData = 'a';
        short shortVal = 123;

        msg.writeBytes(byteArray);
        msg.writeChar('b');
        msg.writeBoolean(true);
        msg.writeInt(100);
        msg.writeDouble(2.1768);
        msg.writeFloat(3.1457f);
        msg.writeLong(1290772974281L);
        msg.writeShort(shortVal);
        msg.writeByte(byteData);
        msg.writeUTF("UTF-String");
        msg.writeObject("test");
        
        msg.reset();

        int length = (int) msg.getBodyLength();
        byte[] body = new byte[length];
        int i = msg.readBytes(body);
        if (i == -1) {
            fail("failed to readByte");
        }

        Message message = new Message();
        message.setBody(Base64.encodeAsString(body));
        
        message.setAttributes(MESSAGE_SYSTEM_ATTRIBUTES);
        SQSBytesMessage receivedByteMsg = new SQSBytesMessage(null, "", message);
        byte[] byteArray1 = new byte[4];
        receivedByteMsg.readBytes(byteArray1);

        assertEquals(1, byteArray1[0]);
        assertEquals(0, byteArray1[1]);
        assertEquals('a', byteArray1[2]);
        assertEquals(65, byteArray1[3]);
        assertEquals('b', receivedByteMsg.readChar());
        assertTrue(receivedByteMsg.readBoolean());
        assertEquals(100, receivedByteMsg.readInt());
        assertEquals(2.1768, receivedByteMsg.readDouble(), 0);
        assertEquals(3.1457f, receivedByteMsg.readFloat(), 0);
        assertEquals(1290772974281L, receivedByteMsg.readLong());
        assertEquals(shortVal, receivedByteMsg.readShort());
        assertEquals(97, receivedByteMsg.readByte());
        assertEquals("UTF-String", receivedByteMsg.readUTF());
        assertEquals("test", receivedByteMsg.readUTF());
    }
    
    @Test(expected = MessageNotWriteableException.class)
    public void testNotWriteable() throws JMSException {
        when(mockSQSSession.createBytesMessage()).thenReturn(new SQSBytesMessage());
        SQSBytesMessage msg = (SQSBytesMessage) mockSQSSession.createBytesMessage(); 

        byte[] byteArray = new byte[] { 'a', 0, 34, 65 };
        msg.writeBytes(byteArray);
        msg.reset();
        assertEquals('a', msg.readByte());
        msg.writeInt(10);
    }

    @Test(expected = MessageNotReadableException.class)
    public void testReadable() throws JMSException {
        when(mockSQSSession.createBytesMessage()).thenReturn(new SQSBytesMessage());
        SQSBytesMessage msg = (SQSBytesMessage) mockSQSSession.createBytesMessage(); 
        
        byte[] byteArray = new byte[] { 'a', 0, 34, 65 };
        msg.writeBytes(byteArray);
        
        msg.readInt();
    }
}