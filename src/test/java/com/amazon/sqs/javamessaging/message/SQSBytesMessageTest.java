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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.junit.Before;
import org.junit.Test;

import com.amazon.sqs.javamessaging.SQSMessagingClientConstants;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazonaws.util.Base64;

import com.amazonaws.services.sqs.model.Message;

/**
 * Test the SQSBytesMessageTest class
 */
public class SQSBytesMessageTest {
    
    private SQSSession mockSQSSession;
    
    private final static Map<String, String> MESSAGE_SYSTEM_ATTRIBUTES;
    static {
        MESSAGE_SYSTEM_ATTRIBUTES = new HashMap<String,String>();
        MESSAGE_SYSTEM_ATTRIBUTES.put(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");
    }

    @Before 
    public void setUp() {
        mockSQSSession = mock(SQSSession.class);
    }

    /**
     * Test message ability to write and read different types
     */
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

        /*
         * Validate MessageEOFException is thrown when reaching end of file
         */
        try {
            receivedByteMsg.readBoolean();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readUnsignedByte();
            fail();
        } catch (MessageEOFException exception) {
        }

        byte[] arr = new byte[10];
        assertEquals(-1, receivedByteMsg.readBytes(arr, 10));

        try {
            receivedByteMsg.readByte();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readShort();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readUnsignedShort();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readInt();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readLong();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readFloat();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readDouble();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readChar();
            fail();
        } catch (MessageEOFException exception) {
        }

        try {
            receivedByteMsg.readUTF();
            fail();
        } catch (MessageEOFException exception) {
        }
    }

    /**
     * Test byte message ability to handle io exception from the read operation
     */
    @Test
    public void testReadIOException() throws JMSException, IOException {

        /*
         * Set up mocks
         */
        Throwable ioException = new IOException();

        InputStream is = mock(InputStream.class);
        DataInputStream dis = new DataInputStream(is);

        when(is.read())
                .thenThrow(ioException);

        when(is.read(any(byte[].class), anyInt(), anyInt()))
                .thenThrow(ioException);

        SQSBytesMessage msg = spy(new SQSBytesMessage());
        doNothing()
                .when(msg).checkCanRead();

        msg.setDataIn(dis);

        try {
            msg.readBoolean();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readByte();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readUnsignedByte();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readShort();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readUnsignedShort();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readInt();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readLong();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readFloat();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readDouble();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readChar();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.readUTF();
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }
    }

    /**
     * Test byte message ability to handle io exception from the write operation
     */
    @Test
    public void testWriteIOException() throws JMSException, IOException {

        /*
         * Set up mocks
         */
        Throwable ioException = new IOException();

        OutputStream os = mock(OutputStream.class);
        DataOutputStream dos = new DataOutputStream(os);



        SQSBytesMessage msg = spy(new SQSBytesMessage());
        doNothing()
                .when(msg).checkCanRead();

        msg.setDataOut(dos);

        doThrow(ioException)
                .when(os).write(anyInt());

        doThrow(ioException)
                .when(os).write(any(byte[].class), anyInt(), anyInt());
        try {
            msg.writeBoolean(true);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeByte((byte)1);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeShort((short)1);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeInt(1);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeLong(1290772974281L);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeFloat(3.1457f);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeDouble(2.1768);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeChar('a');
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        try {
            msg.writeUTF("test");
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }
    }

    /**
     * Test write object function
     */
    @Test
    public void testWriteObject() throws JMSException, IOException {
        SQSBytesMessage msg = new SQSBytesMessage();

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        byte byteData = 'a';
        short shortVal = 123;

        msg.writeObject(byteArray);
        msg.writeObject('b');
        msg.writeObject(true);
        msg.writeObject(100);
        msg.writeObject(2.1768);
        msg.writeObject(3.1457f);
        msg.writeObject(1290772974281L);
        msg.writeObject(shortVal);
        msg.writeObject(byteData);
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
        assertEquals("test", receivedByteMsg.readUTF());

        /*
         * Check write object error cases
         */
        try {
            msg.writeObject(new HashSet<String>());
            fail();
        } catch(MessageFormatException exception) {
            // expected
        }

        /*
         * Check write object error cases
         */
        try {
            msg.writeObject(null);
            fail();
        } catch(NullPointerException exception) {
            // expected
        }
    }

    /**
     * Test clear body
     */
    @Test
    public void testClearBody() throws JMSException, IOException {

        SQSBytesMessage msg = new SQSBytesMessage();

        byte[] byteArray = new byte[] { 1, 0, 'a', 65 };
        msg.writeBytes(byteArray);

        msg.clearBody();

        byte[] readByteArray = new byte[4];

        /*
         * Verify message is in write-only mode
         */
        try {
            msg.readBytes(readByteArray);
        } catch(MessageNotReadableException exception) {
            assertEquals("Message is not readable", exception.getMessage());
        }

        msg.writeBytes(byteArray);
    }

    /**
     * Test after reset the message is read only mode
     */
    @Test(expected = MessageNotWriteableException.class)
    public void testNotWriteable() throws JMSException {
        SQSBytesMessage msg = new SQSBytesMessage();

        byte[] byteArray = new byte[] { 'a', 0, 34, 65 };
        msg.writeBytes(byteArray);
        msg.reset();
        assertEquals('a', msg.readByte());
        msg.writeInt(10);
    }

    /**
     * Test before reset the message is not readable
     */
    @Test(expected = MessageNotReadableException.class)
    public void testReadable() throws JMSException {
        when(mockSQSSession.createBytesMessage()).thenReturn(new SQSBytesMessage());
        SQSBytesMessage msg = (SQSBytesMessage) mockSQSSession.createBytesMessage(); 
        
        byte[] byteArray = new byte[] { 'a', 0, 34, 65 };
        msg.writeBytes(byteArray);
        
        msg.readInt();
    }
}