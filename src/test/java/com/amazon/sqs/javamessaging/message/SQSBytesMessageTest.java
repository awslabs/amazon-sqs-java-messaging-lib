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
package com.amazon.sqs.javamessaging.message;

import com.amazon.sqs.javamessaging.SQSMessagingClientConstants;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.util.Base64;
import jakarta.jms.JMSException;
import jakarta.jms.MessageEOFException;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotReadableException;
import jakarta.jms.MessageNotWriteableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Test the SQSBytesMessageTest class
 */
public class SQSBytesMessageTest {
    
    private SQSSession mockSQSSession;
    private final static Map<String, String> MESSAGE_SYSTEM_ATTRIBUTES = Map.of(
            SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT, "1");

    @BeforeEach
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
        assertThrows(MessageEOFException.class, receivedByteMsg::readBoolean);
        assertThrows(MessageEOFException.class, receivedByteMsg::readUnsignedByte);

        byte[] arr = new byte[10];
        assertEquals(-1, receivedByteMsg.readBytes(arr, 10));
        assertThrows(MessageEOFException.class, receivedByteMsg::readByte);
        assertThrows(MessageEOFException.class, receivedByteMsg::readShort);
        assertThrows(MessageEOFException.class, receivedByteMsg::readUnsignedShort);
        assertThrows(MessageEOFException.class, receivedByteMsg::readInt);
        assertThrows(MessageEOFException.class, receivedByteMsg::readLong);
        assertThrows(MessageEOFException.class, receivedByteMsg::readFloat);
        assertThrows(MessageEOFException.class, receivedByteMsg::readDouble);
        assertThrows(MessageEOFException.class, receivedByteMsg::readChar);
        assertThrows(MessageEOFException.class, receivedByteMsg::readUTF);
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

        when(is.read()).thenThrow(ioException);

        when(is.read(any(byte[].class), anyInt(), anyInt())).thenThrow(ioException);

        SQSBytesMessage msg = spy(new SQSBytesMessage());
        doNothing().when(msg).checkCanRead();

        msg.setDataIn(dis);

        assertThrows(JMSException.class, msg::readBoolean);
        assertThrows(JMSException.class, msg::readBoolean);
        assertThrows(JMSException.class, msg::readByte);
        assertThrows(JMSException.class, msg::readUnsignedByte);
        assertThrows(JMSException.class, msg::readShort);
        assertThrows(JMSException.class, msg::readUnsignedShort);
        assertThrows(JMSException.class, msg::readInt);
        assertThrows(JMSException.class, msg::readLong);
        assertThrows(JMSException.class, msg::readFloat);
        assertThrows(JMSException.class, msg::readDouble);
        assertThrows(JMSException.class, msg::readChar);
        assertThrows(JMSException.class, msg::readUTF);
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
        doNothing().when(msg).checkCanRead();

        msg.setDataOut(dos);

        doThrow(ioException).when(os).write(anyInt());
        doThrow(ioException).when(os).write(any(byte[].class), anyInt(), anyInt());
        try {
            msg.writeBoolean(true);
            fail();
        } catch (JMSException exception) {
            assertEquals(ioException, exception.getCause());
        }

        assertThrows(JMSException.class, () -> msg.writeByte((byte)1));
        assertThrows(JMSException.class, () -> msg.writeShort((short)1));
        assertThrows(JMSException.class, () -> msg.writeInt(1));
        assertThrows(JMSException.class, () -> msg.writeLong(1290772974281L));
        assertThrows(JMSException.class, () -> msg.writeFloat(3.1457f));
        assertThrows(JMSException.class, () -> msg.writeDouble(2.1768));
        assertThrows(JMSException.class, () -> msg.writeChar('a'));
        assertThrows(JMSException.class, () -> msg.writeUTF("test"));
    }

    /**
     * Test write object function
     */
    @Test
    public void testWriteObject() throws JMSException {
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
        assertThrows(MessageFormatException.class, () -> msg.writeObject(new HashSet<String>()));

        /*
         * Check write object error cases
         */
        assertThrows(NullPointerException.class, () -> msg.writeObject(null));
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
        assertThrows(MessageNotReadableException.class, () -> msg.readBytes(readByteArray), "Message is not readable");

        msg.writeBytes(byteArray);
    }

    /**
     * Test after reset the message is read only mode
     */
    @Test
    public void testNotWriteable() throws JMSException {
        SQSBytesMessage msg = new SQSBytesMessage();

        byte[] byteArray = new byte[] { 'a', 0, 34, 65 };
        msg.writeBytes(byteArray);
        msg.reset();

        assertEquals('a', msg.readByte());
        assertThrows(MessageNotWriteableException.class, () -> msg.writeInt(10));
    }

    /**
     * Test before reset the message is not readable
     */
    @Test
    public void testReadable() throws JMSException {
        when(mockSQSSession.createBytesMessage()).thenReturn(new SQSBytesMessage());
        SQSBytesMessage msg = (SQSBytesMessage) mockSQSSession.createBytesMessage(); 
        
        byte[] byteArray = new byte[] { 'a', 0, 34, 65 };
        msg.writeBytes(byteArray);

        assertThrows(MessageNotReadableException.class, msg::readInt);
    }
}
