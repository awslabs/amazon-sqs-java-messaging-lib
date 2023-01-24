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
import jakarta.jms.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.utils.BinaryUtils;

import java.io.*;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test the SQSBytesMessageTest class
 */
public class SQSBytesMessageTest {

    private SQSSession mockSQSSession;

    private final static Map<MessageSystemAttributeName, String> MESSAGE_SYSTEM_ATTRIBUTES = Map.of(
            MessageSystemAttributeName.fromValue(SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT), "1");

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

        byte[] byteArray = new byte[]{1, 0, 'a', 65};
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

        Message message = Message.builder()
                .body(BinaryUtils.toBase64(body))
                .attributes(MESSAGE_SYSTEM_ATTRIBUTES)
                .build();

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

        assertThatThrownBy(receivedByteMsg::readBoolean).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readUnsignedByte).isInstanceOf(MessageEOFException.class);

        byte[] arr = new byte[10];
        assertEquals(-1, receivedByteMsg.readBytes(arr, 10));

        assertThatThrownBy(receivedByteMsg::readByte).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readShort).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readUnsignedShort).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readInt).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readLong).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readFloat).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readDouble).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readChar).isInstanceOf(MessageEOFException.class);
        assertThatThrownBy(receivedByteMsg::readUTF).isInstanceOf(MessageEOFException.class);
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

        assertThatThrownBy(msg::readBoolean).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readByte).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readUnsignedByte).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readShort).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readUnsignedShort).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readInt).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readLong).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readFloat).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readDouble).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readChar).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
        assertThatThrownBy(msg::readUTF).isInstanceOf(JMSException.class).cause().isEqualTo(ioException);
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

        assertThatThrownBy(() -> msg.writeByte((byte) 1))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);

        assertThatThrownBy(() -> msg.writeShort((short) 1))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);

        assertThatThrownBy(() -> msg.writeInt(1))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);

        assertThatThrownBy(() -> msg.writeLong(1290772974281L))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);

        assertThatThrownBy(() -> msg.writeFloat(3.1457f))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);

        assertThatThrownBy(() -> msg.writeDouble(2.1768))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);

        assertThatThrownBy(() -> msg.writeChar('a'))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);

        assertThatThrownBy(() -> msg.writeUTF("test"))
                .isInstanceOf(JMSException.class)
                .cause().isEqualTo(ioException);
    }

    /**
     * Test write object function
     */
    @Test
    public void testWriteObject() throws JMSException {
        SQSBytesMessage msg = new SQSBytesMessage();

        byte[] byteArray = new byte[]{1, 0, 'a', 65};
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

        Message message = Message.builder()
                .attributes(MESSAGE_SYSTEM_ATTRIBUTES)
                .body(BinaryUtils.toBase64(body))
                .build();
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
        assertThatThrownBy(() -> msg.writeObject(Set.of())).isInstanceOf(MessageFormatException.class);

        /*
         * Check write object error cases
         */
        assertThatThrownBy(() -> msg.writeObject(null)).isInstanceOf(NullPointerException.class);
    }

    /**
     * Test clear body
     */
    @Test
    public void testClearBody() throws JMSException {
        SQSBytesMessage msg = new SQSBytesMessage();

        byte[] byteArray = new byte[]{1, 0, 'a', 65};
        msg.writeBytes(byteArray);

        msg.clearBody();

        byte[] readByteArray = new byte[4];

        /*
         * Verify message is in write-only mode
         */
        assertThatThrownBy(() -> msg.readBytes(readByteArray))
                .isInstanceOf(MessageNotReadableException.class)
                .hasMessage("Message is not readable");

        msg.writeBytes(byteArray);
    }

    /**
     * Test after reset the message is read only mode
     */
    @Test
    public void testNotWriteable() throws JMSException {
        SQSBytesMessage msg = new SQSBytesMessage();

        byte[] byteArray = new byte[]{'a', 0, 34, 65};
        msg.writeBytes(byteArray);
        msg.reset();
        assertEquals('a', msg.readByte());

        assertThatThrownBy(() -> msg.writeInt(10))
                .isInstanceOf(MessageNotWriteableException.class);
    }

    /**
     * Test before reset the message is not readable
     */
    @Test
    public void testReadable() throws JMSException {
        when(mockSQSSession.createBytesMessage()).thenReturn(new SQSBytesMessage());
        SQSBytesMessage msg = (SQSBytesMessage) mockSQSSession.createBytesMessage();

        byte[] byteArray = new byte[]{'a', 0, 34, 65};
        msg.writeBytes(byteArray);

        assertThatThrownBy(msg::readInt).isInstanceOf(MessageNotReadableException.class);
    }
}
