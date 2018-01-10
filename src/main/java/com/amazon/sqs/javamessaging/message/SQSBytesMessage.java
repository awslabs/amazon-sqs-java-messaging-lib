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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import com.amazonaws.util.Base64;

import com.amazonaws.services.sqs.model.Message;

/**
 * This class borrows from <code>ActiveMQStreamMessage</code>, which is also
 * licensed under Apache2.0. Its methods are based largely on those found in
 * <code>java.io.DataInputStream</code> and
 * <code>java.io.DataOutputStream</code>.
 * 
 * @see org.apache.activemq.command.ActiveMQStreamMessage
 */
public class SQSBytesMessage extends SQSMessage implements BytesMessage {
    private static final Log LOG = LogFactory.getLog(SQSBytesMessage.class);

    private byte[] bytes;

    private ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

    private DataInputStream dataIn;
    
    private DataOutputStream dataOut = new DataOutputStream(bytesOut);

    /**
     * Convert received SQSMessage into BytesMessage.
     */
    public SQSBytesMessage(Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException {
        super(acknowledger, queueUrl, sqsMessage);
        try {
            /** Bytes is set by the reset() */
            dataOut.write(Base64.decode(sqsMessage.getBody()));
            /** Makes it read-only */
            reset();
        } catch (IOException e) {
            LOG.error("IOException: Message cannot be written", e);
            throw convertExceptionToJMSException(e);
        } catch (Exception e) {
            LOG.error("Unexpected exception: ", e);
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Create new empty BytesMessage to send.
     */
    public SQSBytesMessage() throws JMSException {
        super();
    }
    
    /**
     * Gets the number of bytes of the message body when the message is in
     * read-only mode. The value returned can be used to allocate a byte array.
     * The value returned is the entire length of the message body, regardless
     * of where the pointer for reading the message is currently located.
     * 
     * @return number of bytes in the message
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public long getBodyLength() throws JMSException {
        checkCanRead();
        return bytes.length;
    }
    
    /**
     * Reads a <code>boolean</code> from the bytes message stream.
     * 
     * @return the <code>boolean</code> value
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public boolean readBoolean() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readBoolean();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a signed 8-bit value from the bytes message stream.
     * 
     * @return the next byte from the bytes message stream as a signed 8-bit
     *         byte
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public byte readByte() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads an unsigned 8-bit value from the bytes message stream.
     * 
     * @return the next byte from the bytes message stream, interpreted as an
     *         unsigned 8-bit number
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public int readUnsignedByte() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readUnsignedByte();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a signed 16-bit number from the bytes message stream.
     * 
     * @return the next two bytes from the bytes message stream, interpreted as
     *         a signed 16-bit number
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public short readShort() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads an unsigned 16-bit number from the bytes message stream.
     * 
     * @return the next two bytes from the bytes message stream, interpreted as
     *         an unsigned 16-bit integer
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public int readUnsignedShort() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readUnsignedShort();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a Unicode character value from the bytes message stream. 
     * 
     * @return a Unicode character from the bytes message stream
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public char readChar() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readChar();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a 32-bit integer from the bytes message stream.
     * 
     * @return the next four bytes from the bytes message stream, interpreted as an int
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public int readInt() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readInt();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a 64-bit integer from the bytes message stream.
     * 
     * @return a 64-bit integer value from the bytes message stream, interpreted as a long
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public long readLong() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readLong();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a <code>float</code> from the bytes message stream.
     * 
     * @return a <code>float</code> value from the bytes message stream
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public float readFloat() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readFloat();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a <code>double</code> from the bytes message stream. 
     * 
     * @return a <code>double</code> value from the bytes message stream
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public double readDouble() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readDouble();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a string that has been encoded using a UTF-8 format from
     * the bytes message stream
     * 
     * @return a Unicode string from the bytes message stream
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageEOFException
     *             If unexpected end of bytes stream has been reached.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public String readUTF() throws JMSException {
        checkCanRead();
        try {
            return dataIn.readUTF();
        } catch (EOFException e) {
            throw new MessageEOFException(e.getMessage());
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Reads a byte array from the bytes message stream.
     * <P>
     * If the length of array value is less than the number of bytes remaining
     * to be read from the stream, the array should be filled. A subsequent call
     * reads the next increment, and so on.
     * <P>
     * If the number of bytes remaining in the stream is less than the length of
     * array value, the bytes should be read into the array. The return value of
     * the total number of bytes read will be less than the length of the array,
     * indicating that there are no more bytes left to be read from the stream.
     * The next read of the stream returns -1.
     * 
     * @param value
     *            The buffer into which the data is read
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the stream has been reached
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }
    
    
    /**
     * Reads a portion of the bytes message stream.
     * <P>
     * If the length of array value is less than the number of bytes remaining
     * to be read from the stream, the array should be filled. A subsequent call
     * reads the next increment, and so on.
     * <P>
     * If the number of bytes remaining in the stream is less than the length of
     * array value, the bytes should be read into the array. The return value of
     * the total number of bytes read will be less than the length of the array,
     * indicating that there are no more bytes left to be read from the stream.
     * The next read of the stream returns -1.
     * <P>
     * If length is negative, then an <code>IndexOutOfBoundsException</code> is
     * thrown. No bytes will be read from the stream for this exception case.
     * 
     * @param value
     *            The buffer into which the data is read
     * @param length
     *            The number of bytes to read; must be less than or equal to
     *            value.length
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the stream has been reached
     * @throws JMSException
     *             If the JMS provider fails to read the message due to some
     *             internal error.
     * @throws MessageNotReadableException
     *             If the message is in write-only mode.
     */
    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        if (length < 0) {
            throw new IndexOutOfBoundsException("Length bytes to read can't be smaller than 0 but was " +
                                                length);
        }
        checkCanRead();
        try {
            /**
             * Almost copy of readFully implementation except that EOFException
             * is not thrown if the stream is at the end of file and no byte is
             * available
             */
            int n = 0;
            while (n < length) {
                int count = dataIn.read(value, n, length - n);
                if (count < 0) {
                    break;
                }
                n += count;
            }
            /**
             * JMS specification mentions that the next read of the stream
             * returns -1 if the previous read consumed the byte stream and
             * there are no more bytes left to be read from the stream
             */
            if (n == 0 && length > 0) {
                n = -1;
            }
            return n;
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>boolean</code> to the bytes message stream
     * 
     * @param value
     *            The <code>boolean</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeBoolean(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>byte</code> to the bytes message stream
     * 
     * @param value
     *            The <code>byte</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeByte(byte value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeByte(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>short</code> to the bytes message stream
     * 
     * @param value
     *            The <code>short</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeShort(short value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeShort(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>char</code> to the bytes message stream
     * 
     * @param value
     *            The <code>char</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeChar(char value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeChar(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>int</code> to the bytes message stream
     * 
     * @param value
     *            The <code>int</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeInt(int value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeInt(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>long</code> to the bytes message stream
     * 
     * @param value
     *            The <code>long</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeLong(long value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeLong(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>float</code> to the bytes message stream
     * 
     * @param value
     *            The <code>float</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeFloat(float value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeFloat(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a <code>double</code> to the bytes message stream
     * 
     * @param value
     *            The <code>double</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeDouble(double value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeDouble(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a string that has been encoded using a UTF-8 format to the bytes
     * message stream
     * 
     * @param value
     *            The <code>String</code> value to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeUTF(String value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeUTF(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a byte array to the bytes
     * message stream
     * 
     * @param value
     *            The byte array value to be written 
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeBytes(byte[] value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.write(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes a portion of a byte array to the bytes message stream.
     * 
     * @param value
     *            The portion of byte array value to be written
     * @param offset
     *            The initial offset within the byte array
     * @param length
     *            The number of bytes to use 
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     */
    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        checkCanWrite();
        try {
            dataOut.write(value, offset, length);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }
    
    /**
     * Writes an object to the bytes message stream.
     * <P>
     * This method works only for the boxed primitive object types
     * (Integer, Double, Long ...), String objects, and byte arrays.
     * 
     * @param value
     *            The Java object to be written
     * @throws JMSException
     *             If the JMS provider fails to write the message due to some
     *             internal error.
     * @throws MessageNotWriteableException
     *             If the message is in read-only mode.
     * @throws MessageFormatException
     *             If the object is of an invalid type.
     * @throws NullPointerException
     *             If the object is null.
     */
    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null) {
            throw new NullPointerException("Cannot write null value of object");
        }
        if (value instanceof Boolean) {
            writeBoolean((Boolean) value);
        } else if (value instanceof Character) {
            writeChar((Character) value);
        } else if (value instanceof Byte) {
            writeByte((Byte) value);
        } else if (value instanceof Short) {
            writeShort((Short) value);
        } else if (value instanceof Integer) {
            writeInt((Integer) value);
        } else if (value instanceof Long) {
            writeLong((Long) value);
        } else if (value instanceof Float) {
            writeFloat((Float) value);
        } else if (value instanceof Double) {
            writeDouble((Double) value);
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Cannot write non-primitive type: " + value.getClass());
        }
    }

    /**
     * Puts the message body in read-only mode and repositions the stream of
     * bytes to the beginning.
     */
    @Override
    public void reset() throws JMSException {

        if (dataOut != null) {
            bytes = bytesOut.toByteArray();
            dataOut = null;
            bytesOut = null;
        }
        dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
    }
    
    /**
     * When the message is first created, and when clearBody is called, the body
     * of the message is in write-only mode. After the first call to reset is
     * made, the message body goes to read-only mode. when the message is sent,
     * the sender can retain and modify it without affecting the sent message.
     * If clearBody is called on a message, when it is in read-only mode, the
     * message body is cleared and the message goes to write-only mode.
     */
    @Override
    public void clearBody() throws JMSException {
        bytes = null;
        dataIn = null;
        bytesOut = new ByteArrayOutputStream();
        dataOut = new DataOutputStream(bytesOut);
        setBodyWritePermissions(true);
    }
    
    /**
     * Reads the body of message, which can be either the body returned from the
     * the receives message as bytes or the bytes put in bytesOut if it is a
     * sent message.
     * 
     * @return value The body returned as byte array
     */
    public byte[] getBodyAsBytes() throws JMSException {
        if (bytes != null) {
            return Arrays.copyOf(bytes, bytes.length);
        } else {
            return bytesOut.toByteArray();
        }
    }

    void checkCanRead() throws JMSException {
        if (bytes == null) {
            throw new MessageNotReadableException("Message is not readable");
        }
    }

    void checkCanWrite() throws JMSException {
        if (dataOut == null) {
            throw new MessageNotWriteableException("Message is not writeable");
        }
    }

    /*
     * Unit Test Utility Function
     */

    void setDataIn(DataInputStream dataIn) {
        this.dataIn = dataIn;
    }

    void setDataOut(DataOutputStream dataOut) {
        this.dataOut = dataOut;
    }
}
