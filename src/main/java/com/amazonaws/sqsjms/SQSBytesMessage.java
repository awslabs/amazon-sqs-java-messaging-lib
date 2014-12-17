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

import com.amazonaws.sqsjms.acknowledge.Acknowledger;
import com.amazonaws.util.Base64;

import com.amazonaws.services.sqs.model.Message;

public class SQSBytesMessage extends SQSMessage implements BytesMessage {
    private static final Log LOG = LogFactory.getLog(SQSBytesMessage.class);

    private byte[] bytes;

    private ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

    private DataInputStream dataIn;
    
    private DataOutputStream dataOut = new DataOutputStream(bytesOut);

    /**
     * Convert received SQSMessage into BytesMessage.
     */
    SQSBytesMessage(Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException {
        super(acknowledger, queueUrl, sqsMessage);
        try {
        	/** Bytes is set by the reset() */
            dataOut.write(Base64.decode(sqsMessage.getBody()));
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
    SQSBytesMessage() throws JMSException {
        super();
    }

    @Override
    public long getBodyLength() throws JMSException {
        checkCanRead();
        return bytes.length;
    }
    
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

    @Override
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }

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

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeBoolean(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeByte(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeShort(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeChar(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeInt(int value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeInt(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeLong(long value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeLong(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeFloat(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeDouble(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.writeUTF(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        checkCanWrite();
        try {
            dataOut.write(value);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        checkCanWrite();
        try {
            dataOut.write(value, offset, length);
        } catch (IOException e) {
            throw convertExceptionToJMSException(e);
        }
    }

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
	 */
	byte[] getBodyAsBytes() throws JMSException {
		if (bytes != null) {
			return Arrays.copyOf(bytes, bytes.length);
		} else {
			return bytesOut.toByteArray();
		}
	}
    
    private void checkCanRead() throws JMSException {        
        if (bytes == null) {
            throw new MessageNotReadableException("Message is not readable");
        }
    }

    private void checkCanWrite() throws JMSException {
        if (dataOut == null) {
            throw new MessageNotWriteableException("Message is not writeable");
        }
    }
}
