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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.util.Base64;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.sqsjms.acknowledge.Acknowledger;

public class SQSObjectMessage extends SQSMessage implements ObjectMessage {
    private static final Log LOG = LogFactory.getLog(SQSObjectMessage.class);

    /**
     * Serialized message body
     */
    private String body;

    /**
     * Convert received SQSMessage into ObjectMessage
     */
    SQSObjectMessage(Acknowledger acknowledger, String queueUrl, Message sqsMessage) throws JMSException {
        super(acknowledger, queueUrl, sqsMessage);
        body = sqsMessage.getBody();
    }
    
    /**
     * Create new empty ObjectMessage to send.
     */
    SQSObjectMessage() throws JMSException {
        super();
    }

    /**
     * Create new ObjectMessage with payload to send.
     */
    SQSObjectMessage(Serializable payload) throws JMSException {
        super();
        body = serialize(payload);
    }

    @Override
    public void setObject(Serializable payload) throws JMSException {
        checkBodyWritePermissions();
        body = serialize(payload);
    }

    @Override
    public Serializable getObject() throws JMSException {
        return deserialize(body);
    }

    @Override
    public void clearBody() throws JMSException {
        body = null;
        setBodyWritePermissions(true);
    }

    protected static Serializable deserialize(String serialized) throws JMSException {
        if (serialized == null) {
            return null;
        }
        Serializable deserializedObject;
        ObjectInputStream objectInputStream = null;
        try {
            byte[] bytes = Base64.decode(serialized);
            objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            deserializedObject = (Serializable) objectInputStream.readObject();
        } catch (IOException e) {
            LOG.error("IOException: Message cannot be written", e);
            throw convertExceptionToMessageFormatException(e);
        } catch (Exception e) {
            LOG.error("Unexpected exception: ", e);
            throw convertExceptionToMessageFormatException(e);
        } finally {
            if (objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    LOG.warn(e.getMessage());
                }
            }
        }
        return deserializedObject;
    }

    protected static String serialize(Serializable serializable) throws JMSException {
        if (serializable == null) {
            return null;
        }
        String serializedString;
        ObjectOutputStream objectOutputStream = null;
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(bytesOut);
            objectOutputStream.writeObject(serializable);
            objectOutputStream.flush();
            serializedString = Base64.encodeAsString(bytesOut.toByteArray());
        } catch (IOException e) {
            LOG.error("IOException: cannot serialize objectMessage", e);
            throw convertExceptionToMessageFormatException(e);
        } finally {
            if (objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    LOG.warn(e.getMessage());
                }
            }
        }
        return serializedString;
    }

    public String getMessageBody() {
        return body;
    }

    public void setMessageBody(String body) {
        this.body = body;
    }
}
