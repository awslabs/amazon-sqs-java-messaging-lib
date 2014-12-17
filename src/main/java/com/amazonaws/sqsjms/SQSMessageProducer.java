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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.util.Base64;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.sqsjms.SQSMessage.JMSMessagePropertyValue;

public class SQSMessageProducer implements MessageProducer, QueueSender {
    private static final Log LOG = LogFactory.getLog(SQSMessageProducer.class);

    /** This field is not actually used. */
    private long timeToLive;
    /** This field is not actually used. */
    private int defaultPriority;
    /** This field is not actually used. */
    private int deliveryMode;
    /** This field is not actually used. */
    private boolean disableMessageTimestamp;
    /** This field is not actually used. */
    private boolean disableMessageID;

    /**
     * State of MessageProducer.
     * True if MessageProducer is closed.
     */
    final AtomicBoolean closed = new AtomicBoolean(false);

    private final AmazonSQSClientJMSWrapper amazonSQSClient;
    private final SQSDestination sqsDestination;
    private final SQSSession parentSQSSession;

    SQSMessageProducer(AmazonSQSClientJMSWrapper amazonSQSClient, SQSSession parentSQSSession,
                       SQSDestination destination) throws JMSException {
        this.sqsDestination = destination;
        this.amazonSQSClient = amazonSQSClient;
        this.parentSQSSession = parentSQSSession;
    }

    void sendInternal(Queue queue, Message message) throws JMSException {
        checkClosed();
        String sqsMessageBody = null;
        String messageType = null;
        if (message instanceof SQSMessage) {           
            message.setJMSDestination(queue);
            if (message instanceof SQSBytesMessage) {
                sqsMessageBody = Base64.encodeAsString(((SQSBytesMessage) message).getBodyAsBytes());
                messageType = SQSMessage.BYTE_MESSAGE_TYPE;
            } else if (message instanceof SQSObjectMessage) {
                sqsMessageBody = ((SQSObjectMessage) message).getMessageBody();
                messageType = SQSMessage.OBJECT_MESSAGE_TYPE;
            } else if (message instanceof SQSTextMessage) {            
                sqsMessageBody = ((SQSTextMessage) message).getText();
                messageType = SQSMessage.TEXT_MESSAGE_TYPE;
            }
        } else {
            throw new MessageFormatException(
                    "Unrecognized message type. Messages have to be one of: SQSBytesMessage, SQSObjectMessage, or SQSTextMessage");
        }
        if (sqsMessageBody == null || sqsMessageBody.isEmpty()) {
            throw new JMSException("Message body cannot be null or empty");
        }
        Map<String, MessageAttributeValue> messageAttributes = propertyToMessageAttribute((SQSMessage) message);
        addMessageTypeReservedAttribute(messageAttributes, (SQSMessage) message, messageType);
        SendMessageRequest sendMessageRequest = new SendMessageRequest(((SQSDestination) queue).getQueueUrl(), sqsMessageBody);
        sendMessageRequest.setMessageAttributes(messageAttributes);

        String messageId = amazonSQSClient.sendMessage(sendMessageRequest).getMessageId();
        LOG.info("Message sent to SQS with SQS-assigned messageId: " + messageId);
        /** TODO: Do not support disableMessageID for now.*/
        message.setJMSMessageID(String.format(SQSJMSClientConstants.MESSAGE_ID_FORMAT, messageId));
        ((SQSMessage)message).setSQSMessageId(messageId);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return sqsDestination;
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        if (!(queue instanceof SQSDestination)) {
            throw new InvalidDestinationException(
                    "Incompatible implementation of Queue. Please use SQSDestination implementation.");
        }
        checkIfDestinationAlreadySet();
        sendInternal(queue, message);
    }

    /**
     * Not verified on the client side, but SQS Attribute names must be
     * valid letter or digit on the basic multilingual plane in addition to
     * allowing '_', '-' and '.'. No component of an attribute name may be
     * empty, thus an attribute name may neither start nor end in '.'. And it
     * may not contain "..".
     */
    Map<String, MessageAttributeValue> propertyToMessageAttribute(SQSMessage message)
            throws JMSException {
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
        Enumeration<String> propertyNames = message.getPropertyNames();

        while (propertyNames.hasMoreElements()) {
            String propertyName = propertyNames.nextElement();

            // This is generated from SQS message attribute "ApproximateReceiveCount"
            if (propertyName.equals(SQSJMSClientConstants.JMSX_DELIVERY_COUNT)) {
                continue;
            }
            JMSMessagePropertyValue propertyObject = message.getJMSMessagePropertyValue(propertyName);
            MessageAttributeValue messageAttributeValue = new MessageAttributeValue();

            messageAttributeValue.setDataType(propertyObject.getType());
            messageAttributeValue.setStringValue(propertyObject.getStringMessageAttributeValue());

            messageAttributes.put(propertyName, messageAttributeValue);
        }
        return messageAttributes;
    }

    /**
     * Adds the message type attribute during send as part of the send message
     * request
     */
    private void addMessageTypeReservedAttribute(Map<String, MessageAttributeValue> messageAttributes,
                                                 SQSMessage message, String value) throws JMSException {

        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();

        messageAttributeValue.setDataType(SQSJMSClientConstants.STRING);
        messageAttributeValue.setStringValue(value);

        /**
         * This will override the existing attribute if exists. Everything that
         * has prefix JMS_ is reserved for JMS Provider, but if the user sets that
         * attribute, it will be overwritten.
         */
        messageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
    }

    /**
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * drop anything in deliveryMode, priority, and timeToLive.
     */
    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        send(queue, message);
    }

    @Override
    public Destination getDestination() throws JMSException {
        return sqsDestination;
    }

    @Override
    public void close() throws JMSException {

        if (closed.compareAndSet(false, true)) {
            parentSQSSession.removeProducer(this);
        }
    }

    @Override
    public void send(Message message) throws JMSException {
        if (sqsDestination == null) {
            throw new UnsupportedOperationException(
                    "MessageProducer has to specify a destination at creation time.");
        }
        sendInternal((Queue) sqsDestination, message);
    }

    /**
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * drop anything in deliveryMode, priority, and timeToLive.
     */
    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message);
    }

    @Override
    public void send(Destination destination, Message message) throws JMSException {
        if (destination == null) {
            throw new InvalidDestinationException("Destination cannot be null");
        }
        if (destination instanceof SQSDestination) {
            send((Queue) destination, message);
        } else {
            throw new InvalidDestinationException("Incompatible implementation of Destination. Please use SQSDestination implementation.");
        }
    }

    /**
     * Send does not support deliveryMode, priority, and timeToLive.
     * It will drop anything in deliveryMode, priority, and timeToLive.
     */
    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(destination, message);
    }

    /** This method is not supported. */
    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        this.disableMessageID = value;
    }

    /** This method is not supported. */
    @Override
    public boolean getDisableMessageID() throws JMSException {
        return disableMessageID;
    }

    /** This method is not supported. */
    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        this.disableMessageTimestamp = value;
    }

    /** This method is not supported. */
    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        return disableMessageTimestamp;
    }

    /** This method is not supported. */
    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        this.deliveryMode = deliveryMode;
    }

    /** This method is not supported. */
    @Override
    public int getDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    /** This method is not supported. */
    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        this.defaultPriority = defaultPriority;
    }

    /** This method is not supported. */
    @Override
    public int getPriority() throws JMSException {
        return defaultPriority;
    }

    /** This method is not supported. */
    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        this.timeToLive = timeToLive;
    }

    /** This method is not supported. */
    @Override
    public long getTimeToLive() throws JMSException {
        return timeToLive;
    }

    void checkClosed() throws IllegalStateException {
        if (closed.get()) {
            throw new IllegalStateException("The producer is closed.");
        }
    }

    void checkIfDestinationAlreadySet() {
        if (sqsDestination != null) {
            throw new UnsupportedOperationException(
                    "MessageProducer already specified a destination at creation time.");
        }
    }

    /*
     * Unit Tests Utility Functions
     */

    AtomicBoolean isClosed() {
        return closed;
    }
}
