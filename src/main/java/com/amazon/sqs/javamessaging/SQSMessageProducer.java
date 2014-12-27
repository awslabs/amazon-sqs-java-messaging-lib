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

import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage.JMSMessagePropertyValue;
import com.amazonaws.util.Base64;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * A client uses a MessageProducer object to send messages to a queue
 * destination. A MessageProducer object is created by passing a Destination
 * object to a message-producer creation method supplied by a session.
 * <P>
 * A client also has the option of creating a message producer without supplying
 * a queue destination. In this case, a destination must be provided with every send
 * operation.
 * <P>
 */
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

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;
    private final SQSQueueDestination sqsDestination;
    private final SQSSession parentSQSSession;

    SQSMessageProducer(AmazonSQSMessagingClientWrapper amazonSQSClient, SQSSession parentSQSSession,
                       SQSQueueDestination destination) throws JMSException {
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
        SendMessageRequest sendMessageRequest = new SendMessageRequest(((SQSQueueDestination) queue).getQueueUrl(), sqsMessageBody);
        sendMessageRequest.setMessageAttributes(messageAttributes);

        String messageId = amazonSQSClient.sendMessage(sendMessageRequest).getMessageId();
        LOG.info("Message sent to SQS with SQS-assigned messageId: " + messageId);
        /** TODO: Do not support disableMessageID for now.*/
        message.setJMSMessageID(String.format(SQSMessagingClientConstants.MESSAGE_ID_FORMAT, messageId));
        ((SQSMessage)message).setSQSMessageId(messageId);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return sqsDestination;
    }
    
    /**
     * Sends a message to a queue.
     * 
     * @param queue
     *            the queue destination to send this message to
     * @param message
     *            the message to send
     * @throws InvalidDestinationException
     *             If a client uses this method with a destination other than
     *             SQS queue destination.
     * @throws MessageFormatException
     *             If an invalid message is specified.
     * @throws UnsupportedOperationException
     *             If a client uses this method with a MessageProducer that
     *             specified a destination at creation time.
     * @throws JMSException
     *             If session is closed or internal error.
     */
    @Override
    public void send(Queue queue, Message message) throws JMSException {
        if (!(queue instanceof SQSQueueDestination)) {
            throw new InvalidDestinationException(
                    "Incompatible implementation of Queue. Please use SQSQueueDestination implementation.");
        }
        checkIfDestinationAlreadySet();
        sendInternal(queue, message);
    }

    /**
     * Not verified on the client side, but SQS Attribute names must be valid
     * letter or digit on the basic multilingual plane in addition to allowing
     * '_', '-' and '.'. No component of an attribute name may be empty, thus an
     * attribute name may neither start nor end in '.'. And it may not contain
     * "..".
     */
    Map<String, MessageAttributeValue> propertyToMessageAttribute(SQSMessage message)
            throws JMSException {
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
        Enumeration<String> propertyNames = message.getPropertyNames();

        while (propertyNames.hasMoreElements()) {
            String propertyName = propertyNames.nextElement();

            // This is generated from SQS message attribute "ApproximateReceiveCount"
            if (propertyName.equals(SQSMessagingClientConstants.JMSX_DELIVERY_COUNT)) {
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
     * request.
     */
    private void addMessageTypeReservedAttribute(Map<String, MessageAttributeValue> messageAttributes,
                                                 SQSMessage message, String value) throws JMSException {

        MessageAttributeValue messageAttributeValue = new MessageAttributeValue();

        messageAttributeValue.setDataType(SQSMessagingClientConstants.STRING);
        messageAttributeValue.setStringValue(value);

        /**
         * This will override the existing attribute if exists. Everything that
         * has prefix JMS_ is reserved for JMS Provider, but if the user sets that
         * attribute, it will be overwritten.
         */
        messageAttributes.put(SQSMessage.JMS_SQS_MESSAGE_TYPE, messageAttributeValue);
    }

    /**
     * Sends a message to a queue.
     * <P>
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * ignore anything in deliveryMode, priority, and timeToLive.
     * 
     * @param queue
     *            the queue destination to send this message to
     * @param message
     *            the message to send
     * @param deliveryMode
     * @param priority
     * @param timeToLive           
     * @throws InvalidDestinationException
     *             If a client uses this method with a destination other than
     *             SQS queue destination.
     * @throws MessageFormatException
     *             If an invalid message is specified.
     * @throws UnsupportedOperationException
     *             If a client uses this method with a MessageProducer that
     *             specified a destination at creation time.
     * @throws JMSException
     *             If session is closed or internal error.
     */
    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        send(queue, message);
    }
    
    /**
     * Gets the destination associated with this MessageProducer.
     * 
     * @return this producer's queue destination
     */
    @Override
    public Destination getDestination() throws JMSException {
        return sqsDestination;
    }
    
    /**
     * Closes the message producer.
     */
    @Override
    public void close() throws JMSException {

        if (closed.compareAndSet(false, true)) {
            parentSQSSession.removeProducer(this);
        }
    }
    
    /**
     * Sends a message to a destination created during the creation time of this
     * message producer.
     * 
     * @param message
     *            the message to send
     * @throws MessageFormatException
     *             If an invalid message is specified.
     * @throws UnsupportedOperationException
     *             If a client uses this method with a MessageProducer that did
     *             not specify a destination at creation time.
     * @throws JMSException
     *             If session is closed or internal error.
     */
    @Override
    public void send(Message message) throws JMSException {
        if (sqsDestination == null) {
            throw new UnsupportedOperationException(
                    "MessageProducer has to specify a destination at creation time.");
        }
        sendInternal((Queue) sqsDestination, message);
    }
  
    /**
     * Sends a message to a destination created during the creation time of this
     * message producer.
     * <P>
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * ignore anything in deliveryMode, priority, and timeToLive.
     * 
     * @param message
     *            the message to send
     * @param deliveryMode
     * @param priority
     * @param timeToLive           
     * @throws MessageFormatException
     *             If an invalid message is specified.
     * @throws UnsupportedOperationException
     *             If a client uses this method with a MessageProducer that did
     *             not specify a destination at creation time.
     * @throws JMSException
     *             If session is closed or internal error.
     */
    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message);
    }
    
    /**
     * Sends a message to a queue destination.
     * 
     * @param destination
     *            the queue destination to send this message to
     * @param message
     *            the message to send
     * @throws InvalidDestinationException
     *             If a client uses this method with a destination other than
     *             valid SQS queue destination.
     * @throws MessageFormatException
     *             If an invalid message is specified.
     * @throws UnsupportedOperationException
     *             If a client uses this method with a MessageProducer that
     *             specified a destination at creation time.
     * @throws JMSException
     *             If session is closed or internal error.
     */
    @Override
    public void send(Destination destination, Message message) throws JMSException {
        if (destination == null) {
            throw new InvalidDestinationException("Destination cannot be null");
        }
        if (destination instanceof SQSQueueDestination) {
            send((Queue) destination, message);
        } else {
            throw new InvalidDestinationException("Incompatible implementation of Destination. Please use SQSQueueDestination implementation.");
        }
    }

    /**
     * Sends a message to a queue destination.
     * <P>
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * ignore anything in deliveryMode, priority, and timeToLive.
     * 
     * @param destination
     *            the queue destination to send this message to
     * @param message
     *            the message to send
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws InvalidDestinationException
     *             If a client uses this method with a destination other than
     *             valid SQS queue destination.
     * @throws MessageFormatException
     *             If an invalid message is specified.
     * @throws UnsupportedOperationException
     *             If a client uses this method with a MessageProducer that
     *             specified a destination at creation time.
     * @throws JMSException
     *             If session is closed or internal error.
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
