/*
 * Copyright 2010-2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.sqs.javamessaging.message.SQSBytesMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazon.sqs.javamessaging.message.SQSMessage.JMSMessagePropertyValue;
import com.amazon.sqs.javamessaging.message.SQSObjectMessage;
import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import jakarta.jms.CompletionListener;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateException;
import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.QueueSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest.Builder;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;
import software.amazon.awssdk.utils.BinaryUtils;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A client uses a MessageProducer object to send messages to a queue
 * destination. A MessageProducer object is created by passing a Destination
 * object to a message-producer creation method supplied by a session.
 * <p>
 * A client also has the option of creating a message producer without supplying
 * a queue destination. In this case, a destination must be provided with every send
 * operation.
 */
public class SQSMessageProducer implements MessageProducer, QueueSender {
    private static final Logger LOG = LoggerFactory.getLogger(SQSMessageProducer.class);

    private final long MAXIMUM_DELIVERY_DELAY_MILLISECONDS = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);

    private int deliveryDelaySeconds = 0;

    /**
     * This field is not actually used.
     */
    private long timeToLive;
    /**
     * This field is not actually used.
     */
    private int defaultPriority;
    /**
     * This field is not actually used.
     */
    private int deliveryMode;
    /**
     * This field is not actually used.
     */
    private boolean disableMessageTimestamp;
    /**
     * This field is not actually used.
     */
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

    void sendInternal(SQSQueueDestination queue, Message rawMessage) throws JMSException {
        checkClosed();
        if (!(rawMessage instanceof SQSMessage message)) {
            throw new MessageFormatException(
                    "Unrecognized message type. Messages have to be one of: SQSBytesMessage, SQSObjectMessage, or SQSTextMessage");
        }

        String sqsMessageBody = null;
        String messageType = null;
        message.setJMSDestination(queue);
        if (message instanceof SQSBytesMessage sqsBytesMessage) {
            sqsMessageBody = BinaryUtils.toBase64(sqsBytesMessage.getBodyAsBytes());
            messageType = SQSMessage.BYTE_MESSAGE_TYPE;
        } else if (message instanceof SQSObjectMessage sqsObjectMessage) {
            sqsMessageBody = sqsObjectMessage.getMessageBody();
            messageType = SQSMessage.OBJECT_MESSAGE_TYPE;
        } else if (message instanceof SQSTextMessage sqsTextMessage) {
            sqsMessageBody = sqsTextMessage.getText();
            messageType = SQSMessage.TEXT_MESSAGE_TYPE;
        }

        if (sqsMessageBody == null || sqsMessageBody.isEmpty()) {
            throw new JMSException("Message body cannot be null or empty");
        }
        Map<String, MessageAttributeValue> messageAttributes = propertyToMessageAttribute(message);

        /*
         * These will override existing attributes if they exist. Everything that
         * has prefix JMS_ is reserved for JMS Provider, but if the user sets that
         * attribute, it will be overwritten.
         */
        addStringAttribute(messageAttributes, SQSMessage.JMS_SQS_MESSAGE_TYPE, messageType);
        addReplyToQueueReservedAttributes(messageAttributes, message);
        addCorrelationIDToQueueReservedAttributes(messageAttributes, message);

        Builder sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queue.getQueueUrl())
                .messageBody(sqsMessageBody)
                .messageAttributes(messageAttributes);

        if (deliveryDelaySeconds != 0) {
            sendMessageRequest.delaySeconds(deliveryDelaySeconds);
        }

        //for FIFO queues, we have to specify both MessageGroupId, which we obtain from standard property JMSX_GROUP_ID
        //and MessageDeduplicationId, which we obtain from a custom provider specific property JMS_SQS_DEDUPLICATION_ID
        //notice that this code does not validate if the values are actually set by the JMS user
        //this means that failure to provide the required values will fail server side and throw a JMSException
        if (queue.isFifo()) {
            sendMessageRequest.messageGroupId(message.getSQSMessageGroupId());
            sendMessageRequest.messageDeduplicationId(message.getSQSMessageDeduplicationId());
        }

        SendMessageResponse sendMessageResult = amazonSQSClient.sendMessage(sendMessageRequest.build());
        String messageId = sendMessageResult.messageId();
        LOG.info("Message sent to SQS with SQS-assigned messageId: {}", messageId);
        // TODO: Do not support disableMessageID for now.
        message.setSQSMessageId(messageId);

        // if the message was sent to FIFO queue, the sequence number will be
        // set in the response
        // pass it to JMS user through provider specific JMS property
        if (sendMessageResult.sequenceNumber() != null) {
            message.setSequenceNumber(sendMessageResult.sequenceNumber());
        }
    }

    @Override
    public Queue getQueue() throws JMSException {
        return sqsDestination;
    }

    /**
     * Sends a message to a queue.
     *
     * @param queue   the queue destination to send this message to
     * @param message the message to send
     * @throws InvalidDestinationException   If a client uses this method with a destination other than
     *                                       SQS queue destination.
     * @throws MessageFormatException        If an invalid message is specified.
     * @throws UnsupportedOperationException If a client uses this method with a MessageProducer that
     *                                       specified a destination at creation time.
     * @throws JMSException                  If session is closed or internal error.
     */
    @Override
    public void send(Queue queue, Message message) throws JMSException {
        if (!(queue instanceof SQSQueueDestination)) {
            throw new InvalidDestinationException(
                    "Incompatible implementation of Queue. Please use SQSQueueDestination implementation.");
        }
        checkIfDestinationAlreadySet();
        sendInternal((SQSQueueDestination) queue, message);
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
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        Enumeration<String> propertyNames = message.getPropertyNames();

        while (propertyNames.hasMoreElements()) {
            String propertyName = propertyNames.nextElement();

            // This is generated from SQS message attribute "ApproximateReceiveCount"
            if (propertyName.equals(SQSMessagingClientConstants.JMSX_DELIVERY_COUNT)) {
                continue;
            }

            // This property will be used as DeduplicationId argument of SendMessage call
            // On receive it is mapped back to this JMS property
            if (propertyName.equals(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID)) {
                continue;
            }

            // the JMSXGroupID and JMSXGroupSeq are always stored as message
            // properties, so they are not lost between send and receive
            // even though SQS Classic does not respect those values when returning messages
            // and SQS FIFO has a different understanding of message groups

            JMSMessagePropertyValue propertyObject = message.getJMSMessagePropertyValue(propertyName);
            MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                    .dataType(propertyObject.getType())
                    .stringValue(propertyObject.getStringMessageAttributeValue())
                    .build();

            messageAttributes.put(propertyName, messageAttributeValue);
        }
        return messageAttributes;
    }

    /**
     * Adds the reply-to queue name and url attributes during send as part of the send message
     * request, if necessary
     */
    private void addReplyToQueueReservedAttributes(Map<String, MessageAttributeValue> messageAttributes,
                                                   SQSMessage message) throws JMSException {

        Destination replyTo = message.getJMSReplyTo();
        if (replyTo instanceof SQSQueueDestination replyToQueue) {
            /*
             * This will override the existing attributes if exists. Everything that
             * has prefix JMS_ is reserved for JMS Provider, but if the user sets that
             * attribute, it will be overwritten.
             */
            addStringAttribute(messageAttributes, SQSMessage.JMS_SQS_REPLY_TO_QUEUE_NAME, replyToQueue.getQueueName());
            addStringAttribute(messageAttributes, SQSMessage.JMS_SQS_REPLY_TO_QUEUE_URL, replyToQueue.getQueueUrl());
        }
    }

    /**
     * Adds the correlation ID attribute during send as part of the send message
     * request, if necessary
     */
    private void addCorrelationIDToQueueReservedAttributes(Map<String, MessageAttributeValue> messageAttributes,
                                                           SQSMessage message) throws JMSException {

        String correlationID = message.getJMSCorrelationID();
        if (correlationID != null) {
            addStringAttribute(messageAttributes, SQSMessage.JMS_SQS_CORRELATION_ID, correlationID);
        }
    }

    /**
     * Convenience method for adding a single string attribute.
     */
    private void addStringAttribute(Map<String, MessageAttributeValue> messageAttributes,
                                    String key, String value) {
        MessageAttributeValue messageAttributeValue = MessageAttributeValue.builder()
                .dataType(SQSMessagingClientConstants.STRING)
                .stringValue(value)
                .build();
        messageAttributes.put(key, messageAttributeValue);
    }

    /**
     * Sends a message to a queue.
     * <p>
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * ignore anything in deliveryMode, priority, and timeToLive.
     *
     * @param queue        the queue destination to send this message to
     * @param message      the message to send
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws InvalidDestinationException   If a client uses this method with a destination other than
     *                                       SQS queue destination.
     * @throws MessageFormatException        If an invalid message is specified.
     * @throws UnsupportedOperationException If a client uses this method with a MessageProducer that
     *                                       specified a destination at creation time.
     * @throws JMSException                  If session is closed or internal error.
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
     * @param message the message to send
     * @throws MessageFormatException        If an invalid message is specified.
     * @throws UnsupportedOperationException If a client uses this method with a MessageProducer that did
     *                                       not specify a destination at creation time.
     * @throws JMSException                  If session is closed or internal error.
     */
    @Override
    public void send(Message message) throws JMSException {
        if (sqsDestination == null) {
            throw new UnsupportedOperationException(
                    "MessageProducer has to specify a destination at creation time.");
        }
        sendInternal(sqsDestination, message);
    }

    /**
     * Sends a message to a destination created during the creation time of this
     * message producer.
     * <p>
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * ignore anything in deliveryMode, priority, and timeToLive.
     *
     * @param message      the message to send
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws MessageFormatException        If an invalid message is specified.
     * @throws UnsupportedOperationException If a client uses this method with a MessageProducer that did
     *                                       not specify a destination at creation time.
     * @throws JMSException                  If session is closed or internal error.
     */
    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(message);
    }

    /**
     * Sends a message to a queue destination.
     *
     * @param destination the queue destination to send this message to
     * @param message     the message to send
     * @throws InvalidDestinationException   If a client uses this method with a destination other than
     *                                       valid SQS queue destination.
     * @throws MessageFormatException        If an invalid message is specified.
     * @throws UnsupportedOperationException If a client uses this method with a MessageProducer that
     *                                       specified a destination at creation time.
     * @throws JMSException                  If session is closed or internal error.
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
     * <p>
     * Send does not support deliveryMode, priority, and timeToLive. It will
     * ignore anything in deliveryMode, priority, and timeToLive.
     *
     * @param destination  the queue destination to send this message to
     * @param message      the message to send
     * @param deliveryMode
     * @param priority
     * @param timeToLive
     * @throws InvalidDestinationException   If a client uses this method with a destination other than
     *                                       valid SQS queue destination.
     * @throws MessageFormatException        If an invalid message is specified.
     * @throws UnsupportedOperationException If a client uses this method with a MessageProducer that
     *                                       specified a destination at creation time.
     * @throws JMSException                  If session is closed or internal error.
     */
    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(destination, message);
    }

    @Override
    public void send(Message message, CompletionListener completionListener) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public void send(Message message, int deliveryMode, int priority, long timeToLive, CompletionListener completionListener) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public void send(Destination destination, Message message, CompletionListener completionListener) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive,
                     CompletionListener completionListener) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /**
     * This method is not supported.
     */
    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        this.disableMessageID = value;
    }

    /**
     * This method is not supported.
     */
    @Override
    public boolean getDisableMessageID() throws JMSException {
        return disableMessageID;
    }

    /**
     * This method is not supported.
     */
    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        this.disableMessageTimestamp = value;
    }

    /**
     * This method is not supported.
     */
    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        return disableMessageTimestamp;
    }

    /**
     * This method is not supported.
     */
    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        this.deliveryMode = deliveryMode;
    }

    /**
     * This method is not supported.
     */
    @Override
    public int getDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    /**
     * This method is not supported.
     */
    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        this.defaultPriority = defaultPriority;
    }

    /**
     * This method is not supported.
     */
    @Override
    public int getPriority() throws JMSException {
        return defaultPriority;
    }

    /**
     * This method is not supported.
     */
    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        this.timeToLive = timeToLive;
    }

    /**
     * This method is not supported.
     */
    @Override
    public long getTimeToLive() throws JMSException {
        return timeToLive;
    }

    /**
     * Sets the minimum length of time in milliseconds that must elapse after a
     * message is sent before the JMS provider may deliver the message to a consumer.
     * <p>
     * This must be a multiple of 1000, since SQS only supports delivery delays
     * in seconds.
     */
    public void setDeliveryDelay(long deliveryDelay) {
        if (deliveryDelay < 0 || deliveryDelay > MAXIMUM_DELIVERY_DELAY_MILLISECONDS) {
            throw new IllegalArgumentException("Delivery delay must be non-negative and at most 15 minutes: " + deliveryDelay);
        }
        if (deliveryDelay % 1000 != 0) {
            throw new IllegalArgumentException("Delivery delay must be a multiple of 1000: " + deliveryDelay);
        }
        this.deliveryDelaySeconds = (int) (deliveryDelay / 1000);
    }

    /**
     * Gets the minimum length of time in milliseconds that must elapse after a
     * message is sent before the JMS provider may deliver the message to a consumer.
     */
    public long getDeliveryDelay() {
        return deliveryDelaySeconds * 1000L;
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
