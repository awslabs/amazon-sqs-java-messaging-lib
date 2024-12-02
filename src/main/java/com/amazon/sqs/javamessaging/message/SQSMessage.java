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

import com.amazon.sqs.javamessaging.SQSMessageConsumerPrefetch;
import com.amazon.sqs.javamessaging.SQSMessagingClientConstants;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.acknowledge.Acknowledger;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatException;
import jakarta.jms.MessageNotWriteableException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.APPROXIMATE_RECEIVE_COUNT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.BINARY;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.BOOLEAN;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.BYTE;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.DOUBLE;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.FLOAT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.INT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.INT_FALSE;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.INT_TRUE;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.JMSX_DELIVERY_COUNT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.JMSX_GROUP_ID;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.LONG;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.MESSAGE_DEDUPLICATION_ID;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.MESSAGE_GROUP_ID;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.NUMBER;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.SEQUENCE_NUMBER;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.SHORT;
import static com.amazon.sqs.javamessaging.SQSMessagingClientConstants.STRING;

/**
 * The SQSMessage is the root class of all SQS JMS messages and implements JMS
 * Message interface.
 * <p>
 * Not all message headers are supported at this time:
 * <ul>
 * <li><code>JMSMessageID</code> is always assigned as SQS provided message id.</li>
 * <li><code>JMSRedelivered</code> is set to true if SQS delivers the message
 * more than once. This not necessarily mean that the user received message more
 * than once, but rather SQS attempted to deliver it more than once. Due to
 * prefetching used in {@link SQSMessageConsumerPrefetch}, this can be set to
 * true although user never received the message. This is set based on SQS
 * ApproximateReceiveCount attribute</li>
 * <li><code>JMSDestination</code> is the destination object which message
 * is sent to and received from.</li>
 * </ul>
 * <p>
 * JMSXDeliveryCount reserved property is supported and set based on the
 * approximate reception count observed on the SQS side.
 */
public class SQSMessage implements Message {

    private static final Logger LOG = LoggerFactory.getLogger(SQSMessage.class);
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    // Define constant message types.
    public static final String BYTE_MESSAGE_TYPE = "byte";
    public static final String OBJECT_MESSAGE_TYPE = "object";
    public static final String TEXT_MESSAGE_TYPE = "text";
    public static final String JMS_SQS_MESSAGE_TYPE = "JMS_SQSMessageType";
    public static final String JMS_SQS_REPLY_TO_QUEUE_NAME = "JMS_SQSReplyToQueueName";
    public static final String JMS_SQS_REPLY_TO_QUEUE_URL = "JMS_SQSReplyToQueueURL";
    public static final String JMS_SQS_CORRELATION_ID = "JMS_SQSCorrelationID";

    // Default JMS Message properties
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timestamp;
    private boolean redelivered;
    private String correlationID;
    private long expiration = Message.DEFAULT_TIME_TO_LIVE;
    private String messageID;
    private String type;
    private SQSQueueDestination replyTo;
    private Destination destination;

    private final Map<String, JMSMessagePropertyValue> properties = new HashMap<>();

    private boolean writePermissionsForProperties;
    private boolean writePermissionsForBody;

    /**
     * Function for acknowledging message.
     */
    private Acknowledger acknowledger;

    /**
     * Original SQS Message ID.
     */
    private String sqsMessageID;

    /**
     * QueueUrl the message came from.
     */
    private String queueUrl;
    /**
     * Original SQS Message receipt handle.
     */
    private String receiptHandle;

    /**
     * This is called at the receiver side to create a
     * JMS message from the SQS message received.
     */
    SQSMessage(Acknowledger acknowledger, String queueUrl, software.amazon.awssdk.services.sqs.model.Message sqsMessage) throws JMSException {
        this.acknowledger = acknowledger;
        this.queueUrl = queueUrl;
        receiptHandle = sqsMessage.receiptHandle();
        this.setSQSMessageId(sqsMessage.messageId());
        Map<MessageSystemAttributeName, String> systemAttributes = sqsMessage.attributes();
        int receiveCount = Integer.parseInt(systemAttributes.get(MessageSystemAttributeName.fromValue(APPROXIMATE_RECEIVE_COUNT)));

        // JMSXDeliveryCount is set based on SQS ApproximateReceiveCount attribute.
        properties.put(JMSX_DELIVERY_COUNT, new JMSMessagePropertyValue(receiveCount, INT));
        if (receiveCount > 1) {
            setJMSRedelivered(true);
        }
        if (sqsMessage.messageAttributes() != null) {
            addMessageAttributes(sqsMessage);
        }

        // map the SequenceNumber, MessageGroupId and MessageDeduplicationId to JMS specific properties
        mapSystemAttributeToJmsMessageProperty(systemAttributes, SEQUENCE_NUMBER, JMS_SQS_SEQUENCE_NUMBER);
        mapSystemAttributeToJmsMessageProperty(systemAttributes, MESSAGE_DEDUPLICATION_ID, JMS_SQS_DEDUPLICATION_ID);
        mapSystemAttributeToJmsMessageProperty(systemAttributes, MESSAGE_GROUP_ID, JMSX_GROUP_ID);

        writePermissionsForBody = false;
        writePermissionsForProperties = false;
    }

    private void mapSystemAttributeToJmsMessageProperty(Map<MessageSystemAttributeName, String> systemAttributes,
                                                        String systemAttributeName, String jmsMessagePropertyName)
            throws JMSException {
        String systemAttributeValue = systemAttributes.get(MessageSystemAttributeName.fromValue(systemAttributeName));
        if (systemAttributeValue != null) {
            properties.put(jmsMessagePropertyName, new JMSMessagePropertyValue(systemAttributeValue, STRING));
        }
    }

    /**
     * Create new empty Message to send. SQSMessage cannot be sent without any
     * payload. One of SQSTextMessage, SQSObjectMessage, or SQSBytesMessage
     * should be used to add payload.
     */
    SQSMessage() {
        writePermissionsForBody = true;
        writePermissionsForProperties = true;
    }

    private void addMessageAttributes(software.amazon.awssdk.services.sqs.model.Message sqsMessage) throws JMSException {
        for (Entry<String, MessageAttributeValue> entry : sqsMessage.messageAttributes().entrySet()) {
            properties.put(entry.getKey(), new JMSMessagePropertyValue(
                    entry.getValue().stringValue(), entry.getValue().dataType()));
        }
    }

    protected void checkPropertyWritePermissions() throws JMSException {
        if (!writePermissionsForProperties) {
            throw new MessageNotWriteableException("Message properties are not writable");
        }
    }

    protected void checkBodyWritePermissions() throws JMSException {
        if (!writePermissionsForBody) {
            throw new MessageNotWriteableException("Message body is not writable");
        }
    }

    protected static JMSException convertExceptionToJMSException(Exception e) {
        JMSException ex = new JMSException(e.getMessage());
        ex.initCause(e);
        return ex;
    }

    protected static MessageFormatException convertExceptionToMessageFormatException(Exception e) {
        MessageFormatException ex = new MessageFormatException(e.getMessage());
        ex.initCause(e);
        return ex;
    }

    protected void setBodyWritePermissions(boolean enable) {
        writePermissionsForBody = enable;
    }

    /**
     * Get SQS Message Group Id (applicable for FIFO queues, available also as JMS property 'JMSXGroupId')
     *
     * @throws JMSException
     */
    public String getSQSMessageGroupId() throws JMSException {
        return getStringProperty(SQSMessagingClientConstants.JMSX_GROUP_ID);
    }

    /**
     * Get SQS Message Deduplication Id (applicable for FIFO queues, available also as JMS property 'JMS_SQS_DeduplicationId')
     *
     * @throws JMSException
     */
    public String getSQSMessageDeduplicationId() throws JMSException {
        return getStringProperty(SQSMessagingClientConstants.JMS_SQS_DEDUPLICATION_ID);
    }

    /**
     * Get SQS Message Sequence Number (applicable for FIFO queues, available also as JMS property 'JMS_SQS_SequenceNumber')
     *
     * @throws JMSException
     */
    public String getSQSMessageSequenceNumber() throws JMSException {
        return getStringProperty(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER);
    }

    /**
     * Get SQS Message Id.
     *
     * @return SQS Message Id.
     */
    public String getSQSMessageId() {
        return sqsMessageID;
    }

    /**
     * Set SQS Message Id, used on send.
     *
     * @param sqsMessageID messageId assigned by SQS during send.
     */
    public void setSQSMessageId(String sqsMessageID) throws JMSException {
        this.sqsMessageID = sqsMessageID;
        this.setJMSMessageID(String.format(SQSMessagingClientConstants.MESSAGE_ID_FORMAT, sqsMessageID));
    }

    /**
     * Get SQS Message receiptHandle.
     *
     * @return SQS Message receiptHandle.
     */
    public String getReceiptHandle() {
        return receiptHandle;
    }

    /**
     * Get queueUrl the message came from.
     *
     * @return queueUrl.
     */
    public String getQueueUrl() {
        return queueUrl;
    }

    /**
     * Gets the message ID.
     * <p>
     * The JMSMessageID header field contains a value that uniquely identifies
     * each message sent by a provider. It is set to SQS messageId with the
     * prefix 'ID:'.
     *
     * @return the ID of the message.
     */
    @Override
    public String getJMSMessageID() throws JMSException {
        return messageID;
    }

    /**
     * Sets the message ID. It should have prefix 'ID:'.
     * <p>
     * Set when a message is sent. This method can be used to change the value
     * for a message that has been received.
     *
     * @param id The ID of the message.
     */
    @Override
    public void setJMSMessageID(String id) throws JMSException {
        messageID = id;
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return correlationID != null ? correlationID.getBytes(DEFAULT_CHARSET) : null;
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        this.correlationID = correlationID != null ? new String(correlationID, StandardCharsets.UTF_8) : null;
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        this.correlationID = correlationID;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return correlationID;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        if (replyTo != null && !(replyTo instanceof SQSQueueDestination)) {
            throw new IllegalArgumentException("The replyTo Destination must be a SQSQueueDestination");
        }
        this.replyTo = (SQSQueueDestination) replyTo;
    }

    /**
     * Gets the Destination object for this message.
     * <p>
     * The JMSDestination header field contains the destination to which the
     * message is being sent.
     * <p>
     * When a message is sent, this field is ignored. After completion of the
     * send or publish method, the field holds the destination specified by the
     * method.
     * <p>
     * When a message is received, its JMSDestination value must be equivalent
     * to the value assigned when it was sent.
     *
     * @return The destination of this message.
     */
    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    /**
     * Sets the Destination object for this message.
     * <p>
     * Set when a message is sent. This method can be used to change the value
     * for a message that has been received.
     *
     * @param destination The destination for this message.
     */
    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.destination = destination;
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.redelivered = redelivered;
    }

    @Override
    public String getJMSType() throws JMSException {
        return type;
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        this.type = type;
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        this.expiration = expiration;
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        // FIXME
        return 0;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        // FIXME
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        this.priority = priority;
    }

    /**
     * Clears a message's properties and set the write permissions for
     * properties. The message's header fields and body are not cleared.
     */
    @Override
    public void clearProperties() throws JMSException {
        properties.clear();
        writePermissionsForProperties = true;
    }

    /**
     * Indicates whether a property value exists for the given property name.
     *
     * @param name The name of the property.
     * @return true if the property exists.
     */
    @Override
    public boolean propertyExists(String name) throws JMSException {
        return properties.containsKey(name);
    }

    /**
     * Get the value for a property that represents a java primitive(e.g. int or
     * long).
     *
     * @param property The name of the property to get.
     * @param type     The type of the property.
     * @return the converted value for the property.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   and NumberFormatException when property name or value is
     *                                null. Method throws same exception as primitives
     *                                corresponding valueOf(String) method.
     */
    <T> T getPrimitiveProperty(String property, Class<T> type) throws JMSException {
        if (property == null) {
            throw new NullPointerException("Property name is null");
        }
        Object value = getObjectProperty(property);
        if (value == null) {
            return handleNullPropertyValue(property, type);
        }
        T convertedValue = TypeConversionSupport.convert(value, type);
        if (convertedValue == null) {
            throw new MessageFormatException("Property " + property + " was " + value.getClass().getName() +
                    " and cannot be read as " + type.getName());
        }
        return convertedValue;
    }

    @SuppressWarnings("unchecked")
    private <T> T handleNullPropertyValue(String name, Class<T> clazz) {
        if (clazz == String.class) {
            return null;
        } else if (clazz == Boolean.class) {
            return (T) Boolean.FALSE;
        } else if (clazz == Double.class || clazz == Float.class) {
            throw new NullPointerException("Value of property with name " + name + " is null.");
        } else {
            throw new NumberFormatException("Value of property with name " + name + " is null.");
        }
    }

    /**
     * Returns the value of the <code>boolean</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>boolean</code> property value for the specified name.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name is null.
     */
    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Boolean.class);
    }

    /**
     * Returns the value of the <code>byte</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>byte</code> property value for the specified name.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name is null.
     * @throws NumberFormatException  When property value is null.
     */
    @Override
    public byte getByteProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Byte.class);
    }

    /**
     * Returns the value of the <code>short</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>short</code> property value for the specified name.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name is null.
     * @throws NumberFormatException  When property value is null.
     */
    @Override
    public short getShortProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Short.class);
    }

    /**
     * Returns the value of the <code>int</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>int</code> property value for the specified name.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name is null.
     * @throws NumberFormatException  When property value is null.
     */
    @Override
    public int getIntProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Integer.class);
    }

    /**
     * Returns the value of the <code>long</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>long</code> property value for the specified name.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name is null.
     * @throws NumberFormatException  When property value is null.
     */
    @Override
    public long getLongProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Long.class);
    }

    /**
     * Returns the value of the <code>float</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>float</code> property value for the specified name.
     * @throws JMSException           Wn internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name or value is null.
     */
    @Override
    public float getFloatProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Float.class);
    }

    /**
     * Returns the value of the <code>double</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>double</code> property value for the specified name.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name or value is null.
     */
    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Double.class);
    }

    /**
     * Returns the value of the <code>String</code> property with the specified
     * name.
     *
     * @param name The name of the property to get.
     * @return the <code>String</code> property value for the specified name.
     * @throws JMSException           On internal error.
     * @throws MessageFormatException If the property cannot be converted to the specified type.
     * @throws NullPointerException   When property name is null.
     */
    @Override
    public String getStringProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, String.class);
    }

    /**
     * Returns the value of the Java object property with the specified name.
     * <p>
     * This method can be used to return, in boxed format, an object that has
     * been stored as a property in the message with the equivalent
     * <code>setObjectProperty</code> method call, or its equivalent primitive
     * setter method.
     *
     * @param name The name of the property to get.
     * @return the Java object property value with the specified name, in boxed
     * format (for example, if the property was set as an
     * <code>int</code>, an <code>Integer</code> is returned); if there
     * is no property by this name, a null value is returned.
     * @throws JMSException On internal error.
     */
    @Override
    public Object getObjectProperty(String name) throws JMSException {
        JMSMessagePropertyValue propertyValue = getJMSMessagePropertyValue(name);
        if (propertyValue != null) {
            return propertyValue.getValue();
        }
        return null;
    }

    /**
     * Returns the property value with message attribute to object property
     * conversions took place.
     * <p>
     *
     * @param name The name of the property to get.
     * @return <code>JMSMessagePropertyValue</code> with object value and
     * corresponding SQS message attribute type and message attribute
     * string value.
     * @throws JMSException On internal error.
     */
    public JMSMessagePropertyValue getJMSMessagePropertyValue(String name) throws JMSException {
        return properties.get(name);
    }

    private record PropertyEnum(Iterator<String> propertyItr) implements Enumeration<String> {

        @Override
        public boolean hasMoreElements() {
            return propertyItr.hasNext();
        }

        @Override
        public String nextElement() {
            return propertyItr.next();
        }
    }

    /**
     * Returns an <code>Enumeration</code> of all the property names.
     * <p>
     * Note that JMS standard header fields are not considered properties and
     * are not returned in this enumeration.
     *
     * @return an enumeration of all the names of property values.
     * @throws JMSException On internal error.
     */
    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        return new PropertyEnum(properties.keySet().iterator());
    }

    /**
     * Sets a <code>boolean</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>boolean</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a <code>byte</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>byte</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a <code>short</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>short</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a <code>int</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>int</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a <code>long</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>long</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a <code>float</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>float</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a <code>double</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>double</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a <code>String</code> property value with the specified name into
     * the message.
     *
     * @param name  The name of the property to set.
     * @param value The <code>String</code> value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    /**
     * Sets a Java object property value with the specified name into the
     * message.
     * <p>
     * Note that this method works only for the boxed primitive object types
     * (Integer, Double, Long ...) and String objects.
     *
     * @param name  The name of the property to set.
     * @param value The object value of the property to set.
     * @throws JMSException                 On internal error.
     * @throws IllegalArgumentException     If the name or value is null or empty string.
     * @throws MessageFormatException       If the object is invalid type.
     * @throws MessageNotWriteableException If properties are read-only.
     */
    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Property name can not be null or empty.");
        }
        if (value == null || "".equals(value)) {
            throw new IllegalArgumentException("Property value can not be null or empty.");
        }
        if (!isValidPropertyValueType(value)) {
            throw new MessageFormatException("Value of property with name " + name + " has incorrect type " + value.getClass().getName() + ".");
        }
        checkPropertyWritePermissions();
        properties.put(name, new JMSMessagePropertyValue(value));
    }

    /**
     * <p>
     * Acknowledges message(s).
     * <p>
     * A client may individually acknowledge each message as it is consumed, or
     * it may choose to acknowledge multiple messages based on acknowledge mode,
     * which in turn might might acknowledge all messages consumed by the
     * session.
     * <p>
     * Messages that have been received but not acknowledged may be redelivered.
     * <p>
     * If the session is closed, messages cannot be acknowledged.
     * <p>
     * If only the consumer is closed, messages can still be acknowledged.
     *
     * @throws JMSException          On Internal error
     * @throws IllegalStateException If this method is called on a closed session.
     * @see com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode
     */
    @Override
    public void acknowledge() throws JMSException {
        if (acknowledger != null) {
            acknowledger.acknowledge(this);
        }
    }

    /**
     * <p>
     * Clears out the message body. Clearing a message's body does not clear its
     * header values or property entries.
     * <p>
     * This method cannot be called directly instead the implementation on the
     * subclasses should be used.
     *
     * @throws JMSException If directly called
     */
    @Override
    public void clearBody() throws JMSException {
        throw new JMSException("SQSMessage does not have any body");
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        throw new JMSException("SQSMessage does not have any body");
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        throw new JMSException("SQSMessage does not have any body");
    }

    private boolean isValidPropertyValueType(Object value) {
        return value instanceof Boolean || value instanceof Byte || value instanceof Short ||
                value instanceof Integer || value instanceof Long || value instanceof Float ||
                value instanceof Double || value instanceof String;
    }

    /**
     * Copied from org.apache.activemq.util.TypeConversionSupport to provide the
     * same property support provided by activemq without creating a dependency
     * on activemq.
     */
    public static class TypeConversionSupport {

        record ConversionKey(Class<?> from, Class<?> to) {

        }

        interface Converter {
            Object convert(Object value);
        }

        static final private Map<ConversionKey, Converter> CONVERSION_MAP = new HashMap<>();

        static {
            CONVERSION_MAP.put(new ConversionKey(Boolean.class, String.class), Object::toString);
            CONVERSION_MAP.put(new ConversionKey(Byte.class, String.class), Object::toString);
            CONVERSION_MAP.put(new ConversionKey(Short.class, String.class), Object::toString);
            CONVERSION_MAP.put(new ConversionKey(Integer.class, String.class), Object::toString);
            CONVERSION_MAP.put(new ConversionKey(Long.class, String.class), Object::toString);
            CONVERSION_MAP.put(new ConversionKey(Float.class, String.class), Object::toString);
            CONVERSION_MAP.put(new ConversionKey(Double.class, String.class), Object::toString);

            CONVERSION_MAP.put(new ConversionKey(String.class, Boolean.class), value -> {
                String stringValue = (String) value;
                if (Boolean.parseBoolean(stringValue) || INT_TRUE.equals(stringValue)) {
                    return Boolean.TRUE;
                }
                return Boolean.FALSE;
            });
            CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), value -> Byte.valueOf((String) value));
            CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), value -> Short.valueOf((String) value));
            CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), value -> Integer.valueOf((String) value));
            CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), value -> Long.valueOf((String) value));
            CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), value -> Float.valueOf((String) value));
            CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), value -> Double.valueOf((String) value));

            Converter longConverter = value -> ((Number) value).longValue();
            CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), longConverter);
            CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConverter);
            CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConverter);
            CONVERSION_MAP.put(new ConversionKey(Date.class, Long.class), value -> ((Date) value).getTime());

            Converter intConverter = value -> ((Number) value).intValue();
            CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), intConverter);
            CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), intConverter);

            CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), value -> ((Number) value).shortValue());

            CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), value -> ((Number) value).doubleValue());
        }

        @SuppressWarnings("unchecked")
        static public <T> T convert(Object value, Class<T> clazz) {
            assert value != null && clazz != null;

            if (value.getClass() == clazz)
                return (T) value;

            Converter c = CONVERSION_MAP.get(new ConversionKey(value.getClass(), clazz));
            if (c == null)
                return null;
            return (T) c.convert(value);

        }
    }

    /**
     * This class is used fulfill object value, corresponding SQS message
     * attribute type and message attribute string value.
     */
    public static class JMSMessagePropertyValue {

        private final Object value;

        private final String type;

        private final String stringMessageAttributeValue;

        public JMSMessagePropertyValue(String stringValue, String type) throws JMSException {
            this.type = type;
            this.value = getObjectValue(stringValue, type);
            this.stringMessageAttributeValue = stringValue;
        }

        public JMSMessagePropertyValue(Object value) throws JMSException {
            this.type = getType(value);
            this.value = value;
            if (BOOLEAN.equals(type)) {
                if ((Boolean) value) {
                    stringMessageAttributeValue = INT_TRUE;
                } else {
                    stringMessageAttributeValue = INT_FALSE;
                }
            } else {
                stringMessageAttributeValue = value.toString();
            }
        }

        public JMSMessagePropertyValue(Object value, String type) throws JMSException {
            this.value = value;
            this.type = type;
            if (BOOLEAN.equals(type)) {
                if ((Boolean) value) {
                    stringMessageAttributeValue = INT_TRUE;
                } else {
                    stringMessageAttributeValue = INT_FALSE;
                }
            } else {
                stringMessageAttributeValue = value.toString();
            }
        }

        private static String getType(Object value) throws JMSException {
            if (value instanceof String) {
                return STRING;
            } else if (value instanceof Integer) {
                return INT;
            } else if (value instanceof Long) {
                return LONG;
            } else if (value instanceof Boolean) {
                return BOOLEAN;
            } else if (value instanceof Byte) {
                return BYTE;
            } else if (value instanceof Double) {
                return DOUBLE;
            } else if (value instanceof Float) {
                return FLOAT;
            } else if (value instanceof Short) {
                return SHORT;
            } else {
                throw new JMSException("Not a supported JMS property type");
            }
        }

        private static Object getObjectValue(String value, String type) throws JMSException {
            if (INT.equals(type)) {
                return Integer.valueOf(value);
            } else if (LONG.equals(type)) {
                return Long.valueOf(value);
            } else if (BOOLEAN.equals(type)) {
                if (INT_TRUE.equals(value)) {
                    return Boolean.TRUE;
                }
                return Boolean.FALSE;
            } else if (BYTE.equals(type)) {
                return Byte.valueOf(value);
            } else if (DOUBLE.equals(type)) {
                return Double.valueOf(value);
            } else if (FLOAT.equals(type)) {
                return Float.valueOf(value);
            } else if (SHORT.equals(type)) {
                return Short.valueOf(value);
            } else if (type != null &&
                    (type.startsWith(STRING) || type.startsWith(NUMBER) || type.startsWith(BINARY))) {
                return value;
            } else {
                LOG.debug("Caught exception while constructing message attribute. " +
                        "Unknown or yet supported JMS property type - " + type);
                throw new JMSException(type + " is not a supported JMS property type");
            }
        }

        public String getType() {
            return type;
        }

        public Object getValue() {
            return value;
        }

        public String getStringMessageAttributeValue() {
            return stringMessageAttributeValue;
        }
    }


    /**
     * This method sets the JMS_SQS_SEQUENCE_NUMBER property on the message. It is exposed explicitly here, so that
     * it can be invoked even on read-only message object obtained through receiving a message.
     * This support the use case of send a received message by using the same JMSMessage object.
     *
     * @param sequenceNumber Sequence number to set. If null or empty, the stored sequence number will be removed.
     * @throws JMSException
     */
    public void setSequenceNumber(String sequenceNumber) throws JMSException {
        if (sequenceNumber == null || sequenceNumber.isEmpty()) {
            properties.remove(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER);
        } else {
            properties.put(SQSMessagingClientConstants.JMS_SQS_SEQUENCE_NUMBER, new JMSMessagePropertyValue(sequenceNumber));
        }
    }

    /*
     * Unit Test Utility Functions
     */
    void setWritePermissionsForProperties(boolean writePermissionsForProperties) {
        this.writePermissionsForProperties = writePermissionsForProperties;
    }
}
