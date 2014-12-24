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

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.sqsjms.acknowledge.Acknowledger;

import static com.amazonaws.sqsjms.SQSJMSClientConstants.*;

/**
 * The SQSMessage is the root class of all SQS JMS messages and implements JMS
 * Message interface.
 * <P>
 * Not all message headers are supported at this time:
 * <ul>
 * <li><code>JMSMessageID</code> is always assigned as SQS provided message id.</li>
 * <li><code>JMSRedelivered</code> is set to true if SQS delivers the message
 * more than once. This not necessarily mean that the user received message more
 * than once, but rather SQS attempted to deliver it more than once. Due to
 * prefetching used in {@link SQSMessageConsumerPrefetch}, this can be set to
 * true although user never received the message. This is set based on SQS
 * ApproximateReceiveCount attribute</li>
 * <li><code>JMSDestination</code></li> is the destination object which message
 * is sent to and received from.
 * </ul>
 * </P>
 * <P>
 * JMSXDeliveryCount reserved property is supported and set based on the
 * approximate receive count observed on the SQS side.
 */
public class SQSMessage implements Message {
        
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    
    // Define constant message types.
    public static final String BYTE_MESSAGE_TYPE = "byte";
    public static final String OBJECT_MESSAGE_TYPE = "object";
    public static final String TEXT_MESSAGE_TYPE = "text";
    public static final String JMS_SQS_MESSAGE_TYPE = "JMS_SQSMessageType";
    
    // Default JMS Message properties
    private int deliveryMode = Message.DEFAULT_DELIVERY_MODE;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timestamp;
    private boolean redelivered;
    private String correlationID;
    private long expiration = Message.DEFAULT_TIME_TO_LIVE;
    private String messageID;
    private String type;
    private Destination replyTo;
    private Destination destination;

    private final Map<String, JMSMessagePropertyValue> properties = new HashMap<String, JMSMessagePropertyValue>();

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
    SQSMessage(Acknowledger acknowledger, String queueUrl, com.amazonaws.services.sqs.model.Message sqsMessage) throws JMSException{
        this.acknowledger = acknowledger;
        this.queueUrl = queueUrl;
        receiptHandle = sqsMessage.getReceiptHandle();
        sqsMessageID = sqsMessage.getMessageId();
        this.messageID = String.format(MESSAGE_ID_FORMAT,sqsMessageID);
        Map<String,String> systemAttributes = sqsMessage.getAttributes();
        int receiveCount = Integer.parseInt(systemAttributes.get(APPROXIMATE_RECEIVE_COUNT));
        
        /**
         * JMSXDeliveryCount is set based on SQS ApproximateReceiveCount
         * attribute
         */
        properties.put(JMSX_DELIVERY_COUNT, new JMSMessagePropertyValue(
                receiveCount, INT));
        if (receiveCount > 1) {
            setJMSRedelivered(true);
        }
        if (sqsMessage.getMessageAttributes() != null) {
            addMessageAttributes(sqsMessage);
        }
        
        writePermissionsForBody = false;
        writePermissionsForProperties = false;
    }

    /**
     * Create new empty Message to send. SQSMessage cannot be sent without any
     * payload. You must use one of SQSTextMessage, SQSObjectMessage, or
     * SQSBytesMessage to add payload.
     */
    SQSMessage() {
        writePermissionsForBody = true;
        writePermissionsForProperties = true;
    }

    private void addMessageAttributes(com.amazonaws.services.sqs.model.Message sqsMessage) throws JMSException {
        for (Entry<String, MessageAttributeValue> entry : sqsMessage.getMessageAttributes().entrySet()) {
            properties.put(entry.getKey(), new JMSMessagePropertyValue(
                    entry.getValue().getStringValue(), entry.getValue().getDataType()));
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
     * Get SQS Message Id
     * 
     * @return SQS Message Id
     */
    public String getSQSMessageId() {
        return sqsMessageID;
    }
    
    /**
     * Set SQS Message Id, used on send.
     * 
     * @param sqsMessageID
     *            messageId assigned by SQS during send
     */
    public void setSQSMessageId(String sqsMessageID) throws JMSException {
        this.sqsMessageID = sqsMessageID;
    }
    
    /**
     * Get original SQS Message receiptHandle.
     */
    public String getReceiptHandle() {
        return receiptHandle;
    }

    /**
     * Get queueUrl the message came from.
     */
    public String getQueueUrl() {
        return queueUrl;
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return messageID;
    }

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
        try {
            this.correlationID = correlationID != null ? new String(correlationID, "UTF-8") : null;
        } catch (UnsupportedEncodingException e) {
            throw new JMSException(e.getMessage());
        }
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
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

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
    public int getJMSPriority() throws JMSException {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        this.priority = priority;
    }

    @Override
    public void clearProperties() throws JMSException {
        properties.clear();
        writePermissionsForProperties = true;
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return properties.containsKey(name);
    }

    /**
     * Get the value for a property that represents a java primitive(e.g. int or long).
     *
     * @param property the name of the property to get
     * @param type the type of the property.
     *
     * @return the converted value for the property
     *
     * @throws JMSException on internal error
     * @throws MessageFormatException if the property cannot be converted to the specified type
     * @throws NullPointerException and NumberFormatException when property is null. Method throws
     *         same exception as primitives corresponding valueOf(String) method.
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

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Boolean.class);
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Byte.class);
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Short.class);
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Integer.class);
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Long.class);
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Float.class);
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, Double.class);
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return getPrimitiveProperty(name, String.class);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        JMSMessagePropertyValue propertyValue = getJMSMessagePropertyValue(name);
        if (propertyValue != null) {
            return propertyValue.getValue();
        }
        return null;
    }
    
    public JMSMessagePropertyValue getJMSMessagePropertyValue(String name) throws JMSException {
        return properties.get(name);
    }

    private static class PropertyEnum implements Enumeration<String> {
        private final Iterator<String> propertyItr;

        public PropertyEnum(Iterator<String> propertyItr) {
            this.propertyItr = propertyItr;
        }

        @Override
        public boolean hasMoreElements() {
            return propertyItr.hasNext();
        }

        @Override
        public String nextElement() {
            return propertyItr.next();
        }
    }

    @Override
    public Enumeration<String> getPropertyNames() throws JMSException {
        return new PropertyEnum(properties.keySet().iterator());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Property name can not be null or empty.");
        }
        if (value == null || "".equals(value)) {
            throw new IllegalArgumentException("Property value can not be null or empty.");
        }
        if(!isValidPropertyValueType(value)) {
            throw new MessageFormatException("Value of property with name " + name + " has incorrect type " + value.getClass().getName() + ".");
        }
        checkPropertyWritePermissions();
        properties.put(name, new JMSMessagePropertyValue(value));
    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledger != null) {
            acknowledger.acknowledge(this);
        }
    }

    @Override
    public void clearBody() throws JMSException {
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

        static class ConversionKey {
            final Class<?> from;

            final Class<?> to;

            public ConversionKey(Class<?> from, Class<?> to) {
                this.from = from;
                this.to = to;
            }
            
            @Override
            public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((from == null) ? 0 : from.hashCode());
                result = prime * result + ((to == null) ? 0 : to.hashCode());
                return result;
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (getClass() != obj.getClass())
                    return false;
                ConversionKey other = (ConversionKey) obj;
                if (from == null) {
                    if (other.from != null)
                        return false;
                } else if (!from.equals(other.from))
                    return false;
                if (to == null) {
                    if (other.to != null)
                        return false;
                } else if (!to.equals(other.to))
                    return false;
                return true;
            }
        }

        interface Converter {
            Object convert(Object value);
        }

        static final private Map<ConversionKey, Converter> CONVERSION_MAP = new HashMap<ConversionKey, Converter>();
        static {
            Converter toStringConverter = new Converter() {
                public Object convert(Object value) {
                    return value.toString();
                }
            };
            CONVERSION_MAP.put(new ConversionKey(Boolean.class, String.class), toStringConverter);
            CONVERSION_MAP.put(new ConversionKey(Byte.class, String.class), toStringConverter);
            CONVERSION_MAP.put(new ConversionKey(Short.class, String.class), toStringConverter);
            CONVERSION_MAP.put(new ConversionKey(Integer.class, String.class), toStringConverter);
            CONVERSION_MAP.put(new ConversionKey(Long.class, String.class), toStringConverter);
            CONVERSION_MAP.put(new ConversionKey(Float.class, String.class), toStringConverter);
            CONVERSION_MAP.put(new ConversionKey(Double.class, String.class), toStringConverter);

            CONVERSION_MAP.put(new ConversionKey(String.class, Boolean.class), new Converter() {
                public Object convert(Object value) {
                    String stringValue = (String) value;
                    if (Boolean.valueOf(stringValue) || INT_TRUE.equals((String) value)) {
                        return Boolean.TRUE;
                    }
                    return Boolean.FALSE;
                }
            });
            CONVERSION_MAP.put(new ConversionKey(String.class, Byte.class), new Converter() {
                public Object convert(Object value) {
                    return Byte.valueOf((String) value);
                }
            });
            CONVERSION_MAP.put(new ConversionKey(String.class, Short.class), new Converter() {
                public Object convert(Object value) {
                    return Short.valueOf((String) value);
                }
            });
            CONVERSION_MAP.put(new ConversionKey(String.class, Integer.class), new Converter() {
                public Object convert(Object value) {
                    return Integer.valueOf((String) value);
                }
            });
            CONVERSION_MAP.put(new ConversionKey(String.class, Long.class), new Converter() {
                public Object convert(Object value) {
                    return Long.valueOf((String) value);
                }
            });
            CONVERSION_MAP.put(new ConversionKey(String.class, Float.class), new Converter() {
                public Object convert(Object value) {
                    return Float.valueOf((String) value);
                }
            });
            CONVERSION_MAP.put(new ConversionKey(String.class, Double.class), new Converter() {
                public Object convert(Object value) {
                    return Double.valueOf((String) value);
                }
            });

            Converter longConverter = new Converter() {
                public Object convert(Object value) {
                    return Long.valueOf(((Number) value).longValue());
                }
            };
            CONVERSION_MAP.put(new ConversionKey(Byte.class, Long.class), longConverter);
            CONVERSION_MAP.put(new ConversionKey(Short.class, Long.class), longConverter);
            CONVERSION_MAP.put(new ConversionKey(Integer.class, Long.class), longConverter);
            CONVERSION_MAP.put(new ConversionKey(Date.class, Long.class), new Converter() {
                public Object convert(Object value) {
                    return Long.valueOf(((Date) value).getTime());
                }
            });

            Converter intConverter = new Converter() {
                public Object convert(Object value) {
                    return Integer.valueOf(((Number) value).intValue());
                }
            };
            CONVERSION_MAP.put(new ConversionKey(Byte.class, Integer.class), intConverter);
            CONVERSION_MAP.put(new ConversionKey(Short.class, Integer.class), intConverter);

            CONVERSION_MAP.put(new ConversionKey(Byte.class, Short.class), new Converter() {
                public Object convert(Object value) {
                    return Short.valueOf(((Number) value).shortValue());
                }
            });

            CONVERSION_MAP.put(new ConversionKey(Float.class, Double.class), new Converter() {
                public Object convert(Object value) {
                    return Double.valueOf(((Number) value).doubleValue());
                }
            });
        }

        @SuppressWarnings("unchecked")
        static public <T> T convert(Object value, Class<T> clazz) {

            assert value != null && clazz != null;

            if (value.getClass() == clazz)
                return (T) value;

            Converter c = (Converter) CONVERSION_MAP.get(new ConversionKey(value.getClass(), clazz));
            if (c == null)
                return null;
            return (T) c.convert(value);

        }
    }
    
    static class JMSMessagePropertyValue {
        
        private final Object value;

        private final String type;
        
        private final String stringMessageAttributeValue;
        
        public JMSMessagePropertyValue(String stringValue, String type) throws JMSException{
            this.type = type;
            this.value = getObjectValue(stringValue, type);
            this.stringMessageAttributeValue = stringValue;
        }
        
        public JMSMessagePropertyValue(Object value) throws JMSException {
            this.type = getType(value);
            this.value = value;
            if (BOOLEAN.equals(type)) {
                if((Boolean) value) {
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
                if((Boolean) value) {
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
            if (STRING.equals(type)) {
                return value;
            } else if (INT.equals(type)) {
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
            } else {
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

    /*
     * Unit Test Utility Functions
     */
    void setWritePermissionsForProperties(boolean writePermissionsForProperties) {
        this.writePermissionsForProperties = writePermissionsForProperties;
    }
}
