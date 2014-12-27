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

import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;


public class SQSMessagingClientConstants {

    public static final String UNSUPPORTED_METHOD = "Unsupported Method";

    public static final ConnectionMetaData CONNECTION_METADATA = new SQSConnectionMetaData();

    public static final int MAX_BATCH = 10;
    
    public static final int MIN_BATCH = 1;

    /**
     * JMSMessage available user property types, which are mapped to message
     * attribute data types
     */
    public static final String STRING = "String";

    public static final String INT = "Number.int";

    public static final String BOOLEAN = "Number.Boolean";

    public static final String BYTE = "Number.byte";

    public static final String DOUBLE = "Number.double";

    public static final String FLOAT = "Number.float";

    public static final String LONG = "Number.long";

    public static final String SHORT = "Number.short";

    public static final String INT_FALSE = "0";

    public static final String INT_TRUE = "1";
    
    public static final String MESSAGE_ID_FORMAT = "ID:%s"; 

    public static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";
    
    public static final String JMSX_GROUP_ID = "JMSXGroupID";
    
    public static final String JMSX_GROUP_SEC = "JMSXGroupSeq";

    public static final String APPROXIMATE_RECEIVE_COUNT = "ApproximateReceiveCount";

    static final String APPENDED_USER_AGENT_HEADER_VERSION;
    static {
        try {
            APPENDED_USER_AGENT_HEADER_VERSION = String.format(
                    "/SQS Java Messaging Client v%s", CONNECTION_METADATA.getProviderVersion());
        } catch (JMSException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
