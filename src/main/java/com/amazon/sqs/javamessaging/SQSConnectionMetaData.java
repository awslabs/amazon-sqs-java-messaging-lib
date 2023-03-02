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
package com.amazon.sqs.javamessaging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import jakarta.jms.ConnectionMetaData;
import jakarta.jms.JMSException;


public class SQSConnectionMetaData implements ConnectionMetaData {
    
    private final String jmsVersion;
    private final int jmsMajorVersion;
    private final int jmsMinorVersion;
    
    private final String jmsProviderName;
    private final String providerVersion;
    private final int providerMajorVersion;
    private final int providerMinorVersion;
    
    private final List<String> jmsxProperty;
    
    SQSConnectionMetaData() {
        this.jmsVersion = "3.1";
        this.jmsMajorVersion = 3;
        this.jmsMinorVersion = 1;
        
        this.jmsProviderName = "Amazon";
        this.providerVersion = "1.0";
        this.providerMajorVersion = 1;
        this.providerMinorVersion = 0;
        
        this.jmsxProperty = new ArrayList<>();
        jmsxProperty.add(SQSMessagingClientConstants.JMSX_DELIVERY_COUNT);
        jmsxProperty.add(SQSMessagingClientConstants.JMSX_GROUP_ID);
        jmsxProperty.add(SQSMessagingClientConstants.JMSX_GROUP_SEC);
    }

    @Override
    public String getJMSVersion() throws JMSException {
        return jmsVersion;
    }

    @Override
    public int getJMSMajorVersion() throws JMSException {
        return jmsMajorVersion;
    }

    @Override
    public int getJMSMinorVersion() throws JMSException {
        return jmsMinorVersion;
    }

    @Override
    public String getJMSProviderName() throws JMSException {
        return jmsProviderName;
    }

    @Override
    public String getProviderVersion() throws JMSException {
        return providerVersion;
    }

    @Override
    public int getProviderMajorVersion() throws JMSException {
        return providerMajorVersion;
    }

    @Override
    public int getProviderMinorVersion() throws JMSException {
        return providerMinorVersion;
    }

    @Override
    public Enumeration<String> getJMSXPropertyNames() throws JMSException {
        return Collections.enumeration(jmsxProperty);
    }    
}
