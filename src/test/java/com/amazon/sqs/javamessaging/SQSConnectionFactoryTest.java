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

import javax.jms.JMSException;

import org.junit.Test;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;


public class SQSConnectionFactoryTest {
    
    @Test
    public void canCreateFactoryWithDefaultProviderSettings() throws JMSException {
        new SQSConnectionFactory(new ProviderConfiguration());
        //cannot actually attempt to create a connection because the default client builder depends on environment settings or instance configuration to be present
        //which we cannot guarantee on the builder fleet
    }
    
    @Test
    public void canCreateFactoryWithCustomClient() throws JMSException {
        SqsClient client = mock(SqsClient.class);
        SQSConnectionFactory factory = new SQSConnectionFactory(new ProviderConfiguration(), client);
        SQSConnection connection = factory.createConnection();
        connection.close();
    }
    
    @Test
    public void factoryWithCustomClientWillUseTheSameClient() throws JMSException {
    	SqsClient client = mock(SqsClient.class);
        SQSConnectionFactory factory = new SQSConnectionFactory(new ProviderConfiguration(), client);
        SQSConnection connection1 = factory.createConnection();
        SQSConnection connection2 = factory.createConnection();
        
        assertSame(client, connection1.getAmazonSQSClient()); 
        assertSame(client, connection2.getAmazonSQSClient()); 
        assertSame(connection1.getAmazonSQSClient(), connection2.getAmazonSQSClient()); 
        
        connection1.close();
        connection2.close();
    }
    
    @Test
    public void canCreateFactoryWithCustomBuilder() throws JMSException {
    	SqsClientBuilder clientBuilder = SqsClient.builder().region(Region.US_EAST_1);
        SQSConnectionFactory factory = new SQSConnectionFactory(new ProviderConfiguration(), clientBuilder);
        SQSConnection connection = factory.createConnection();
        connection.close();
    }
    
    @Test
    public void factoryWithCustomBuilderWillCreateNewClient() throws JMSException {
    	SqsClientBuilder clientBuilder = SqsClient.builder().region(Region.US_EAST_1);
        SQSConnectionFactory factory = new SQSConnectionFactory(new ProviderConfiguration(), clientBuilder);
        SQSConnection connection1 = factory.createConnection();
        SQSConnection connection2 = factory.createConnection();
        
        assertNotSame(connection1.getAmazonSQSClient(), connection2.getAmazonSQSClient()); 
        
        connection1.close();
        connection2.close();
    }
}
