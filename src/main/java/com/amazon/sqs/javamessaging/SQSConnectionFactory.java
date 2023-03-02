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

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.QueueConnection;
import jakarta.jms.QueueConnectionFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import java.util.function.Supplier;

/**
 * A ConnectionFactory object encapsulates a set of connection configuration
 * parameters for <code>SqsClient</code> as well as setting
 * <code>numberOfMessagesToPrefetch</code>.
 * <p>
 * The <code>numberOfMessagesToPrefetch</code> parameter is used to size of the
 * prefetched messages, which can be tuned based on the application workload. It
 * helps in returning messages from internal buffers(if there is any) instead of
 * waiting for the SQS <code>receiveMessage</code> call to return.
 * <p>
 * If more physical connections than the default maximum value (that is 50 as of
 * today) are needed on the connection pool,
 * {@link software.amazon.awssdk.core.client.config.ClientOverrideConfiguration} needs to be configured.
 * <p>
 * None of the <code>createConnection</code> methods set-up the physical
 * connection to SQS, so validity of credentials are not checked with those
 * methods.
 */

public class SQSConnectionFactory implements ConnectionFactory, QueueConnectionFactory {
    private final ProviderConfiguration providerConfiguration;
    private final Supplier<SqsClient> amazonSQSClientSupplier;

    /**
     * Creates a SQSConnectionFactory that uses SqsClientBuilder.standard() for creating SqsClient connections.
     * Every SQSConnection will have its own copy of SqsClient.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration) {
        this(providerConfiguration, SqsClient.create());
    }

    /**
     * Creates a SQSConnectionFactory that uses the provided SqsClient connection.
     * Every SQSConnection will use the same provided SqsClient.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration, final SqsClient client) {
        if (providerConfiguration == null) {
            throw new IllegalArgumentException("Provider configuration cannot be null");
        }
        if (client == null) {
            throw new IllegalArgumentException("AmazonSQS client cannot be null");
        }
        this.providerConfiguration = providerConfiguration;
        this.amazonSQSClientSupplier = () -> client;
    }

    /**
     * Creates a SQSConnectionFactory that uses the provided SqsClientBuilder for creating AmazonSQS client connections.
     * Every SQSConnection will have its own copy of AmazonSQS client created through the provided builder.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration, final SqsClientBuilder clientBuilder) {
        if (providerConfiguration == null) {
            throw new IllegalArgumentException("Provider configuration cannot be null");
        }
        if (clientBuilder == null) {
            throw new IllegalArgumentException("AmazonSQS client builder cannot be null");
        }
        this.providerConfiguration = providerConfiguration;
        this.amazonSQSClientSupplier = clientBuilder::build;
    }


    @Override
    public SQSConnection createConnection() throws JMSException {
        try {
            SqsClient amazonSQS = amazonSQSClientSupplier.get();
            return createConnection(amazonSQS, null);
        } catch (RuntimeException e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }

    @Override
    public SQSConnection createConnection(String awsAccessKeyId, String awsSecretKey) throws JMSException {
        AwsBasicCredentials basicAWSCredentials = AwsBasicCredentials.create(awsAccessKeyId, awsSecretKey);
        return createConnection(basicAWSCredentials);
    }

    @Override
    public JMSContext createContext() {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    public SQSConnection createConnection(AwsCredentials awsCredentials) throws JMSException {
        AwsCredentialsProvider awsCredentialsProvider = StaticCredentialsProvider.create(awsCredentials);
        return createConnection(awsCredentialsProvider);
    }

    public SQSConnection createConnection(AwsCredentialsProvider awsCredentialsProvider) throws JMSException {
        try {
            SqsClient amazonSQS = amazonSQSClientSupplier.get();
            return createConnection(amazonSQS, awsCredentialsProvider);
        } catch (Exception e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }

    private SQSConnection createConnection(SqsClient amazonSQS, AwsCredentialsProvider awsCredentialsProvider) throws JMSException {
        AmazonSQSMessagingClientWrapper amazonSQSClientJMSWrapper = new AmazonSQSMessagingClientWrapper(amazonSQS, awsCredentialsProvider);
        return new SQSConnection(amazonSQSClientJMSWrapper, providerConfiguration.getNumberOfMessagesToPrefetch());
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return createConnection(userName, password);
    }
}
