/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

/**
 * A ConnectionFactory object encapsulates a set of connection configuration
 * parameters for <code>AmazonSQSClient</code> as well as setting
 * <code>numberOfMessagesToPrefetch</code>.
 * <P>
 * The <code>numberOfMessagesToPrefetch</code> parameter is used to size of the
 * prefetched messages, which can be tuned based on the application workload. It
 * helps in returning messages from internal buffers(if there is any) instead of
 * waiting for the SQS <code>receiveMessage</code> call to return.
 * <P>
 * If more physical connections than the default maximum value (that is 50 as of
 * today) are needed on the connection pool,
 * {@link com.amazonaws.ClientConfiguration} needs to be configured.
 * <P>
 * None of the <code>createConnection</code> methods set-up the physical
 * connection to SQS, so validity of credentials are not checked with those
 * methods.
 */

public class SQSConnectionFactory implements ConnectionFactory, QueueConnectionFactory {
    private final ProviderConfiguration providerConfiguration;
    private final AmazonSQSClientSupplier amazonSQSClientSupplier;
    
    /*
     * At the time when the library will stop supporting Java 7, this can be removed and Supplier<T> from Java 8 can be used directly.
     */
    private interface AmazonSQSClientSupplier {
        AmazonSQS get();
    }

    /*
     * Creates a SQSConnectionFactory that uses AmazonSQSClientBuilder.standard() for creating AmazonSQS client connections.
     * Every SQSConnection will have its own copy of AmazonSQS client.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration) {
        this(providerConfiguration, AmazonSQSClientBuilder.standard());
    }
    
    /*
     * Creates a SQSConnectionFactory that uses the provided AmazonSQS client connection.
     * Every SQSConnection will use the same provided AmazonSQS client.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration, final AmazonSQS client) {
        if (providerConfiguration == null) {
            throw new IllegalArgumentException("Provider configuration cannot be null");
        }
        if (client == null) {
            throw new IllegalArgumentException("AmazonSQS client cannot be null");
        }
        this.providerConfiguration = providerConfiguration;
        this.amazonSQSClientSupplier = new AmazonSQSClientSupplier() {
            @Override
            public AmazonSQS get() {
                return client;
            }
        };
    }
    
    /*
     * Creates a SQSConnectionFactory that uses the provided AmazonSQSClientBuilder for creating AmazonSQS client connections.
     * Every SQSConnection will have its own copy of AmazonSQS client created through the provided builder.
     */
    public SQSConnectionFactory(ProviderConfiguration providerConfiguration, final AmazonSQSClientBuilder clientBuilder) {
        if (providerConfiguration == null) {
            throw new IllegalArgumentException("Provider configuration cannot be null");
        }
        if (clientBuilder == null) {
            throw new IllegalArgumentException("AmazonSQS client builder cannot be null");
        }
        this.providerConfiguration = providerConfiguration;
        this.amazonSQSClientSupplier = new AmazonSQSClientSupplier() {
            @Override
            public AmazonSQS get() {
                return clientBuilder.build();
            }
        };
    }
    
    private SQSConnectionFactory(final Builder builder) {
        this.providerConfiguration = builder.providerConfiguration;
        this.amazonSQSClientSupplier = new AmazonSQSClientSupplier() {
            @Override
            public AmazonSQS get() {
                AmazonSQSClient amazonSQSClient = new AmazonSQSClient(builder.awsCredentialsProvider, builder.clientConfiguration);
                if (builder.region != null) {
                    amazonSQSClient.setRegion(builder.region);
                }
                if (builder.endpoint != null) {
                    amazonSQSClient.setEndpoint(builder.endpoint);
                }
                if (builder.signerRegionOverride != null) {
                    amazonSQSClient.setSignerRegionOverride(builder.signerRegionOverride);
                }
                return amazonSQSClient;
            }
        };
    }

    @Override
    public SQSConnection createConnection() throws JMSException {
        try {
            AmazonSQS amazonSQS = amazonSQSClientSupplier.get();
            return createConnection(amazonSQS, null);
        } catch (RuntimeException e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }

    @Override
    public SQSConnection createConnection(String awsAccessKeyId, String awsSecretKey) throws JMSException {
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey);
        return createConnection(basicAWSCredentials);
    }

    public SQSConnection createConnection(AWSCredentials awsCredentials) throws JMSException {
        AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        return createConnection(awsCredentialsProvider);
    }
    
    public SQSConnection createConnection(AWSCredentialsProvider awsCredentialsProvider) throws JMSException {
        try {
            AmazonSQS amazonSQS = amazonSQSClientSupplier.get();
            return createConnection(amazonSQS, awsCredentialsProvider);
        } catch(Exception e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }
    
    private SQSConnection createConnection(AmazonSQS amazonSQS, AWSCredentialsProvider awsCredentialsProvider) throws JMSException {
        AmazonSQSMessagingClientWrapper amazonSQSClientJMSWrapper = new AmazonSQSMessagingClientWrapper(amazonSQS, awsCredentialsProvider);
        return new SQSConnection(amazonSQSClientJMSWrapper, providerConfiguration.getNumberOfMessagesToPrefetch());
    }
    
    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection) createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection) createConnection(userName, password);
    }
    
    /**
     * Deprecated. Use one of the constructors of this class instead and provide either AmazonSQS client or AmazonSQSClientBuilder.
     * @return
     */
    @Deprecated
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Deprecated. Use one of the constructors of SQSConnectionFactory instead.
     * @return
     */
    @Deprecated
    public static class Builder {
        private Region region;
        private String endpoint;
        private String signerRegionOverride;
        private ClientConfiguration clientConfiguration;
        private AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        private ProviderConfiguration providerConfiguration;
        
        public Builder(Region region) {
            this();
            this.region = region;
        }

        /** Recommended way to set the AmazonSQSClient is to use region */
        public Builder(String region) {
            this(Region.getRegion(Regions.fromName(region)));
        }

        public Builder() {
            providerConfiguration = new ProviderConfiguration();
            clientConfiguration = new ClientConfiguration();
        }
        
        public Builder withRegion(Region region) {
            setRegion(region);
            return this;
        }
        
        public Builder withRegionName(String regionName) 
            throws IllegalArgumentException
        {
            setRegion(Region.getRegion( Regions.fromName(regionName) ) );
            return this;
        }
        
        public Builder withEndpoint(String endpoint) {
            setEndpoint(endpoint);
            return this;
        }
        
        /**
         * An internal method used to explicitly override the internal signer region
         * computed by the default implementation. This method is not expected to be
         * normally called except for AWS internal development purposes.
         */
        public Builder withSignerRegionOverride(String signerRegionOverride) {
            setSignerRegionOverride(signerRegionOverride);
            return this;
        }
        
        public Builder withAWSCredentialsProvider(AWSCredentialsProvider awsCredentialsProvider) {
            setAwsCredentialsProvider(awsCredentialsProvider);
            return this;
        }

        public Builder withClientConfiguration(ClientConfiguration clientConfig) {
            setClientConfiguration(clientConfig);
            return this;
        }

        public Builder withNumberOfMessagesToPrefetch(int numberOfMessagesToPrefetch) {
            providerConfiguration.setNumberOfMessagesToPrefetch(numberOfMessagesToPrefetch);
            return this;
        }

        public SQSConnectionFactory build() {
            return new SQSConnectionFactory(this);
        }

        public Region getRegion() {
            return region;
        }

        public void setRegion(Region region) {
            this.region = region;
            this.endpoint = null;
        }
        
        public void setRegionName(String regionName) {
            setRegion( Region.getRegion( Regions.fromName( regionName ) ) );
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            this.region = null;
        }

        public String getSignerRegionOverride() {
            return signerRegionOverride;
        }

        public void setSignerRegionOverride(String signerRegionOverride) {
            this.signerRegionOverride = signerRegionOverride;
        }

        public ClientConfiguration getClientConfiguration() {
            return clientConfiguration;
        }

        public void setClientConfiguration(ClientConfiguration clientConfig) {
            clientConfiguration = clientConfig;
        }

        public int getNumberOfMessagesToPrefetch() {
            return providerConfiguration.getNumberOfMessagesToPrefetch();
        }

        public void setNumberOfMessagesToPrefetch(int numberOfMessagesToPrefetch) {
            providerConfiguration.setNumberOfMessagesToPrefetch(numberOfMessagesToPrefetch);
        }

        public AWSCredentialsProvider getAwsCredentialsProvider() {
            return awsCredentialsProvider;
        }

        public void setAwsCredentialsProvider(
                AWSCredentialsProvider awsCredentialsProvider) {
            this.awsCredentialsProvider = awsCredentialsProvider;
        }
    }
    
}
