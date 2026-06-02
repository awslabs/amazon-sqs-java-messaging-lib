package com.amazon.sqs.javamessaging;

import jakarta.jms.JMSException;
import jakarta.jms.QueueConnection;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

import java.util.function.Supplier;

public class SQSAsyncConnectionFactory extends AbstractSQSConnectionFactory {
    private final ProviderConfiguration providerConfiguration;
    private final Supplier<SqsAsyncClient> amazonSQSClientSupplier;

    /**
     * Creates a SQSConnectionFactory that uses default ProviderConfiguration
     * and SqsClientAsyncBuilder.standard() for creating SqsAsyncClient connections.
     * Every SQSConnection will have its own copy of SqsAsyncClient.
     */
    public SQSAsyncConnectionFactory() {
        this(new ProviderConfiguration());
    }

    /**
     * Creates a SQSConnectionFactory that uses SqsAsyncClientBuilder.standard() for creating SqsAsyncClient connections.
     * Every SQSConnection will have its own copy of SqsAsyncClient.
     */
    public SQSAsyncConnectionFactory(ProviderConfiguration providerConfiguration) {
        this(providerConfiguration, SqsAsyncClient.create());
    }

    /**
     * Creates a SQSConnectionFactory that uses the provided SqsAsyncClient connection.
     * Every SQSConnection will use the same provided SqsAsyncClient.
     */
    public SQSAsyncConnectionFactory(ProviderConfiguration providerConfiguration, final SqsAsyncClient client) {
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
    public SQSAsyncConnectionFactory(ProviderConfiguration providerConfiguration, final SqsAsyncClientBuilder clientBuilder) {
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
            SqsAsyncClient amazonSQS = amazonSQSClientSupplier.get();
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

    public SQSConnection createConnection(AwsCredentials awsCredentials) throws JMSException {
        AwsCredentialsProvider awsCredentialsProvider = StaticCredentialsProvider.create(awsCredentials);
        return createConnection(awsCredentialsProvider);
    }

    public SQSConnection createConnection(AwsCredentialsProvider awsCredentialsProvider) throws JMSException {
        try {
            SqsAsyncClient amazonSQS = amazonSQSClientSupplier.get();
            return createConnection(amazonSQS, awsCredentialsProvider);
        } catch (Exception e) {
            throw (JMSException) new JMSException("Error creating SQS client: " + e.getMessage()).initCause(e);
        }
    }

    private SQSConnection createConnection(SqsAsyncClient amazonSQS, AwsCredentialsProvider awsCredentialsProvider) throws JMSException {
        AmazonSQSAsyncMessagingClientWrapper amazonSQSClientJMSWrapper = new AmazonSQSAsyncMessagingClientWrapper(amazonSQS, awsCredentialsProvider);
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
