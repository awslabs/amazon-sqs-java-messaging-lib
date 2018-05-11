package com.amazon.sqs.javamessaging;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

public class SqsTemporaryQueue implements TemporaryQueue {

    private final String tempQueueName;

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;

    public SqsTemporaryQueue(String tempQueueName, AmazonSQSMessagingClientWrapper amazonSQSClient) {
        this.tempQueueName = tempQueueName;
        this.amazonSQSClient = amazonSQSClient;
    }

    @Override
    public void delete() throws JMSException {
        this.amazonSQSClient.getAmazonSQSClient().deleteQueue(this.tempQueueName);
    }

    @Override
    public String getQueueName() throws JMSException {
        return this.tempQueueName;
    }
}
