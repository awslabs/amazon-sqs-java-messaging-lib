package com.amazon.sqs.javamessaging;

import jakarta.jms.JMSException;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.services.sqs.model.*;

public interface AmazonSQSMessagingClient {

    AwsClient getAmazonSQSClient();

    void deleteMessage(DeleteMessageRequest deleteMessageRequest) throws JMSException;

    DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) throws JMSException;

    SendMessageResponse sendMessage(SendMessageRequest sendMessageRequest) throws JMSException;

    boolean queueExists(String queueName) throws JMSException;

    boolean queueExists(String queueName, String queueOwnerAccountId) throws JMSException;

    GetQueueUrlResponse getQueueUrl(String queueName) throws JMSException;

    GetQueueUrlResponse getQueueUrl(String queueName, String queueOwnerAccountId) throws JMSException;

    GetQueueUrlResponse getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws JMSException;

    CreateQueueResponse createQueue(String queueName) throws JMSException;

    CreateQueueResponse createQueue(CreateQueueRequest createQueueRequest) throws JMSException;

    ReceiveMessageResponse receiveMessage(ReceiveMessageRequest receiveMessageRequest) throws JMSException;

    void changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest) throws JMSException;

    ChangeMessageVisibilityBatchResponse changeMessageVisibilityBatch(
            ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest) throws JMSException;
}
