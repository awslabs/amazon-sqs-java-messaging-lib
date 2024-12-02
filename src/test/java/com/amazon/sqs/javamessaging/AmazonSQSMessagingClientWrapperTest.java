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

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Test the AmazonSQSMessagingClientWrapper class
 */
public class AmazonSQSMessagingClientWrapperTest {

    private final static String QUEUE_NAME = "queueName";
    private static final String OWNER_ACCOUNT_ID = "accountId";

    private SqsClient amazonSQSClient;
    private AmazonSQSMessagingClient wrapper;

    @BeforeEach
    public void setup() throws JMSException {
        amazonSQSClient = mock(SqsClient.class);
        wrapper = new AmazonSQSMessagingClientWrapper(amazonSQSClient);
    }

    /*
     * Test constructing client with null amazon sqs client
     */
    @Test
    public void testNullSQSClient() {
        assertThatThrownBy(() -> new AmazonSQSMessagingClientWrapper(null))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test delete message wrap amazon sqs client amazon client exception
     */
    @Test
    public void testDeleteMessageThrowAmazonClientException() {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        assertThatThrownBy(() -> wrapper.deleteMessage(deleteMessageRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test delete message wrap amazon sqs client amazon service exception
     */
    @Test
    public void testDeleteMessageThrowAmazonServiceException() {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("broken")))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        assertThatThrownBy(() -> wrapper.deleteMessage(deleteMessageRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon client exception
     */
    @Test
    public void testDeleteMessageBatchThrowAmazonClientException() {
        DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        assertThatThrownBy(() -> wrapper.deleteMessageBatch(deleteMessageBatchRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon service exception
     */
    @Test
    public void testDeleteMessageBatchThrowAmazonServiceException() {
        DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("broken")))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        assertThatThrownBy(() -> wrapper.deleteMessageBatch(deleteMessageBatchRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon client exception
     */
    @Test
    public void testSendMessageThrowAmazonClientException() {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        assertThatThrownBy(() -> wrapper.sendMessage(sendMessageRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon service exception
     */
    @Test
    public void testSendMessageThrowAmazonServiceException() {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("broken")))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        assertThatThrownBy(() -> wrapper.sendMessage(sendMessageRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test getQueueUrl with queue name input
     */
    @Test
    public void testGetQueueUrlQueueName() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();

        wrapper.getQueueUrl(QUEUE_NAME);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl with queue name and owner account id input
     */
    @Test
    public void testGetQueueUrlQueueNameWithAccountId() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME)
                .queueOwnerAWSAccountId(OWNER_ACCOUNT_ID).build();

        wrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon client exception
     */
    @Test
    public void testGetQueueUrlQueueNameThrowAmazonClientException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.getQueueUrl(QUEUE_NAME))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon service exception
     */
    @Test
    public void testGetQueueUrlQueueNameThrowAmazonServiceException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.getQueueUrl(QUEUE_NAME))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test
    public void testGetQueueUrlQueueNameThrowQueueDoesNotExistException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build())
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.getQueueUrl(QUEUE_NAME))
                .isInstanceOf(InvalidDestinationException.class);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test
    public void testGetQueueUrlQueueNameWithAccountIdThrowQueueDoesNotExistException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueOwnerAWSAccountId(OWNER_ACCOUNT_ID)
                .queueName(QUEUE_NAME).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build())
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID))
                .isInstanceOf(InvalidDestinationException.class);
    }

    /*
     * Test getQueueUrl
     */
    @Test
    public void testGetQueueUrl() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();

        wrapper = new AmazonSQSMessagingClientWrapper(amazonSQSClient);

        wrapper.getQueueUrl(getQueueUrlRequest);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon client exception
     */
    @Test
    public void testGetQueueUrlThrowAmazonClientException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.getQueueUrl(getQueueUrlRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon service exception
     */
    @Test
    public void testGetQueueUrlThrowAmazonServiceException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.getQueueUrl(QUEUE_NAME))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test queue exist
     */
    @Test
    public void testQueueExistsWhenQueueIsPresent() throws JMSException {
        assertTrue(wrapper.queueExists(QUEUE_NAME));

        verify(amazonSQSClient).getQueueUrl(eq(GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build()));
    }

    /*
     * Test queue exist when amazon sqs client throws QueueDoesNotExistException
     */
    @Test
    public void testQueueExistsThrowQueueDoesNotExistException() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build())
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertFalse(wrapper.queueExists(QUEUE_NAME));
    }

    /*
     * Test queue exist when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testQueueExistsThrowAmazonClientException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.queueExists(QUEUE_NAME))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test queue exist when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testQueueExistsThrowAmazonServiceException() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.queueExists(QUEUE_NAME))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test create queue with name input
     */
    @Test
    public void testCreateQueueWithName() throws JMSException {
        wrapper.createQueue(QUEUE_NAME);
        verify(amazonSQSClient).createQueue(CreateQueueRequest.builder().queueName(QUEUE_NAME).build());
    }

    /*
     * Test create queue when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testCreateQueueWithNameThrowAmazonClientException() {
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()));

        assertThatThrownBy(() -> wrapper.createQueue(QUEUE_NAME))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testCreateQueueWithNameThrowAmazonServiceException() {
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()));

        assertThatThrownBy(() -> wrapper.createQueue(QUEUE_NAME))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test create queue
     */
    @Test
    public void testCreateQueue() throws JMSException {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();

        wrapper.createQueue(createQueueRequest);
        verify(amazonSQSClient).createQueue(createQueueRequest);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testCreateQueueThrowAmazonClientException() {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(createQueueRequest));

        assertThatThrownBy(() -> wrapper.createQueue(createQueueRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testCreateQueueThrowAmazonServiceException() {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(createQueueRequest));

        assertThatThrownBy(() -> wrapper.createQueue(createQueueRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test receive message
     */
    @Test
    public void testReceiveMessage() throws JMSException {
        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        wrapper.receiveMessage(getQueueUrlRequest);
        verify(amazonSQSClient).receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test receive message when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testReceiveMessageThrowAmazonClientException() {
        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.receiveMessage(getQueueUrlRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test receive message when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testReceiveMessageThrowAmazonServiceException() {
        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        assertThatThrownBy(() -> wrapper.receiveMessage(getQueueUrlRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test change message visibility
     */
    @Test
    public void testChangeMessageVisibility() throws JMSException {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder().build();
        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
        verify(amazonSQSClient).changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testChangeMessageVisibilityThrowAmazonClientException() {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        assertThatThrownBy(() -> wrapper.changeMessageVisibility(changeMessageVisibilityRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test change message visibility when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testChangeMessageVisibilityThrowAmazonServiceException() {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        assertThatThrownBy(() -> wrapper.changeMessageVisibility(changeMessageVisibilityRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test change message visibility batch
     */
    @Test
    public void testChangeMessageVisibilityBatch() throws JMSException {
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = ChangeMessageVisibilityBatchRequest.builder().build();
        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        verify(amazonSQSClient).changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test change message visibility batch when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testChangeMessageVisibilityBatchThrowAmazonClientException() {
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = ChangeMessageVisibilityBatchRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        assertThatThrownBy(() -> wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test change message visibility batch when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testChangeMessageVisibilityBatchThrowAmazonServiceException() {
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = ChangeMessageVisibilityBatchRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        assertThatThrownBy(() -> wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest))
                .isInstanceOf(JMSException.class);
    }

    /*
     * Test get amazon SQS client
     */
    @Test
    public void testGetAmazonSQSClient() {
        assertEquals(amazonSQSClient, wrapper.getAmazonSQSClient());
    }
}
