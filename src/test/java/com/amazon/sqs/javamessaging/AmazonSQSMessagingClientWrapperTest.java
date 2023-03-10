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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import jakarta.jms.InvalidDestinationException;
import jakarta.jms.JMSException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

//import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private AmazonSQSClient amazonSQSClient;
    private AmazonSQSMessagingClientWrapper wrapper;

    @BeforeEach
    public void setup() throws JMSException {
        amazonSQSClient = mock(AmazonSQSClient.class);
        wrapper = new AmazonSQSMessagingClientWrapper(amazonSQSClient);
    }

    /**
     * Test constructing client with null amazon sqs client
     */
    @Test
    public void testNullSQSClient() {
        assertThrows(JMSException.class, () -> new AmazonSQSMessagingClientWrapper(null));
    }

    /**
     * Test set endpoint
     */
    @Test
    public void testSetEndpoint() throws JMSException {
        String endpoint = "endpoint";
        wrapper.setEndpoint(endpoint);
        verify(amazonSQSClient).setEndpoint(eq(endpoint));
    }

    /**
     * Test set endpoint wrap amazon sqs client exception
     */
    @Test
    public void testSetEndpointThrowIllegalArgumentException() {
        String endpoint = "endpoint";
        doThrow(new IllegalArgumentException("iae")).when(amazonSQSClient).setEndpoint(eq(endpoint));
        assertThrows(JMSException.class, () -> wrapper.setEndpoint(endpoint));
    }

    /**
     * Test set region
     */
    @Test
    public void testSetRegion() throws JMSException {
        Region region = Region.getRegion(Regions.DEFAULT_REGION);
        wrapper.setRegion(region);
        verify(amazonSQSClient).setRegion(eq(region));
    }

    /**
     * Test set region wrap amazon sqs client exception
     */
    @Test
    public void testSetRegionThrowIllegalArgumentException() {
        Region region = Region.getRegion(Regions.DEFAULT_REGION);
        doThrow(new IllegalArgumentException("iae")).when(amazonSQSClient).setRegion(eq(region));
        assertThrows(JMSException.class, () -> wrapper.setRegion(region));
    }

    /**
     * Test delete message wrap amazon sqs client amazon client exception
     */
    @Test
    public void testDeleteMessageThrowAmazonClientException() {
        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));
        assertThrows(JMSException.class, () -> wrapper.deleteMessage(deleteMessageRequest));
    }

    /**
     * Test delete message wrap amazon sqs client amazon service exception
     */
    @Test
    public void testDeleteMessageThrowAmazonServiceException() {
        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));
        assertThrows(JMSException.class, () -> wrapper.deleteMessage(deleteMessageRequest));
    }

    /**
     * Test delete message batch wrap amazon sqs client amazon client exception
     */
    @Test
    public void testDeleteMessageBatchThrowAmazonClientException() {
        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));
        assertThrows(JMSException.class, () -> wrapper.deleteMessageBatch(deleteMessageBatchRequest));
    }

    /**
     * Test delete message batch wrap amazon sqs client amazon service exception
     */
    @Test
    public void testDeleteMessageBatchThrowAmazonServiceException() {
        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));
        assertThrows(JMSException.class, () -> wrapper.deleteMessageBatch(deleteMessageBatchRequest));
    }

    /**
     * Test send message batch wrap amazon sqs client amazon client exception
     */
    @Test
    public void testSendMessageThrowAmazonClientException() {
        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).sendMessage(eq(sendMessageRequest));
        assertThrows(JMSException.class, () -> wrapper.sendMessage(sendMessageRequest));
    }

    /**
     * Test send message batch wrap amazon sqs client amazon service exception
     */
    @Test
    public void testSendMessageThrowAmazonServiceException() {
        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).sendMessage(eq(sendMessageRequest));
        assertThrows(JMSException.class, () -> wrapper.sendMessage(sendMessageRequest));
    }

    /**
     * Test getQueueUrl with queue name input
     */
    @Test
    public void testGetQueueUrlQueueName() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        wrapper.getQueueUrl(QUEUE_NAME);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }
    
    /**
     * Test getQueueUrl with queue name and owner account id input
     */
    @Test
    public void testGetQueueUrlQueueNameWithAccountId() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        getQueueUrlRequest.setQueueOwnerAWSAccountId(OWNER_ACCOUNT_ID);
        wrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /**
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon client exception
     */
    @Test
    public void testGetQueueUrlQueueNameThrowAmazonClientException() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.getQueueUrl(QUEUE_NAME));
    }

    /**
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon service exception
     */
    @Test
    public void testGetQueueUrlQueueNameThrowAmazonServiceException() {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.getQueueUrl(QUEUE_NAME));
    }

    /**
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test
    public void testGetQueueUrlQueueNameThrowQueueDoesNotExistException() {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new QueueDoesNotExistException("qdnee")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(InvalidDestinationException.class, () -> wrapper.getQueueUrl(QUEUE_NAME));
    }
    
    /**
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test
    public void testGetQueueUrlQueueNameWithAccountIdThrowQueueDoesNotExistException() {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        getQueueUrlRequest.setQueueOwnerAWSAccountId(OWNER_ACCOUNT_ID);
        doThrow(new QueueDoesNotExistException("qdnee")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(InvalidDestinationException.class, () -> wrapper.getQueueUrl(QUEUE_NAME,OWNER_ACCOUNT_ID));
    }

    /**
     * Test getQueueUrl
     */
    @Test
    public void testGetQueueUrl() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        wrapper.getQueueUrl(getQueueUrlRequest);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /**
     * Test getQueueUrl wrap amazon sqs amazon client exception
     */
    @Test
    public void testGetQueueUrlThrowAmazonClientException() {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.getQueueUrl(getQueueUrlRequest));
    }

    /**
     * Test getQueueUrl wrap amazon sqs amazon service exception
     */
    @Test
    public void testGetQueueUrlThrowAmazonServiceException() {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.getQueueUrl(QUEUE_NAME));
    }

    /**
     * Test queue exist
     */
    @Test
    public void testQueueExistsWhenQueueIsPresent() throws JMSException {
        assertTrue(wrapper.queueExists(QUEUE_NAME));
        verify(amazonSQSClient).getQueueUrl(eq(new GetQueueUrlRequest(QUEUE_NAME)));
    }

    /**
     * Test queue exist when amazon sqs client throws QueueDoesNotExistException
     */
    @Test
    public void testQueueExistsThrowQueueDoesNotExistException() throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new QueueDoesNotExistException("qdnee")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertFalse(wrapper.queueExists(QUEUE_NAME));
    }

    /**
     * Test queue exist when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testQueueExistsThrowAmazonClientException() {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.queueExists(QUEUE_NAME));
    }

    /**
     * Test queue exist when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testQueueExistsThrowAmazonServiceException() {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.queueExists(QUEUE_NAME));
    }

    /**
     * Test create queue with name input
     */
    @Test
    public void testCreateQueueWithName() throws JMSException {
        wrapper.createQueue(QUEUE_NAME);
        verify(amazonSQSClient).createQueue(new CreateQueueRequest(QUEUE_NAME));
    }

    /**
     * Test create queue when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testCreateQueueWithNameThrowAmazonClientException() {
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).createQueue(eq(new CreateQueueRequest(QUEUE_NAME)));
        assertThrows(JMSException.class, () -> wrapper.createQueue(QUEUE_NAME));
    }

    /**
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testCreateQueueWithNameThrowAmazonServiceException() {
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).createQueue(eq(new CreateQueueRequest(QUEUE_NAME)));
        assertThrows(JMSException.class, () -> wrapper.createQueue(QUEUE_NAME));
    }

    /**
     * Test create queue
     */
    @Test
    public void testCreateQueue() throws JMSException {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);
        wrapper.createQueue(createQueueRequest);
        verify(amazonSQSClient).createQueue(createQueueRequest);
    }

    /**
     * Test create queue when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testCreateQueueThrowAmazonClientException() {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).createQueue(eq(createQueueRequest));
        assertThrows(JMSException.class, () -> wrapper.createQueue(createQueueRequest));
    }

    /**
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testCreateQueueThrowAmazonServiceException() {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).createQueue(eq(createQueueRequest));
        assertThrows(JMSException.class, () -> wrapper.createQueue(createQueueRequest));
    }

    /**
     * Test receive message
     */
    @Test
    public void testReceiveMessage() throws JMSException {
        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        wrapper.receiveMessage(getQueueUrlRequest);
        verify(amazonSQSClient).receiveMessage(getQueueUrlRequest);
    }

    /**
     * Test receive message when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testReceiveMessageThrowAmazonClientException() {
        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.receiveMessage(getQueueUrlRequest));
    }

    /**
     * Test receive message when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testReceiveMessageThrowAmazonServiceException() {
        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));
        assertThrows(JMSException.class, () -> wrapper.receiveMessage(getQueueUrlRequest));
    }

    /**
     * Test change message visibility
     */
    @Test
    public void testChangeMessageVisibility() throws JMSException {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
        verify(amazonSQSClient).changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /**
     * Test change message visibility when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testChangeMessageVisibilityThrowAmazonClientException() {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        doThrow(new AmazonClientException("ace")).when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));
        assertThrows(JMSException.class, () -> wrapper.changeMessageVisibility(changeMessageVisibilityRequest));
    }

    /**
     * Test change message visibility when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testChangeMessageVisibilityThrowAmazonServiceException() {
        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        doThrow(new AmazonServiceException("ase")).when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));
        assertThrows(JMSException.class, () -> wrapper.changeMessageVisibility(changeMessageVisibilityRequest));
    }

    /**
     * Test change message visibility batch
     */
    @Test
    public void testChangeMessageVisibilityBatch() throws JMSException {
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        verify(amazonSQSClient).changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /**
     * Test change message visibility batch when amazon sqs client throws AmazonClientException
     */
    @Test
    public void testChangeMessageVisibilityBatchThrowAmazonClientException() {
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));
        assertThrows(JMSException.class, () -> wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest));
    }

    /**
     * Test change message visibility batch when amazon sqs client throws AmazonServiceException
     */
    @Test
    public void testChangeMessageVisibilityBatchThrowAmazonServiceException() {
        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));
        assertThrows(JMSException.class, () -> wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest));
    }

    /**
     * Test get amazon SQS client
     */
    @Test
    public void testGetAmazonSQSClient() {
        assertEquals(amazonSQSClient, wrapper.getAmazonSQSClient());
    }
}
