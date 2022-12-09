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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;

import org.junit.Before;
import org.junit.Test;

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

/**
 * Test the AmazonSQSMessagingClientWrapper class
 */
public class AmazonSQSMessagingClientWrapperTest {

    private final static String QUEUE_NAME = "queueName";
    private static final String OWNER_ACCOUNT_ID = "accountId";

    private SqsClient amazonSQSClient;
    private AmazonSQSMessagingClientWrapper wrapper;

    @Before
    public void setup() throws JMSException {
        amazonSQSClient = mock(SqsClient.class);
        wrapper = new AmazonSQSMessagingClientWrapper(amazonSQSClient);
    }

    /*
     * Test constructing client with null amazon sqs client
     */
    @Test(expected = JMSException.class)
    public void testNullSQSClient() throws JMSException {
        new AmazonSQSMessagingClientWrapper(null);
    }

    /*
     * Test delete message wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowAmazonClientException() throws JMSException {

        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        wrapper.deleteMessage(deleteMessageRequest);
    }

    /*
     * Test delete message wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowAmazonServiceException() throws JMSException {

        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("broken")))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        wrapper.deleteMessage(deleteMessageRequest);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowAmazonClientException() throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowAmazonServiceException() throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = DeleteMessageBatchRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("broken")))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testSendMessageThrowAmazonClientException() throws JMSException {

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        wrapper.sendMessage(sendMessageRequest);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testSendMessageThrowAmazonServiceException() throws JMSException {

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("broken")))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        wrapper.sendMessage(sendMessageRequest);
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
    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).queueOwnerAWSAccountId(OWNER_ACCOUNT_ID).build();     
    	
    	wrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowAmazonClientException() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowAmazonServiceException() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test(expected = InvalidDestinationException.class)
    public void testGetQueueUrlQueueNameThrowQueueDoesNotExistException() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build())
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }
    
    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test(expected = InvalidDestinationException.class)
    public void testGetQueueUrlQueueNameWithAccountIdThrowQueueDoesNotExistException() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueOwnerAWSAccountId(OWNER_ACCOUNT_ID).queueName(QUEUE_NAME).build();
        doThrow(QueueDoesNotExistException.builder().message("qdnee").build())
        .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME,OWNER_ACCOUNT_ID);
    }

    /*
     * Test getQueueUrl
     */
    @Test
    public void testGetQueueUrl() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();

        wrapper.getQueueUrl(getQueueUrlRequest);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowAmazonClientException() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(getQueueUrlRequest);
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowAmazonServiceException() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
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
    @Test(expected = JMSException.class)
    public void testQueueExistsThrowAmazonClientException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.queueExists(QUEUE_NAME);
    }

    /*
     * Test queue exist when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testQueueExistsThrowAmazonServiceException() throws JMSException {

    	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.queueExists(QUEUE_NAME);
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
    @Test(expected = JMSException.class)
    public void testCreateQueueWithNameThrowAmazonClientException() throws JMSException {

        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()));

        wrapper.createQueue(QUEUE_NAME);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueWithNameThrowAmazonServiceException() throws JMSException {

        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(CreateQueueRequest.builder().queueName(QUEUE_NAME).build()));

        wrapper.createQueue(QUEUE_NAME);
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
    @Test(expected = JMSException.class)
    public void testCreateQueueThrowAmazonClientException() throws JMSException {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(createQueueRequest));

        wrapper.createQueue(createQueueRequest);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueThrowAmazonServiceException() throws JMSException {


        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).createQueue(eq(createQueueRequest));

        wrapper.createQueue(createQueueRequest);
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
    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowAmazonClientException() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        wrapper.receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test receive message when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowAmazonServiceException() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = ReceiveMessageRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        wrapper.receiveMessage(getQueueUrlRequest);
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
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowAmazonClientException() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowAmazonServiceException() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
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
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowAmazonClientException() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = ChangeMessageVisibilityBatchRequest.builder().build();
        doThrow(SdkException.create("ace", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test change message visibility batch when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowAmazonServiceException() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = ChangeMessageVisibilityBatchRequest.builder().build();
        doThrow(SdkServiceException.create("ase", new Throwable("BROKEN")))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test get amazon SQS client
     */
    @Test
    public void testGetAmazonSQSClient() {
        assertEquals(amazonSQSClient, wrapper.getAmazonSQSClient());
    }
}
