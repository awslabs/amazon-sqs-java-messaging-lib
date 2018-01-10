/*
 * Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
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

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
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

    @Before
    public void setup() throws JMSException {
        amazonSQSClient = mock(AmazonSQSClient.class);
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
     * Test set endpoint
     */
    @Test
    public void testSetEndpoint() throws JMSException {

        String endpoint = "endpoint";
        wrapper.setEndpoint(endpoint);
        verify(amazonSQSClient).setEndpoint(eq(endpoint));
    }

    /*
     * Test set endpoint wrap amazon sqs client exception
     */
    @Test(expected = JMSException.class)
    public void testSetEndpointThrowIllegalArgumentException() throws JMSException {

        String endpoint = "endpoint";
        doThrow(new IllegalArgumentException("iae"))
                .when(amazonSQSClient).setEndpoint(eq(endpoint));

        wrapper.setEndpoint(endpoint);
    }

    /*
     * Test set region
     */
    @Test
    public void testSetRegion() throws JMSException {

        Region region = Region.getRegion(Regions.DEFAULT_REGION);
        wrapper.setRegion(region);
        verify(amazonSQSClient).setRegion(eq(region));
    }

    /*
     * Test set region wrap amazon sqs client exception
     */
    @Test(expected = JMSException.class)
    public void testSetRegionThrowIllegalArgumentException() throws JMSException {

        Region region = Region.getRegion(Regions.DEFAULT_REGION);
        doThrow(new IllegalArgumentException("iae"))
                .when(amazonSQSClient).setRegion(eq(region));

        wrapper.setRegion(region);
    }

    /*
     * Test delete message wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowAmazonClientException() throws JMSException {

        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        wrapper.deleteMessage(deleteMessageRequest);
    }

    /*
     * Test delete message wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowAmazonServiceException() throws JMSException {

        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        wrapper.deleteMessage(deleteMessageRequest);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowAmazonClientException() throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    /*
     * Test delete message batch wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowAmazonServiceException() throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testSendMessageThrowAmazonClientException() throws JMSException {

        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        wrapper.sendMessage(sendMessageRequest);
    }

    /*
     * Test send message batch wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testSendMessageThrowAmazonServiceException() throws JMSException {

        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        wrapper.sendMessage(sendMessageRequest);
    }

    /*
     * Test getQueueUrl with queue name input
     */
    @Test
    public void testGetQueueUrlQueueName() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);

        wrapper.getQueueUrl(QUEUE_NAME);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }
    
    /*
     * Test getQueueUrl with queue name and owner account id input
     */
    @Test
    public void testGetQueueUrlQueueNameWithAccountId() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        getQueueUrlRequest.setQueueOwnerAWSAccountId(OWNER_ACCOUNT_ID);
        
        wrapper.getQueueUrl(QUEUE_NAME, OWNER_ACCOUNT_ID);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowAmazonClientException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs client amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowAmazonServiceException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test(expected = InvalidDestinationException.class)
    public void testGetQueueUrlQueueNameThrowQueueDoesNotExistException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new QueueDoesNotExistException("qdnee"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }
    
    /*
     * Test getQueueUrl with queue name input wrap amazon sqs queue does not exist exception
     */
    @Test(expected = InvalidDestinationException.class)
    public void testGetQueueUrlQueueNameWithAccountIdThrowQueueDoesNotExistException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        getQueueUrlRequest.setQueueOwnerAWSAccountId(OWNER_ACCOUNT_ID);
        doThrow(new QueueDoesNotExistException("qdnee"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME,OWNER_ACCOUNT_ID);
    }

    /*
     * Test getQueueUrl
     */
    @Test
    public void testGetQueueUrl() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);

        wrapper.getQueueUrl(getQueueUrlRequest);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon client exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowAmazonClientException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(getQueueUrlRequest);
    }

    /*
     * Test getQueueUrl wrap amazon sqs amazon service exception
     */
    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowAmazonServiceException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    /*
     * Test queue exist
     */
    @Test
    public void testQueueExistsWhenQueueIsPresent() throws JMSException {

        assertTrue(wrapper.queueExists(QUEUE_NAME));
        verify(amazonSQSClient).getQueueUrl(eq(new GetQueueUrlRequest(QUEUE_NAME)));
    }

    /*
     * Test queue exist when amazon sqs client throws QueueDoesNotExistException
     */
    @Test
    public void testQueueExistsThrowQueueDoesNotExistException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new QueueDoesNotExistException("qdnee"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        assertFalse(wrapper.queueExists(QUEUE_NAME));
    }

    /*
     * Test queue exist when amazon sqs client throws AmazonClientException
     */
    @Test(expected = JMSException.class)
    public void testQueueExistsThrowAmazonClientException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.queueExists(QUEUE_NAME);
    }

    /*
     * Test queue exist when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testQueueExistsThrowAmazonServiceException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.queueExists(QUEUE_NAME);
    }

    /*
     * Test create queue with name input
     */
    @Test
    public void testCreateQueueWithName() throws JMSException {

        wrapper.createQueue(QUEUE_NAME);
        verify(amazonSQSClient).createQueue(QUEUE_NAME);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonClientException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueWithNameThrowAmazonClientException() throws JMSException {

        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).createQueue(eq(QUEUE_NAME));

        wrapper.createQueue(QUEUE_NAME);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueWithNameThrowAmazonServiceException() throws JMSException {

        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).createQueue(eq(QUEUE_NAME));

        wrapper.createQueue(QUEUE_NAME);
    }

    /*
     * Test create queue
     */
    @Test
    public void testCreateQueue() throws JMSException {

        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);

        wrapper.createQueue(createQueueRequest);
        verify(amazonSQSClient).createQueue(createQueueRequest);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonClientException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueThrowAmazonClientException() throws JMSException {

        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).createQueue(eq(createQueueRequest));

        wrapper.createQueue(createQueueRequest);
    }

    /*
     * Test create queue when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testCreateQueueThrowAmazonServiceException() throws JMSException {


        CreateQueueRequest createQueueRequest = new CreateQueueRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).createQueue(eq(createQueueRequest));

        wrapper.createQueue(createQueueRequest);
    }

    /*
     * Test receive message
     */
    @Test
    public void testReceiveMessage() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        wrapper.receiveMessage(getQueueUrlRequest);
        verify(amazonSQSClient).receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test receive message when amazon sqs client throws AmazonClientException
     */
    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowAmazonClientException() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        wrapper.receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test receive message when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowAmazonServiceException() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        wrapper.receiveMessage(getQueueUrlRequest);
    }

    /*
     * Test change message visibility
     */
    @Test
    public void testChangeMessageVisibility() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
        verify(amazonSQSClient).changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility when amazon sqs client throws AmazonClientException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowAmazonClientException() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowAmazonServiceException() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    /*
     * Test change message visibility batch
     */
    @Test
    public void testChangeMessageVisibilityBatch() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        verify(amazonSQSClient).changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test change message visibility batch when amazon sqs client throws AmazonClientException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowAmazonClientException() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    /*
     * Test change message visibility batch when amazon sqs client throws AmazonServiceException
     */
    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowAmazonServiceException() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        doThrow(new AmazonServiceException("ase"))
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
