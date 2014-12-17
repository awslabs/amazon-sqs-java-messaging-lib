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
package com.amazonaws.sqsjms;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import javax.jms.JMSException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AmazonSQSClientJMSWrapperTest {

    private AmazonSQSClient amazonSQSClient;
    private AmazonSQSClientJMSWrapper wrapper;
    private final static String QUEUE_NAME = "queueName";

    @Before
    public void setup() throws JMSException {
        amazonSQSClient = mock(AmazonSQSClient.class);
        wrapper = new AmazonSQSClientJMSWrapper(amazonSQSClient);
    }

    @Test(expected = JMSException.class)
    public void testNullSQSClient() throws JMSException {
        new AmazonSQSClientJMSWrapper(null);
    }

    @Test(expected = JMSException.class)
    public void testSetEndpointThrowIllegalArgumentException() throws JMSException {

        String endpoint = "endpoint";
        doThrow(new IllegalArgumentException("iae"))
                .when(amazonSQSClient).setEndpoint(eq(endpoint));

        wrapper.setEndpoint(endpoint);
    }

    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowAmazonClientException() throws JMSException {

        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        wrapper.deleteMessage(deleteMessageRequest);
    }

    @Test(expected = JMSException.class)
    public void testDeleteMessageThrowAmazonServiceException() throws JMSException {

        DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).deleteMessage(eq(deleteMessageRequest));

        wrapper.deleteMessage(deleteMessageRequest);
    }

    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowAmazonClientException() throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    @Test(expected = JMSException.class)
    public void testDeleteMessageBatchThrowAmazonServiceException() throws JMSException {

        DeleteMessageBatchRequest deleteMessageBatchRequest = new DeleteMessageBatchRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).deleteMessageBatch(eq(deleteMessageBatchRequest));

        wrapper.deleteMessageBatch(deleteMessageBatchRequest);
    }

    @Test(expected = JMSException.class)
    public void testSendMessageThrowAmazonClientException() throws JMSException {

        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        wrapper.sendMessage(sendMessageRequest);
    }

    @Test(expected = JMSException.class)
    public void testSendMessageThrowAmazonServiceException() throws JMSException {

        SendMessageRequest sendMessageRequest = new SendMessageRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).sendMessage(eq(sendMessageRequest));

        wrapper.sendMessage(sendMessageRequest);
    }

    @Test
    public void testGetQueueUrlQueueName() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);

        wrapper.getQueueUrl(QUEUE_NAME);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowAmazonClientException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    @Test(expected = JMSException.class)
    public void testGetQueueUrlQueueNameThrowAmazonServiceException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    @Test
    public void testGetQueueUrl() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);

        wrapper.getQueueUrl(getQueueUrlRequest);
        verify(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));
    }

    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowAmazonClientException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(getQueueUrlRequest);
    }

    @Test(expected = JMSException.class)
    public void testGetQueueUrlThrowAmazonServiceException() throws JMSException {

        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(QUEUE_NAME);
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).getQueueUrl(eq(getQueueUrlRequest));

        wrapper.getQueueUrl(QUEUE_NAME);
    }

    @Test
    public void testCreateQueue() throws JMSException {

        wrapper.createQueue(QUEUE_NAME);
        verify(amazonSQSClient).createQueue(QUEUE_NAME);
    }

    @Test(expected = JMSException.class)
    public void testCreateQueueThrowAmazonClientException() throws JMSException {

        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).createQueue(eq(QUEUE_NAME));

        wrapper.createQueue(QUEUE_NAME);
    }

    @Test(expected = JMSException.class)
    public void testCreateQueueThrowAmazonServiceException() throws JMSException {

        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).createQueue(eq(QUEUE_NAME));

        wrapper.createQueue(QUEUE_NAME);
    }

    @Test
    public void testReceiveMessage() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        wrapper.receiveMessage(getQueueUrlRequest);
        verify(amazonSQSClient).receiveMessage(getQueueUrlRequest);
    }

    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowAmazonClientException() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        wrapper.receiveMessage(getQueueUrlRequest);
    }

    @Test(expected = JMSException.class)
    public void testReceiveMessageThrowAmazonServiceException() throws JMSException {

        ReceiveMessageRequest getQueueUrlRequest = new ReceiveMessageRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).receiveMessage(eq(getQueueUrlRequest));

        wrapper.receiveMessage(getQueueUrlRequest);
    }

    @Test
    public void testChangeMessageVisibility() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
        verify(amazonSQSClient).changeMessageVisibility(changeMessageVisibilityRequest);
    }

    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowAmazonClientException() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityThrowAmazonServiceException() throws JMSException {

        ChangeMessageVisibilityRequest changeMessageVisibilityRequest = new ChangeMessageVisibilityRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).changeMessageVisibility(eq(changeMessageVisibilityRequest));

        wrapper.changeMessageVisibility(changeMessageVisibilityRequest);
    }

    @Test
    public void testChangeMessageVisibilityBatch() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        verify(amazonSQSClient).changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowAmazonClientException() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        doThrow(new AmazonClientException("ace"))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }

    @Test(expected = JMSException.class)
    public void testChangeMessageVisibilityBatchThrowAmazonServiceException() throws JMSException {

        ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest = new ChangeMessageVisibilityBatchRequest();
        doThrow(new AmazonServiceException("ase"))
                .when(amazonSQSClient).changeMessageVisibilityBatch(eq(changeMessageVisibilityBatchRequest));

        wrapper.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
    }
}
