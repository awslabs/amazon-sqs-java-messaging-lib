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

import java.util.HashSet;
import java.util.Set;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;



/**
 * This is a JMS Wrapper of <code>AmazonSQSClient</code>. This class changes all
 * <code>AmazonServiceException</code> and <code>AmazonClientException</code> into
 * JMSException/JMSSecurityException.
 */
public class AmazonSQSMessagingClientWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(AmazonSQSMessagingClientWrapper.class);
    
    private static final Set<String> SECURITY_EXCEPTION_ERROR_CODES;
    static {
        /**
         * List of exceptions that can classified as security. These exceptions
         * are not thrown during connection-set-up rather after the service
         * calls of the <code>AmazonSQSClient</code>.
         */
        SECURITY_EXCEPTION_ERROR_CODES = new HashSet<String>();
        SECURITY_EXCEPTION_ERROR_CODES.add("MissingClientTokenId");
        SECURITY_EXCEPTION_ERROR_CODES.add("InvalidClientTokenId");
        SECURITY_EXCEPTION_ERROR_CODES.add("MissingAuthenticationToken");
        SECURITY_EXCEPTION_ERROR_CODES.add("AccessDenied");
    }

    private final SqsClient amazonSQSClient;
    private final AwsCredentialsProvider credentialsProvider;
    
    /**
     * @param amazonSQSClient
     *            The AWS SDK Client for SQS.
     * @throws JMSException
     *            if the client is null
     */
    public AmazonSQSMessagingClientWrapper(SqsClient amazonSQSClient) throws JMSException {
        this(amazonSQSClient, null);
    }
    
    /**
     * @param amazonSQSClient
     *            The AWS SDK Client for SQS.
     * @throws JMSException
     *            if the client is null
     */
    public AmazonSQSMessagingClientWrapper(SqsClient amazonSQSClient, AwsCredentialsProvider credentialsProvider) throws JMSException {
        if (amazonSQSClient == null) {
            throw new JMSException("Amazon SQS client cannot be null");
        }
        this.amazonSQSClient = amazonSQSClient;
        this.credentialsProvider = credentialsProvider;
    }
    
    /**
     * If one uses any other AWS SDK operations other than explicitly listed
     * here, the exceptions thrown by those operations will not be wrapped as
     * <code>JMSException</code>.
     * @return amazonSQSClient
     */
    public SqsClient getAmazonSQSClient() {
        return amazonSQSClient;
    }
    
    /**
     * Calls <code>deleteMessage</code> and wraps <code>AmazonClientException</code>. This is used to
     * acknowledge single messages, so that they can be deleted from SQS queue.
     * 
     * @param deleteMessageRequest
     *            Container for the necessary parameters to execute the
     *            deleteMessage service method on AmazonSQS.
     * @throws JMSException
     */
    public void deleteMessage(DeleteMessageRequest deleteMessageRequest) throws JMSException {
        try {
            prepareRequest(deleteMessageRequest);
            amazonSQSClient.deleteMessage(deleteMessageRequest);
        } catch (SdkException e) {
            throw handleException(e, "deleteMessage");
        }
    }
    
    /**
     * Calls <code>deleteMessageBatch</code> and wraps
     * <code>AmazonClientException</code>. This is used to acknowledge multiple
     * messages on client_acknowledge mode, so that they can be deleted from SQS
     * queue.
     * 
     * @param deleteMessageBatchRequest
     *            Container for the necessary parameters to execute the
     *            deleteMessageBatch service method on AmazonSQS. This is the
     *            batch version of deleteMessage. Max batch size is 10.
     * @return The response from the deleteMessageBatch service method, as
     *         returned by AmazonSQS
     * @throws JMSException
     */
    public DeleteMessageBatchResponse deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) throws JMSException {
        try {
            prepareRequest(deleteMessageBatchRequest);
            return amazonSQSClient.deleteMessageBatch(deleteMessageBatchRequest);
        } catch (SdkException e) {
            throw handleException(e, "deleteMessageBatch");
        }    
    }
    
    /**
     * Calls <code>sendMessage</code> and wraps
     * <code>AmazonClientException</code>.
     * 
     * @param sendMessageRequest
     *            Container for the necessary parameters to execute the
     *            sendMessage service method on AmazonSQS.
     * @return The response from the sendMessage service method, as returned by
     *         AmazonSQS
     * @throws JMSException
     */
    public SendMessageResponse sendMessage(SendMessageRequest sendMessageRequest) throws JMSException {
        try {
            prepareRequest(sendMessageRequest);
            return amazonSQSClient.sendMessage(sendMessageRequest);
        } catch (SdkException e) {
            throw handleException(e, "sendMessage");
        }    
    }
    
    /**
     * Check if the requested queue exists. This function calls
     * <code>GetQueueUrl</code> for the given queue name, returning true on
     * success, false if it gets <code>QueueDoesNotExistException</code>.
     * 
     * @param queueName
     *            the queue to check
     * @return true if the queue exists, false if it doesn't.
     * @throws JMSException
     */
    public boolean queueExists(String queueName) throws JMSException {
        try {
        	GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
        	prepareRequest(getQueueUrlRequest);
            amazonSQSClient.getQueueUrl(getQueueUrlRequest);
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (SdkException e) {
            throw handleException(e, "getQueueUrl");  
        } 
    }
    
    /**
     * Check if the requested queue exists. This function calls
     * <code>GetQueueUrl</code> for the given queue name with the given owner
     * accountId, returning true on success, false if it gets
     * <code>QueueDoesNotExistException</code>.
     * 
     * @param queueName
     *            the queue to check
     * @param queueOwnerAccountId
     *            The AWS accountId of the account that created the queue
     * @return true if the queue exists, false if it doesn't.
     * @throws JMSException
     */
    public boolean queueExists(String queueName, String queueOwnerAccountId) throws JMSException {
        try {
            GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
            		.queueName(queueName)
            		.queueOwnerAWSAccountId(queueOwnerAccountId)
            		.build();
            prepareRequest(getQueueUrlRequest);
            amazonSQSClient.getQueueUrl(getQueueUrlRequest);
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (SdkException e) {
            throw handleException(e, "getQueueUrl");
        }
    }
    
    /**
     * Gets the queueUrl of a queue given a queue name.
     * 
     * @param queueName
     * @return The response from the GetQueueUrl service method, as returned by
     *         AmazonSQS, which will include queue`s URL
     * @throws JMSException
     */
    public GetQueueUrlResponse getQueueUrl(String queueName) throws JMSException {
    	GetQueueUrlRequest request = GetQueueUrlRequest.builder()
    			.queueName(queueName)
    			.build();
        return getQueueUrl(request);
    }
    
    /**
     * Gets the queueUrl of a queue given a queue name owned by the provided accountId.
     * 
     * @param queueName
     * @param queueOwnerAccountId The AWS accountId of the account that created the queue
     * @return The response from the GetQueueUrl service method, as returned by
     *         AmazonSQS, which will include queue`s URL
     * @throws JMSException
     */
    public GetQueueUrlResponse getQueueUrl(String queueName, String queueOwnerAccountId) throws JMSException {
    	GetQueueUrlRequest request = GetQueueUrlRequest.builder()
    			.queueName(queueName)
    			.queueOwnerAWSAccountId(queueOwnerAccountId)
    			.build();
        return getQueueUrl(request);
    }
     
    /**
     * Calls <code>getQueueUrl</code> and wraps <code>AmazonClientException</code>
     * 
     * @param getQueueUrlRequest
     *            Container for the necessary parameters to execute the
     *            getQueueUrl service method on AmazonSQS.
     * @return The response from the GetQueueUrl service method, as returned by
     *         AmazonSQS, which will include queue`s URL
     * @throws JMSException
     */
    public GetQueueUrlResponse getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws JMSException {
        try {
            prepareRequest(getQueueUrlRequest);
            return amazonSQSClient.getQueueUrl(getQueueUrlRequest);
        } catch (SdkException e) {
            throw handleException(e, "getQueueUrl");  
        }    
    }
    
    /**
     * Calls <code>createQueue</code> to create the queue with the default queue attributes,
     * and wraps <code>AmazonClientException</code>
     * 
     * @param queueName
     * @return The response from the createQueue service method, as returned by
     *         AmazonSQS. This call creates a new queue, or returns the URL of
     *         an existing one.
     * @throws JMSException
     */
    public CreateQueueResponse createQueue(String queueName) throws JMSException {
        return createQueue(CreateQueueRequest.builder().queueName(queueName).build());
    }
    
    /**
     * Calls <code>createQueue</code> to create the queue with the provided queue attributes
     * if any, and wraps <code>AmazonClientException</code>
     * 
     * @param createQueueRequest
     *            Container for the necessary parameters to execute the
     *            createQueue service method on AmazonSQS.
     * @return The response from the createQueue service method, as returned by
     *         AmazonSQS. This call creates a new queue, or returns the URL of
     *         an existing one.
     * @throws JMSException
     */
    public CreateQueueResponse createQueue(CreateQueueRequest createQueueRequest) throws JMSException {
        try {
            prepareRequest(createQueueRequest);
            return amazonSQSClient.createQueue(createQueueRequest);
        } catch (SdkException e) {
            throw handleException(e, "createQueue");   
        }    
    }
    
    /**
     * Calls <code>receiveMessage</code> and wraps <code>AmazonClientException</code>. Used by
     * {@link SQSMessageConsumerPrefetch} to receive up to minimum of
     * (<code>numberOfMessagesToPrefetch</code>,10) messages from SQS queue into consumer
     * prefetch buffers.
     * 
     * @param receiveMessageRequest
     *            Container for the necessary parameters to execute the
     *            receiveMessage service method on AmazonSQS.
     * @return The response from the ReceiveMessage service method, as returned
     *         by AmazonSQS.
     * @throws JMSException
     */    
    public ReceiveMessageResponse receiveMessage(ReceiveMessageRequest receiveMessageRequest) throws JMSException {
        try {
            prepareRequest(receiveMessageRequest);
            return amazonSQSClient.receiveMessage(receiveMessageRequest);
        } catch (SdkException e) {
            throw handleException(e, "receiveMessage");
        }
    }
    
    /**
     * Calls <code>changeMessageVisibility</code> and wraps <code>AmazonClientException</code>. This is 
     * used to for negative acknowledge of a single message, so that messages can be received again without any delay.
     * 
     * @param changeMessageVisibilityRequest
     *            Container for the necessary parameters to execute the
     *            changeMessageVisibility service method on AmazonSQS.
     * @throws JMSException
     */
    public void changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest) throws JMSException {
        try {
            prepareRequest(changeMessageVisibilityRequest);
            amazonSQSClient.changeMessageVisibility(changeMessageVisibilityRequest);
        } catch (SdkException e) {
            throw handleException(e, "changeMessageVisibility");
        }    
    }
    
    /**
     * Calls <code>changeMessageVisibilityBatch</code> and wraps <code>AmazonClientException</code>. This is
     * used to for negative acknowledge of messages in batch, so that messages
     * can be received again without any delay.
     * 
     * @param changeMessageVisibilityBatchRequest
     *            Container for the necessary parameters to execute the
     *            changeMessageVisibilityBatch service method on AmazonSQS.
     * @return The response from the changeMessageVisibilityBatch service
     *         method, as returned by AmazonSQS.
     * @throws JMSException
     */
    public ChangeMessageVisibilityBatchResponse changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest)
            throws JMSException {
        try {
            prepareRequest(changeMessageVisibilityBatchRequest);
            return amazonSQSClient.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        } catch (SdkException e) {
            throw handleException(e, "changeMessageVisibilityBatch");
        }
    }

    /**
     * Create generic error message for <code>AmazonServiceException</code>. Message include
     * Action, RequestId, HTTPStatusCode, and AmazonErrorCode.
     */
    private String logAndGetAmazonServiceException(AwsServiceException ase, String action) {
        String errorMessage = "AmazonServiceException: " + action + ". RequestId: " + ase.requestId() +
                              "\nHTTPStatusCode: " + ase.statusCode() + " AmazonErrorCode: " + errorCode(ase);
        LOG.error(errorMessage, ase);
        return errorMessage;
    }

    /**
     * Create generic error message for <code>AmazonClientException</code>. Message include
     * Action.
     */
    private String logAndGetAmazonClientException(SdkException ace, String action) {
        String errorMessage = "AmazonClientException: " + action + ".";
        LOG.error(errorMessage, ace);
        return errorMessage;
    }
    
    private JMSException handleException(SdkException e, String operationName) throws JMSException {
        JMSException jmsException;
        if (e instanceof AwsServiceException) {
        	AwsServiceException se = ( AwsServiceException ) e;
            
            if (e instanceof QueueDoesNotExistException) {
                jmsException = new InvalidDestinationException(
                        logAndGetAmazonServiceException(se, operationName), errorCode(se));
            } else if (isJMSSecurityException(se)) {
                jmsException = new JMSSecurityException(
                        logAndGetAmazonServiceException(se, operationName), errorCode(se));
            } else {
                jmsException = new JMSException(
                        logAndGetAmazonServiceException(se, operationName), errorCode(se));
            }

        } else {
            jmsException = new JMSException(logAndGetAmazonClientException(e, operationName));
        }
        jmsException.initCause(e);
        return jmsException;
    }
    
    private static String errorCode(AwsServiceException e)
    {
    	return e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : "";
    }
    
    
    private static boolean isJMSSecurityException(AwsServiceException e) {
        return SECURITY_EXCEPTION_ERROR_CODES.contains(errorCode(e)) ;
    }
    
    private void prepareRequest(AwsRequest request) {
    	
    	if(credentialsProvider != null)
    	{
    		request = request.toBuilder().overrideConfiguration(AwsRequestOverrideConfiguration
    				.builder()
    				.credentialsProvider(credentialsProvider)
    				.build()).build();
    	}
    }
    
}
