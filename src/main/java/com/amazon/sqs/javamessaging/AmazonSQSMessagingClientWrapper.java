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

import java.util.HashSet;
import java.util.Set;

import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * This is a JMS Wrapper of <code>AmazonSQSClient</code>. This class changes all
 * <code>AmazonServiceException</code> and <code>AmazonClientException</code> into
 * JMSException/JMSSecurityException.
 */
public class AmazonSQSMessagingClientWrapper {
    private static final Log LOG = LogFactory.getLog(AmazonSQSMessagingClientWrapper.class);
    
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

    private final AmazonSQS amazonSQSClient;
    
    /**
     * @param amazonSQSClient
     *            The AWS SDK Client for SQS.
     * @throws JMSException
     *            if the client is null
     */
    public AmazonSQSMessagingClientWrapper(AmazonSQS amazonSQSClient) throws JMSException {
        if (amazonSQSClient == null) {
            throw new JMSException("Amazon SQS client cannot be null");
        }
        this.amazonSQSClient = amazonSQSClient;
    }
    
    /**
     * If one uses any other AWS SDK operations other than explicitly listed
     * here, the exceptions thrown by those operations will not be wrapped as
     * <code>JMSException</code>.
     * @return amazonSQSClient
     */
    public AmazonSQS getAmazonSQSClient() {
        return amazonSQSClient;
    }

    /**
     * Sets SQS endpoint and wraps IllegalArgumentException.
     * 
     * @param endpoint
     *            The endpoint (ex: "sqs.us-east-1.amazonaws.com") of the region
     *            specific AWS endpoint this client will communicate with.
     * @throws JMSException
     */
    public void setEndpoint(String endpoint) throws JMSException {
        try {
            amazonSQSClient.setEndpoint(endpoint);
        } catch (IllegalArgumentException ase) {
            JMSException jmsException = new JMSException(ase.getMessage());
            throw (JMSException) jmsException.initCause(ase);
        }
    }
    
    /**
     * Sets SQS region and wraps <code>IllegalArgumentException</code>. This is the recommend
     * way to set-up the SQS end-points.
     * 
     * @param region
     *            The region this client will communicate with. See
     *            {@link Region#getRegion(com.amazonaws.regions.Regions)} for
     *            accessing a given region.
     * @throws JMSException
     */
    public void setRegion(Region region) throws JMSException {
        try {
            amazonSQSClient.setRegion(region);
        } catch (IllegalArgumentException ase) {
            JMSException jmsException = new JMSException(ase.getMessage());
            throw (JMSException) jmsException.initCause(ase);
        }
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
            amazonSQSClient.deleteMessage(deleteMessageRequest);
        } catch (AmazonClientException e) {
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
    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) throws JMSException {
        try {
            return amazonSQSClient.deleteMessageBatch(deleteMessageBatchRequest);
        } catch (AmazonClientException e) {
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
    public SendMessageResult sendMessage(SendMessageRequest sendMessageRequest) throws JMSException {
        try {
            return amazonSQSClient.sendMessage(sendMessageRequest);
        } catch (AmazonClientException e) {
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
            amazonSQSClient.getQueueUrl(new GetQueueUrlRequest(queueName));
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (AmazonClientException e) {
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
            GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
            getQueueUrlRequest.setQueueOwnerAWSAccountId(queueOwnerAccountId);
            amazonSQSClient.getQueueUrl(getQueueUrlRequest);
            return true;
        } catch (QueueDoesNotExistException e) {
            return false;
        } catch (AmazonClientException e) {
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
    public GetQueueUrlResult getQueueUrl(String queueName) throws JMSException {
        return getQueueUrl(new GetQueueUrlRequest(queueName));
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
    public GetQueueUrlResult getQueueUrl(String queueName, String queueOwnerAccountId) throws JMSException {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
        getQueueUrlRequest.setQueueOwnerAWSAccountId(queueOwnerAccountId);
        return getQueueUrl(getQueueUrlRequest);
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
    public GetQueueUrlResult getQueueUrl(GetQueueUrlRequest getQueueUrlRequest) throws JMSException {
        try {
            return amazonSQSClient.getQueueUrl(getQueueUrlRequest);
        } catch (AmazonClientException e) {
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
    public CreateQueueResult createQueue(String queueName) throws JMSException {
        try {
            return amazonSQSClient.createQueue(queueName);
        } catch (AmazonClientException e) {
            throw handleException(e, "createQueue");   
        }    
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
    public CreateQueueResult createQueue(CreateQueueRequest createQueueRequest) throws JMSException {
        try {
            return amazonSQSClient.createQueue(createQueueRequest);
        } catch (AmazonClientException e) {
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
    public ReceiveMessageResult receiveMessage(ReceiveMessageRequest receiveMessageRequest) throws JMSException {
        try {
            return amazonSQSClient.receiveMessage(receiveMessageRequest);
        } catch (AmazonClientException e) {
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
            amazonSQSClient.changeMessageVisibility(changeMessageVisibilityRequest);
        } catch (AmazonClientException e) {
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
    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest)
            throws JMSException {
        try {
            return amazonSQSClient.changeMessageVisibilityBatch(changeMessageVisibilityBatchRequest);
        } catch (AmazonClientException e) {
            throw handleException(e, "changeMessageVisibilityBatch");
        }
    }

    /**
     * Create generic error message for <code>AmazonServiceException</code>. Message include
     * Action, RequestId, HTTPStatusCode, and AmazonErrorCode.
     */
    private String logAndGetAmazonServiceException(AmazonServiceException ase, String action) {
        String errorMessage = "AmazonServiceException: " + action + ". RequestId: " + ase.getRequestId() +
                              "\nHTTPStatusCode: " + ase.getStatusCode() + " AmazonErrorCode: " +
                              ase.getErrorCode();
        LOG.error(errorMessage, ase);
        return errorMessage;
    }

    /**
     * Create generic error message for <code>AmazonClientException</code>. Message include
     * Action.
     */
    private String logAndGetAmazonClientException(AmazonClientException ace, String action) {
        String errorMessage = "AmazonClientException: " + action + ".";
        LOG.error(errorMessage, ace);
        return errorMessage;
    }
    
    private JMSException handleException(AmazonClientException e, String operationName) throws JMSException {
        JMSException jmsException;
        if (e instanceof AmazonServiceException) {
            AmazonServiceException se = ( AmazonServiceException ) e;
            
            if (e instanceof QueueDoesNotExistException) {
                jmsException = new InvalidDestinationException(
                        logAndGetAmazonServiceException(se, operationName), se.getErrorCode());
            } else if (isJMSSecurityException(se)) {
                jmsException = new JMSSecurityException(
                        logAndGetAmazonServiceException(se, operationName), se.getErrorCode());
            } else {
                jmsException = new JMSException(
                        logAndGetAmazonServiceException(se, operationName), se.getErrorCode());
            }

        } else {
            jmsException = new JMSException(logAndGetAmazonClientException(e, operationName));
        }
        jmsException.initCause(e);
        return jmsException;
    }
    
    private boolean isJMSSecurityException(AmazonServiceException e) {
        return SECURITY_EXCEPTION_ERROR_CODES.contains(e.getErrorCode()) ;
    }
}