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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


import javax.jms.IllegalStateException;

import javax.jms.Connection;

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.sqsjms.acknowledge.AcknowledgeMode;

public class SQSConnection implements Connection, QueueConnection {
    private static final Log LOG = LogFactory.getLog(SQSConnection.class);
    
    /** For now this doesn't do anything. */
    private ExceptionListener exceptionListener;
    /** For now this doesn't do anything. */
    private String clientID;


    /** Used for interactions with connection state */
    private final Object stateLock = new Object();
    
    private final AmazonSQSClientJMSWrapper amazonSQSClient;
    private final int numberOfMessagesToPrefetch;
    
    private volatile boolean closed = false;
    private volatile boolean closing = false;
    private volatile boolean running = false;
    
    /**
     * Used to determine if any other action was taken on the
     * connection, that might prevent setting the clientId
     */
    private volatile boolean actionOnConnectionTaken = false;

    private final Set<Session> sessions = Collections.newSetFromMap(new ConcurrentHashMap<Session, Boolean>());

    SQSConnection(AmazonSQSClientJMSWrapper amazonSQSClientJMSWrapper, int numberOfMessagesToPrefetch) {
        amazonSQSClient = amazonSQSClientJMSWrapper;
        this.numberOfMessagesToPrefetch = numberOfMessagesToPrefetch;

    }
    
    /**
     * Get the AmazonSQSClient used by this connection. This can be used to do administrative operations
     * that aren't included in the JMS specification, e.g. creating new queues.
     * 
     * @return the AmazonSQSClient used by this connection
     */
    public AmazonSQS getAmazonSQSClient() {
        return amazonSQSClient.getAmazonSQSClient();
    }

    /**
     * Get a wrapped version of the AmazonSQSClient used by this connection. The wrapper transforms 
     * all exceptions from the client into JMSExceptions so that it can more easily be used
     * by existing code that already expects JMSExceptions. This client can be used to do 
     * administrative operations that aren't included in the JMS specification, e.g. creating new queues.
     * 
     * @return  wrapped version of the AmazonSQSClient used by this connection
     */
    public AmazonSQSClientJMSWrapper getWrappedAmazonSQSClient() {
        return amazonSQSClient;        
    }
    
    int getNumberOfMessagesToPrefetch() {
        return numberOfMessagesToPrefetch;
    }
    
    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (QueueSession) createSession(transacted, acknowledgeMode);
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosed();
        actionOnConnectionTaken = true;
        if (transacted || acknowledgeMode == Session.SESSION_TRANSACTED)
            throw new JMSException("SQSSession does not support transacted");

        SQSSession sqsSession;
        if (acknowledgeMode == Session.AUTO_ACKNOWLEDGE) {
            sqsSession = new SQSSession(this, AcknowledgeMode.ACK_AUTO.withOriginalAcknowledgeMode(acknowledgeMode));
        } else if (acknowledgeMode == Session.CLIENT_ACKNOWLEDGE || acknowledgeMode == Session.DUPS_OK_ACKNOWLEDGE) {
            sqsSession = new SQSSession(this, AcknowledgeMode.ACK_RANGE.withOriginalAcknowledgeMode(acknowledgeMode));
        } else if (acknowledgeMode == SQSSession.UNORDERED_ACKNOWLEDGE) {
            sqsSession = new SQSSession(this, AcknowledgeMode.ACK_UNORDERED.withOriginalAcknowledgeMode(acknowledgeMode));
        } else {
            LOG.error("Unrecognized acknowledgeMode. Cannot create Session.");
            throw new JMSException("Unrecognized acknowledgeMode. Cannot create Session.");
        }
        synchronized (stateLock) { 
            checkClosing();
            sessions.add(sqsSession);

            /**
             * Any new sessions created on a started connection should be
             * started on creation
             */
            if (running) {
                sqsSession.start();
            }
        }
               
        return sqsSession;
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        checkClosing();
        return exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        checkClosing();
        actionOnConnectionTaken = true;
        this.exceptionListener = listener;
    }
    
    public void checkClosing() throws IllegalStateException {
        if (closing) {
            throw new IllegalStateException("Connection is closed or closing");
        }
    }
    
    public void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Connection is closed");
        }
    }

    @Override
    public void start() throws JMSException {
        checkClosed();
        actionOnConnectionTaken = true;
        /**
         * A call to start on a connection that has already been started is
         * ignored.
         */
        if (running) {
            return;
        }
        synchronized (stateLock) {
            checkClosing();
            if (!running) {
                try {
                    for (Session session : sessions) {
                        SQSSession sqsSession = (SQSSession) session;
                        sqsSession.start();
                    }
                } finally {
                    running = true;
                }
            }
        }
    }

    @Override
    public void stop() throws JMSException {
        checkClosed();
                
        if (!running) {
            return;
        }
        
        actionOnConnectionTaken = true;
        
        if (SQSSession.SESSION_THREAD_FACTORY.wasThreadCreatedWithThisThreadGroup(Thread.currentThread())) {
            throw new IllegalStateException(
                    "MessageListener must not attempt to stop its own Connection to prevent potential deadlock issues");
        }

        synchronized (stateLock) {
            checkClosing();
            if (running) {
                try {
                    for (Session session : sessions) {
                        SQSSession sqsSession = (SQSSession) session;
                        /**
                         * Session stop call blocks until receives and/or
                         * message listeners in progress have completed.
                         */
                        sqsSession.stop();
                    }
                } finally {
                    running = false;
                }

            }
        }
    }
    
    @Override
    public void close() throws JMSException {

        if (closed) {
            return;
        }

        /**
         * A message listener must not attempt to close its own connection as
         * this would lead to deadlock.
         */
        if (SQSSession.SESSION_THREAD_FACTORY.wasThreadCreatedWithThisThreadGroup(Thread.currentThread())) {
            throw new IllegalStateException(
                    "MessageListener must not attempt to close its own Connection to prevent potential deadlock issues");
        }

        boolean shouldClose = false;
        synchronized (stateLock) {
            if (!closing) {
                shouldClose = true;
                closing = true;
            }
        }

        if (shouldClose) {
            synchronized (stateLock) {
                try {
                    for (Session session : sessions) {
                        SQSSession sqsSession = (SQSSession) session;
                        sqsSession.close();
                    }
                    sessions.clear();
                } finally {
                    closed = true;
                    stateLock.notifyAll();

                }
            }
        }/** Blocks until closing of the connection completes */
        else {
            synchronized (stateLock) {
                while (!closed) {
                    try {
                        stateLock.wait();
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted while waiting the session to close.", e);
                    }
                }
            }
        }

    } 

    
    /**
     * This is used in Session. When Session is closed it will remove itself
     * from list of Sessions.
     */
    void removeSession(Session session) throws JMSException {
        /**
         * No need to synchronize on stateLock assuming this can be only called
         * by session.close(), on which point connection will not be worried
         * about missing closing this session.
         */
        sessions.remove(session);
    }
    

    @Override
    public String getClientID() throws JMSException {
        checkClosing();
        return clientID;
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        checkClosing();
        if (clientID == null || clientID.isEmpty()) {
            throw new InvalidClientIDException("ClientID is empty");
        }
        if (this.clientID != null) {
            throw new IllegalStateException("ClientID is already set");
        }
        if (actionOnConnectionTaken) {
            throw new IllegalStateException(
                    "Client ID cannot be set after any action on the connection is taken");
        }
        this.clientID = clientID;
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosing();
        return SQSJMSClientConstants.CONNECTION_METADATA;
    }

    /** This method is not supported. */
    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool,
            int maxMessages) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
            throws JMSException {
        throw new JMSException(SQSJMSClientConstants.UNSUPPORTED_METHOD);
    }

    /*
     * Unit Test Utility Functions
     */
    void setClosed(boolean closed) {
        this.closed = closed;
    }

    void setClosing(boolean closing) {
        this.closing = closing;
    }

    void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    public void setActionOnConnectionTaken(boolean actionOnConnectionTaken) {
        this.actionOnConnectionTaken = actionOnConnectionTaken;
    }

    public boolean isActionOnConnectionTaken() {
        return actionOnConnectionTaken;
    }

    public Set<Session> getSessions() {
        return sessions;
    }

    public Object getStateLock() {
        return stateLock;
    }
}
