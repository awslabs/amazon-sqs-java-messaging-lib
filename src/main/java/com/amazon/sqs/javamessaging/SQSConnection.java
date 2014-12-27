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

import com.amazon.sqs.javamessaging.acknowledge.AcknowledgeMode;
import com.amazonaws.services.sqs.AmazonSQS;

/**
 * This is a logical connection entity, which encapsulates the logic to create
 * sessions.
 * <P>
 * Supports concurrent use, but the session objects it creates do no support
 * concurrent use.
 * <P>
 * The authentication does not take place with the creation of connection. It
 * takes place when the <code>amazonSQSClient</code> is used to call any SQS
 * API.
 * <P>
 * The physical connections are handled by the underlying
 * <code>amazonSQSClient</code>.
 * <P>
 * A JMS client typically creates a connection, one or more sessions, and a
 * number of message producers and consumers. When a connection is created, it
 * is in stopped mode. That means that no messages are being delivered, but
 * message producer can send messages while a connection is stopped.
 * <P>
 * Although the connection can be started immediately, it is typical to leave
 * the connection in stopped mode until setup is complete (that is, until all
 * message consumers have been created). At that point, the client calls the
 * connection's <code>start</code> method, and messages begin arriving at the
 * connection's consumers. This setup convention minimizes any client confusion
 * that may result from asynchronous message delivery while the client is still
 * in the process of setting itself up.
 * <P>
 * A connection can be started immediately, and the setup can be done
 * afterwards. Clients that do this must be prepared to handle asynchronous
 * message delivery while they are still in the process of setting up.
 * <P>
 * Transacted sessions are not supported.
 * <P>
 * Exception listener on connection is not supported.
 */
public class SQSConnection implements Connection, QueueConnection {
    private static final Log LOG = LogFactory.getLog(SQSConnection.class);
    
    /** For now this doesn't do anything. */
    private ExceptionListener exceptionListener;
    /** For now this doesn't do anything. */
    private String clientID;


    /** Used for interactions with connection state. */
    private final Object stateLock = new Object();
    
    private final AmazonSQSMessagingClientWrapper amazonSQSClient;

    /**
     * Configures the amount of messages that can be prefetched by a consumer. A
     * single consumer cannot prefetch more than 10 messages.
     */
    private final int numberOfMessagesToPrefetch;
    private volatile boolean closed = false;
    private volatile boolean closing = false;

    /** Used to determine if the connection is stopped or not. */
    private volatile boolean running = false;
    
    /**
     * Used to determine if any other action was taken on the
     * connection, that might prevent setting the clientId
     */
    private volatile boolean actionOnConnectionTaken = false;

    private final Set<Session> sessions = Collections.newSetFromMap(new ConcurrentHashMap<Session, Boolean>());

    SQSConnection(AmazonSQSMessagingClientWrapper amazonSQSClientJMSWrapper, int numberOfMessagesToPrefetch) {
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
    public AmazonSQSMessagingClientWrapper getWrappedAmazonSQSClient() {
        return amazonSQSClient;        
    }
    
    int getNumberOfMessagesToPrefetch() {
        return numberOfMessagesToPrefetch;
    }
    
    /**
     * Creates a <code>QueueSession</code>
     * 
     * @param transacted
     *            Only false is supported.
     * @param acknowledgeMode
     *            Legal values are <code>Session.AUTO_ACKNOWLEDGE</code>,
     *            <code>Session.CLIENT_ACKNOWLEDGE</code>,
     *            <code>Session.DUPS_OK_ACKNOWLEDGE</code>, and
     *            <code>SQSSession.UNORDERED_ACKNOWLEDGE</code>
     * @return a new queue session.
     * @throws JMSException
     *             If the QueueConnection object fails to create a session due
     *             to some internal error or lack of support for the specific
     *             transaction and acknowledge mode.
     */
    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (QueueSession) createSession(transacted, acknowledgeMode);
    }
    
    /**
     * Creates a <code>Session</code>
     * 
     * @param transacted
     *            Only false is supported.
     * @param acknowledgeMode
     *            Legal values are <code>Session.AUTO_ACKNOWLEDGE</code>,
     *            <code>Session.CLIENT_ACKNOWLEDGE</code>,
     *            <code>Session.DUPS_OK_ACKNOWLEDGE</code>, and
     *            <code>SQSSession.UNORDERED_ACKNOWLEDGE</code>
     * @return a new session.
     * @throws JMSException
     *             If the QueueConnection object fails to create a session due
     *             to some internal error or lack of support for the specific
     *             transaction and acknowledge mode.
     */
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
    
    /**
     * Checks if the connection close is in-progress or already completed.
     * 
     * @throws IllegalStateException
     *             If the connection close is in-progress or already completed.
     */
    public void checkClosing() throws IllegalStateException {
        if (closing) {
            throw new IllegalStateException("Connection is closed or closing");
        }
    }

    /**
     * Checks if the connection close is already completed.
     * 
     * @throws IllegalStateException
     *             If the connection close is already completed.
     */
    public void checkClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Connection is closed");
        }
    }
    
    /**
     * Starts a connection's delivery of incoming messages. A call to
     * <code>start</code> on a connection that has already been started is
     * ignored.
     * <P>
     * This will not return until all the sessions start internally.
     * 
     * @throws JMSException
     *             On internal error
     */
    @Override
    public void start() throws JMSException {
        checkClosed();
        actionOnConnectionTaken = true;

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
    
    /**
     * Stops a connection's delivery of incoming messages. A call to
     * <code>stop</code> on a connection that has already been stopped is
     * ignored.
     * <P>
     * This will not return until all the sessions stop internally, which blocks
     * until receives and/or message listeners in progress have completed. While
     * these message listeners are completing, they must have the full services
     * of the connection available to them.
     * <P>
     * A call to stop must not return until delivery of messages has paused.
     * This means that a client can rely on the fact that none of its message
     * listeners will be called and that all threads of control waiting for
     * receive calls to return will not return with a message until the
     * connection is restarted. The receive timers for a stopped connection
     * continue to advance, so receives may time out while the connection is
     * stopped.
     * <P>
     * A message listener must not attempt to stop its own connection; otherwise
     * throws a IllegalStateException.
     * 
     * @throws IllegalStateException
     *             If called by a message listener on its own
     *             <code>Connection</code>.
     * @throws JMSException
     *             On internal error or called if close is in progress.
     */
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
    
    /**
     * Closes the connection.
     * <P>
     * This will not return until all the sessions close internally, which
     * blocks until receives and/or message listeners in progress have
     * completed.
     * <P>
     * The receives may return with a message or with null, depending on whether
     * there was a message available at the time of the close. If one or more of
     * the connection's sessions' message listeners is processing a message at
     * the time when connection close is invoked, all the facilities of the
     * connection and its sessions must remain available to those listeners
     * until they return control to the JMS provider.
     * <P>
     * A message listener must not attempt to close its own connection;
     * otherwise throws a IllegalStateException.
     * 
     * @throws IllegalStateException
     *             If called by a message listener on its own
     *             <code>Connection</code>.
     * @throws JMSException
     *             On internal error.
     */
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
    
    /**
     * Gets the client identifier for this connection.
     * 
     * @return client identifier
     * @throws JMSException
     *             If the connection is being closed
     */
    @Override
    public String getClientID() throws JMSException {
        checkClosing();
        return clientID;
    }
    
    /**
     * Sets the client identifier for this connection.
     * <P>
     * Does not verify uniqueness of client ID, so does not detect if another
     * connection is already using the same client ID
     * 
     * @param clientID
     *            The client identifier
     * @throws JMSException
     *             If the connection is being closed
     * @throws InvalidClientIDException
     *             If empty or null client ID is used
     * @throws IllegalStateException
     *             If the client ID is already set or attempted to set after an
     *             action on the connection already took place
     */
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
    
    /**
     * Get the metadata for this connection
     * 
     * @return the connection metadata
     * @throws JMSException
     *             If the connection is being closed
     */
    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        checkClosing();
        return SQSMessagingClientConstants.CONNECTION_METADATA;
    }

    /** This method is not supported. */
    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool,
            int maxMessages) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /** This method is not supported. */
    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String messageSelector, ServerSessionPool sessionPool, int maxMessages)
            throws JMSException {
        throw new JMSException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    /*
     * Unit Test Utility Functions
     */
    void setClosed(boolean closed) {
        this.closed = closed;
    }

    boolean isClosed() {
        return closed;
    }

    void setClosing(boolean closing) {
        this.closing = closing;
    }

    void setRunning(boolean running) {
        this.running = running;
    }

    boolean isRunning() {
        return running;
    }

    void setActionOnConnectionTaken(boolean actionOnConnectionTaken) {
        this.actionOnConnectionTaken = actionOnConnectionTaken;
    }

    boolean isActionOnConnectionTaken() {
        return actionOnConnectionTaken;
    }

    Set<Session> getSessions() {
        return sessions;
    }

    Object getStateLock() {
        return stateLock;
    }
}
