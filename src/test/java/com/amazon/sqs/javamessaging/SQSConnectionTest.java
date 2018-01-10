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

import javax.jms.*;
import javax.jms.IllegalStateException;

import org.junit.Before;
import org.junit.Test;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSQueueDestination;
import com.amazon.sqs.javamessaging.SQSSession;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test the SQSConnectionTest class
 */
public class SQSConnectionTest {

    public static final String QUEUE_URL = "QueueUrl";
    public static final String QUEUE_NAME = "QueueName";

    private AmazonSQSMessagingClientWrapper amazonSQSClientJMSWrapper;
    private SQSConnection sqsConnection;
    private SQSQueueDestination destination;
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
    private SQSSession session1;
    private SQSSession session2;

    @Before
    public void setup() throws JMSException {

        destination = new SQSQueueDestination(QUEUE_NAME, QUEUE_URL);

        int numberOfMessagesToPrefetch = 10;
        amazonSQSClientJMSWrapper = mock(AmazonSQSMessagingClientWrapper.class);
        sqsConnection = spy(new SQSConnection(amazonSQSClientJMSWrapper, numberOfMessagesToPrefetch));

        session1 = mock(SQSSession.class);
        session2 = mock(SQSSession.class);
        sqsConnection.getSessions().add(session1);
        sqsConnection.getSessions().add(session2);
    }

    /**
     * Test unsupported features
     */
    @Test
    public void testUnsupportedFeatures() {

        try {
            sqsConnection.createConnectionConsumer(destination, "messageSelector", null, 10);
            fail();
        } catch (JMSException jmse) {
            // expected
        }

        try {
            Topic topic = mock(Topic.class);
            sqsConnection.createDurableConnectionConsumer(topic, "subscriptionName", "messageSelector", null, 10);
            fail();
        } catch (JMSException jmse) {
            // expected
        }

        try {
            Queue queue = mock(Queue.class);
            sqsConnection.createConnectionConsumer(queue, "messageSelector", null, 10);
            fail();
        } catch (JMSException jmse) {
            // expected
        }
    }

    /**
     * Test set client id when connection is closing
     */
    @Test
    public void testSetClientIdWhenClosing() throws JMSException {

        sqsConnection.setClosing(true);

        try {
            sqsConnection.setClientID("clientId");
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("Connection is closed or closing", ise.getMessage());
        }
    }

    /**
     * Test set client id on an invalid client id
     */
    @Test
    public void testSetClientIdInvalidClientId() throws JMSException {

        try {
            sqsConnection.setClientID(null);
            fail();
        } catch (InvalidClientIDException ice) {
            assertEquals("ClientID is empty", ice.getMessage());
        }

        try {
            sqsConnection.setClientID("");
            fail();
        } catch (InvalidClientIDException ice) {
            assertEquals("ClientID is empty", ice.getMessage());
        }
    }

    /**
     * Test set client id when action on connection is already made
     */
    @Test
    public void testSetClientIdActionTaken() throws JMSException {

        sqsConnection.setActionOnConnectionTaken(true);
        try {
            sqsConnection.setClientID("id");
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("Client ID cannot be set after any action on the connection is taken", ise.getMessage());
        }
    }

    /**
     * Test set client id
     */
    @Test
    public void testSetClientId() throws JMSException {

        sqsConnection.setClientID("id");
        try {
            sqsConnection.setClientID("id2");
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("ClientID is already set", ise.getMessage());
        }

        assertEquals("id", sqsConnection.getClientID());
    }

    /**
     * Test closing
     */
    @Test
    public void testClosing() throws JMSException {

        sqsConnection.checkClosing();

        sqsConnection.setClosing(true);
        try {
            sqsConnection.checkClosing();
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("Connection is closed or closing", ise.getMessage());
        }
    }

    /**
     * Test check closed
     */
    @Test
    public void testCheckClosed() throws JMSException {

        sqsConnection.checkClosed();

        sqsConnection.setClosed(true);
        try {
            sqsConnection.checkClosed();
            fail();
        } catch (IllegalStateException ise) {
            assertEquals("Connection is closed", ise.getMessage());
        }
    }

    /**
     * Test exception listener
     */
    @Test
    public void testExceptionListener() throws JMSException {

        ExceptionListener listener = mock(ExceptionListener.class);

        sqsConnection.setExceptionListener(listener);

        assertTrue(sqsConnection.isActionOnConnectionTaken());
        assertEquals(listener, sqsConnection.getExceptionListener());
    }

    /**
     * Test close when connection is already closed
     */
    @Test
    public void testCloseWhenAlreadyClosed() throws JMSException {

        /*
         * Set up connection
         */
        sqsConnection.setClosed(true);

        /*
         * Do close
         */
        sqsConnection.close();

        /*
         * Verify results
         */
        verify(session1, never()).close();
        verify(session2, never()).close();
    }

    /**
     * Test close when connection is closing
     */
    @Test
    public void testCloseWhenClosing() throws JMSException, InterruptedException {

        /*
         * Set up connection and mocks
         */
        sqsConnection.setClosing(true);
        final CountDownLatch beforeCloseCall= new CountDownLatch(1);
        final CountDownLatch passedCloseCall = new CountDownLatch(1);

        /*
         * call close in different thread
         */
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeCloseCall.countDown();
                    sqsConnection.close();
                    passedCloseCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        // Yield execution to allow the connection to wait
        assertEquals(true, beforeCloseCall.await(10, TimeUnit.SECONDS));
        Thread.sleep(10);

        // Release the lock and ensure that we are still waiting since the did not run
        synchronized (sqsConnection.getStateLock()) {
            sqsConnection.getStateLock().notifyAll();
        }
        assertEquals(false, passedCloseCall.await(2, TimeUnit.SECONDS));

        // Simulate connection closed
        sqsConnection.setClosed(true);

        synchronized (sqsConnection.getStateLock()) {
            sqsConnection.getStateLock().notifyAll();
        }
        passedCloseCall.await();

        verify(session1, never()).close();
        verify(session2, never()).close();
    }

    /**
     * Test close
     */
    @Test
    public void testClose() throws JMSException, InterruptedException {

        /*
         * Close
         */
        sqsConnection.close();

        /*
         * Verify results
         */
        verify(session1).close();
        verify(session1).close();

        assertTrue(sqsConnection.getSessions().isEmpty());
    }

    /**
     * Test close from the callback thread
     */
    @Test
    public void testCloseThreadGroup() throws JMSException, InterruptedException {

        final AtomicBoolean flag = new AtomicBoolean(false);
        final CountDownLatch passedCloseCall = new CountDownLatch(1);

        Thread t = SQSSession.SESSION_THREAD_FACTORY.newThread(new Runnable() {
            @Override
            public void run() {
                try {
                    sqsConnection.close();
                } catch (IllegalStateException e) {
                    flag.set(true);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                passedCloseCall.countDown();
            }
        });

        t.start();

        /*
         * Verify results
         */
        passedCloseCall.await();
        assertTrue(flag.get());
    }

    /**
     * Test stop is a no op if already closed
     */
    @Test
    public void testStopNoOpIfAlreadyClosed() throws JMSException {

        /*
         * Set up connection
         */
        sqsConnection.close();

        /*
         * stop consumer
         */
        try {
            sqsConnection.stop();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Connection is closed", ise.getMessage());
        }

        /*
         * Verify results
         */
        verify(session1, never()).stop();
        verify(session2, never()).stop();
    }

    /**
     * Test stop is a no op if already closed
     */
    @Test
    public void testStopNoOpIfNotRunning() throws JMSException {

        /*
         * Set up connection
         */
        sqsConnection.setRunning(false);

        /*
         * stop connection
         */
        sqsConnection.stop();

        /*
         * Verify results
         */
        verify(session1, never()).stop();
        verify(session2, never()).stop();
    }

    /**
     * Test close from the callback thread
     */
    @Test
    public void testStopThreadGroup() throws JMSException, InterruptedException {

        /*
         * Set up connection
         */
        final AtomicBoolean flag = new AtomicBoolean(false);
        final CountDownLatch passedStopCall = new CountDownLatch(1);
        sqsConnection.setRunning(true);

        Thread t = SQSSession.SESSION_THREAD_FACTORY.newThread(new Runnable() {
            @Override
            public void run() {

                try {
                    sqsConnection.close();
                } catch (IllegalStateException e) {
                    flag.set(true);
                } catch (JMSException e) {
                    e.printStackTrace();
                }
                passedStopCall.countDown();
            }
        });

        t.start();

        /*
         * Verify results
         */
        passedStopCall.await();
        assertTrue(flag.get());
        verify(session1, never()).stop();
        verify(session2, never()).stop();
    }

    /**
     * Test stop when connection is closing
     */
    @Test
    public void testStopWhenClosing() throws JMSException, InterruptedException {

        /*
         * Set up connection
         */
        sqsConnection.setClosing(true);
        sqsConnection.setRunning(true);

        try {
            sqsConnection.stop();
            fail();
        } catch (IllegalStateException e) {
            //expected
        }

        /*
         * Verify results
         */
        verify(session1, never()).stop();
        verify(session2, never()).stop();
    }

    /**
     * Test stop blocks on state lock
     */
    @Test
    public void testStopBlocksOnStateLock() throws InterruptedException, IllegalStateException {

        /*
         * Set up the latches and mocks
         */
        final CountDownLatch mainRelease = new CountDownLatch(1);
        final CountDownLatch holdStateLock = new CountDownLatch(1);
        final CountDownLatch beforeConnectionStopCall = new CountDownLatch(1);
        final CountDownLatch passedConnectionStopCall = new CountDownLatch(1);
        sqsConnection.setRunning(true);

        // Run a thread to hold the stateLock
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (sqsConnection.getStateLock()) {
                        holdStateLock.countDown();
                        mainRelease.await();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to start the connection while state lock is been held
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeConnectionStopCall.countDown();
                    sqsConnection.stop();
                    passedConnectionStopCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        beforeConnectionStopCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock
        assertEquals(false, passedConnectionStopCall.await(2, TimeUnit.SECONDS));

        // Release the thread holding the state lock
        mainRelease.countDown();

        // Ensure that the session start completed
        passedConnectionStopCall.await();

        verify(session1).stop();
        verify(session2).stop();
        assertFalse(sqsConnection.isRunning());
    }

    /**
     * Test start is a no op if already closed
     */
    @Test
    public void testStartNoOpIfAlreadyClosed() throws JMSException {

        /*
         * Set up conenction
         */
        sqsConnection.close();

        /*
         * Start connection
         */
        try {
            sqsConnection.start();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Connection is closed", ise.getMessage());
        }

        /*
         * Verify results
         */
        verify(session1, never()).start();
        verify(session2, never()).start();
    }

    /**
     * Test start is a no op if closing
     */
    @Test
    public void testStartNoOpIfClosing() throws JMSException {

        /*
         * Set up session
         */
        sqsConnection.setClosing(true);

        /*
         * Start connection
         */
        try {
            sqsConnection.start();
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Connection is closed or closing", ise.getMessage());
        }

        /*
         * Verify results
         */
        verify(session1, never()).start();
        verify(session2, never()).start();
    }

    /**
     * Test start is a no op if closing
     */
    @Test
    public void testStartNoOpIfRunning() throws JMSException {

        /*
         * Set up session
         */
        sqsConnection.setRunning(true);

        /*
         * Start connection
         */
        sqsConnection.start();

        /*
         * Verify results
         */
        verify(session1, never()).start();
        verify(session2, never()).start();
    }

    /**
     * Test start blocks on state lock
     */
    @Test
    public void testStartBlocksOnStateLock() throws InterruptedException, IllegalStateException {

        /*
         * Set up the latches and mocks
         */
        final CountDownLatch mainRelease = new CountDownLatch(1);
        final CountDownLatch holdStateLock = new CountDownLatch(1);
        final CountDownLatch beforeConnectionStartCall = new CountDownLatch(1);
        final CountDownLatch passedConnectionStartCall = new CountDownLatch(1);

        // Run a thread to hold the stateLock
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (sqsConnection.getStateLock()) {
                        holdStateLock.countDown();
                        mainRelease.await();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to start the connection while state lock is been held
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeConnectionStartCall.countDown();
                    sqsConnection.start();
                    passedConnectionStartCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        beforeConnectionStartCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock
        assertEquals(false, passedConnectionStartCall.await(2, TimeUnit.SECONDS));

        // Release the thread holding the state lock
        mainRelease.countDown();

        // Ensure that the connection start completed
        passedConnectionStartCall.await();

        verify(session1).start();
        verify(session2).start();
        assertTrue(sqsConnection.isRunning());
    }

    /**
     * Test create session is a no op if already closed
     */
    @Test
    public void testCreateSessionNoOpIfAlreadyClosed() throws JMSException {

        /*
         * Set up connection
         */
        sqsConnection.setClosed(true);

        /*
         * Create session
         */
        try {
            sqsConnection.createSession(true, 1);
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Connection is closed", ise.getMessage());
        }

        try {
            sqsConnection.createSession(false, 1);
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Connection is closed", ise.getMessage());
        }

        /*
         * Verify results
         */
        verify(session1, never()).start();
        verify(session2, never()).start();
    }

    /**
     * Test create session throws correct exception when using unsupported features
     */
    @Test
    public void testCreateSessionUnsupportedFeatures() throws JMSException {

        try {
            sqsConnection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            fail();
        } catch(JMSException ise) {
            assertEquals("SQSSession does not support transacted", ise.getMessage());
        }

        try {
            sqsConnection.createSession(false, Session.SESSION_TRANSACTED);
            fail();
        } catch(JMSException ise) {
            assertEquals("SQSSession does not support transacted", ise.getMessage());
        }

        /*
         * Verify results
         */
        verify(session1, never()).start();
        verify(session2, never()).start();
    }

    /**
     * Test create session when connection is closing
     */
    @Test
    public void testCreateSessionWhenClosing() throws JMSException {

        /*
         * Set up connection
         */
        sqsConnection.setClosing(true);

        /*
         * Start connection
         */
        try {
            sqsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail();
        } catch(IllegalStateException ise) {
            assertEquals("Connection is closed or closing", ise.getMessage());
        }

        /*
         * Verify results
         */
        assertEquals(2, sqsConnection.getSessions().size());
    }

    /**
     * Test create session
     */
    @Test
    public void testCreateSessionUnknownAckMode() throws JMSException {

        /*
         * Craete session
         */
        try {
            sqsConnection.createSession(false, 42);
        } catch (JMSException jmse) {
            assertEquals("Unrecognized acknowledgeMode. Cannot create Session.", jmse.getMessage());
        }


        /*
         * Verify results
         */
        assertEquals(2, sqsConnection.getSessions().size());
    }

    /**
     * Test create session when connection running
     */
    @Test
    public void testCreateSessionWhenConnectionRunning() throws JMSException {

        sqsConnection.setRunning(true);

        SQSSession session = (SQSSession)sqsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertTrue(session.isRunning());

        session = (SQSSession) sqsConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertEquals(Session.CLIENT_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertTrue(session.isRunning());

        session = (SQSSession) sqsConnection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        assertEquals(Session.DUPS_OK_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertTrue(session.isRunning());

        session = (SQSSession) sqsConnection.createSession(false, SQSSession.UNORDERED_ACKNOWLEDGE);
        session.isRunning();
        assertEquals(SQSSession.UNORDERED_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertTrue(session.isRunning());

        /*
         * Verify results
         */
        assertEquals(6, sqsConnection.getSessions().size());
    }

    /**
     * Test create session when connection running
     */
    @Test
    public void testCreateSessionWhenConnectionStopped() throws JMSException {

        sqsConnection.setRunning(false);

        SQSSession session = (SQSSession)sqsConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertFalse(session.isRunning());

        session = (SQSSession) sqsConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertEquals(Session.CLIENT_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertFalse(session.isRunning());

        session = (SQSSession) sqsConnection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        assertEquals(Session.DUPS_OK_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertFalse(session.isRunning());

        session = (SQSSession) sqsConnection.createSession(false, SQSSession.UNORDERED_ACKNOWLEDGE);
        session.isRunning();
        assertEquals(SQSSession.UNORDERED_ACKNOWLEDGE, session.getAcknowledgeMode());
        assertEquals(sqsConnection, session.getParentConnection());
        assertTrue(sqsConnection.getSessions().contains(session));
        assertFalse(session.isRunning());

        /*
         * Verify results
         */
        assertEquals(6, sqsConnection.getSessions().size());
    }

    /**
     * Test CreateSession blocks on state lock
     */
    @Test
    public void testCreateSessionBlocksOnStateLock() throws InterruptedException, IllegalStateException {

        /*
         * Set up the latches and mocks
         */
        final CountDownLatch mainRelease = new CountDownLatch(1);
        final CountDownLatch holdStateLock = new CountDownLatch(1);
        final CountDownLatch beforeCreateSessionStartCall = new CountDownLatch(1);
        final CountDownLatch passedCreateSessionStartCall = new CountDownLatch(1);

        // Run a thread to hold the stateLock
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    synchronized (sqsConnection.getStateLock()) {
                        holdStateLock.countDown();
                        mainRelease.await();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Waiting for the thread to hold state lock
        holdStateLock.await();

        // Run another thread that tries to start the connection while state lock is been held
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    beforeCreateSessionStartCall.countDown();
                    sqsConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                    passedCreateSessionStartCall.countDown();
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        beforeCreateSessionStartCall.await();
        Thread.sleep(10);

        // Ensure that we wait on state lock
        assertEquals(false, passedCreateSessionStartCall.await(2, TimeUnit.SECONDS));
        assertEquals(2, sqsConnection.getSessions().size());

        // Release the thread holding the state lock
        mainRelease.countDown();

        // Ensure that the session start completed
        passedCreateSessionStartCall.await();

        assertEquals(3, sqsConnection.getSessions().size());
    }
}