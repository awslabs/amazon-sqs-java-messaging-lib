package com.amazon.sqs.javamessaging.jndi;

import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.directory.InvalidAttributeValueException;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;

/**
 * Manage the use of {@link SQSConnection connections} and their closings
 * through an {@link SQSConnectionFactory} instance.
 * 
 * @author krloss
 * @since 1.1.0
 */
public class ConnectionsManager {
	/**
	 * Set of connection configuration parameters.<br>
	 * Externally visible information.
	 */
	protected final SQSConnectionFactory connectionFactory;
	
	private final HashSet<Callable<Boolean>> closeableConnections = new HashSet<>();
	private SQSConnection defaultConnection;
	
    private final Object stateLock = new Object(); // Used for interactions with connection state.
	
	/**
	 * Public constructor that requires {@link SQSConnectionFactory} parameter.
	 * 
	 * @param connectionFactory - set of connection configuration parameters.
	 * @throws NamingException
	 */
	public ConnectionsManager(final SQSConnectionFactory connectionFactory) throws InvalidAttributeValueException {
		if(connectionFactory == null ) throw new InvalidAttributeValueException("ConnectionsManager Requires SQSConnectionFactory.");
		
		this.connectionFactory = connectionFactory;
	}
	
	private static final Callable<Boolean> createCloseableConnection(final SQSConnection connection) {
		return (new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				connection.close();
				return true;
			}
		});
	}
	
	/**
	 * Creates and returns a new connection.
	 * 
	 * @return {@link SQSConnection}
	 * @throws JMSException
	 */
	public SQSConnection createConnection() throws JMSException {
		SQSConnection connection = connectionFactory.createConnection();
		
		synchronized(stateLock) {
			closeableConnections.add(createCloseableConnection(connection));
		}
		
		return connection;
	}
	
	/**
	 * Get default connection lazily.
	 * 
	 * @return {@link SQSConnection}
	 * @throws JMSException
	 */
	public synchronized SQSConnection getLazyDefaultConnection() throws JMSException {
		if(defaultConnection == null) defaultConnection = createConnection();
		
		return defaultConnection;
	}
	
	private void close(ExecutorService executor) throws InterruptedException {
		synchronized(stateLock) {
			defaultConnection = null;
			executor.invokeAll(closeableConnections);
			closeableConnections.clear();
		}
	}
	
	/**
	 * Manage the closing of {@link SQSConnection connections} through asynchronous tasks using a thread pool.
	 * 
	 * @throws JMSException
	 * @see Executors#newCachedThreadPool()
	 */
	public synchronized void close() throws JMSException {
		ExecutorService executor = Executors.newCachedThreadPool();
		
		try {
			close(executor);
		}
		catch(InterruptedException ie) {
			throw new IllegalStateException(ie.getMessage());
		}
		finally {
			executor.shutdown();
		}
	}
}
