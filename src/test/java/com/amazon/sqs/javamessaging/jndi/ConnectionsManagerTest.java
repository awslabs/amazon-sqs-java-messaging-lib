package com.amazon.sqs.javamessaging.jndi;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jms.IllegalStateException;
import javax.naming.directory.InvalidAttributeValueException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;

public class ConnectionsManagerTest {
	private static final int POOLING_LENGTH = 2 * 5;
	private SQSConnectionFactory connectionFactory;
	
	private static final Answer<SQSConnection> createAnswerConnection() {
		return new Answer<SQSConnection>() {
			@Override
			public SQSConnection answer(InvocationOnMock invocation) throws Throwable {
				Thread.sleep(100);
				return mock(SQSConnection.class);
			}
		};
	}
	
	@Before
	public void setUp() throws Exception {
		connectionFactory = mock(SQSConnectionFactory.class);
		
		when(connectionFactory.createConnection()).thenAnswer(createAnswerConnection());
	}
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testConnectionsManagerWithoutConnectionFactory() throws InvalidAttributeValueException {	
		new ConnectionsManager(null);
	}
	
	@Test
	public void testConnectionsManager() throws InvalidAttributeValueException {
		assertNotNull(new ConnectionsManager(connectionFactory));
	}
	
	@Test
	public void testCreateConnection() throws Exception {
		ConnectionsManager connectionsManager = new ConnectionsManager(connectionFactory);
		
		assertNotNull(connectionsManager.createConnection());
		assertEquals(connectionFactory,connectionsManager.connectionFactory);
	}
	
	@Test
	public void testGetLazyDefaultConnection() throws Exception {
		HashSet<SQSConnection> connections = new HashSet<SQSConnection>();
		final ConnectionsManager connectionsManager = new ConnectionsManager(connectionFactory);
		ExecutorService executor = Executors.newFixedThreadPool(POOLING_LENGTH);
		
		List<Future<SQSConnection>> results = executor.invokeAll(Collections.nCopies(POOLING_LENGTH,
			new Callable<SQSConnection>() {
				@Override
				public SQSConnection call() throws Exception {
					return connectionsManager.getLazyDefaultConnection();
				}
			}
		));
		
		for(Future<SQSConnection> it : results) {
			SQSConnection connection = it.get();
			
			assertNotNull(connection);
			connections.add(connection);
		}
		
		executor.shutdown();
		
		assertEquals(1,connections.size());
	}
	
	@Test
	public void testCloseWithoutConnections() throws Exception {
		ConnectionsManager connectionsManager = new ConnectionsManager(connectionFactory);
		
		connectionsManager.close();
	}
	
	
	@Test(expected = IllegalStateException.class)
	public void testCloseWithInterruptedException() throws Exception {
		SQSConnection connection = mock(SQSConnection.class);
		SQSConnectionFactory connectionFactory = mock(SQSConnectionFactory.class);
		
		doAnswer(new Answer<Boolean>() {
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				for(Thread it : Thread.getAllStackTraces().keySet()) it.interrupt();
				
				return true;
			}
		}).when(connection).close();
		
		when(connectionFactory.createConnection()).thenReturn(connection);
		
		ConnectionsManager connectionsManager = new ConnectionsManager(connectionFactory);
		
		assertEquals(connection,connectionsManager.createConnection());
		connectionsManager.close();
	}
	
	@Test
	public void testClose() throws Exception {
		HashSet<SQSConnection> connections = new HashSet<SQSConnection>();
		final ConnectionsManager connectionsManager = new ConnectionsManager(connectionFactory);
		
		connections.add(connectionsManager.getLazyDefaultConnection());
		assertEquals(1,connections.size());
		
		connectionsManager.close();
		
		for(SQSConnection it : connections) verify(it).close();
		
		connections.add(connectionsManager.getLazyDefaultConnection());
		assertEquals(2,connections.size());
	}
	
	private static final class CallableReturn {
		Boolean isBoolean;
		Boolean booleanReturn;
		SQSConnection connectionReturn;
		
		CallableReturn(Boolean result) {
			isBoolean = true;
			booleanReturn = result;
		}
		CallableReturn(SQSConnection result) {
			isBoolean = false;
			connectionReturn = result;
		}
	}
	@Test
	public void testConcurrentBetweenCloseAndCreateConnection() throws Exception {
		ArrayList<Callable<CallableReturn>> callables =  new ArrayList<Callable<CallableReturn>>();
		final ConnectionsManager connectionsManager = new ConnectionsManager(connectionFactory);
		
		callables.addAll(Collections.nCopies(POOLING_LENGTH,new Callable<CallableReturn>() {
			@Override
			public CallableReturn call() throws Exception {
				connectionsManager.close();
				return new CallableReturn(true);
			}
		}));
		
		callables.addAll(Collections.nCopies(2 * POOLING_LENGTH,new Callable<CallableReturn>() {
			@Override
			public CallableReturn call() throws Exception {
				return new CallableReturn(connectionsManager.getLazyDefaultConnection());
			}
		}));
		
		assertEquals(3 * POOLING_LENGTH,callables.size());
		Collections.shuffle(callables);
		
		ArrayList<Boolean> closedCallables = new ArrayList<Boolean>();
		HashSet<SQSConnection> connections = new HashSet<SQSConnection>();
		ExecutorService executor = Executors.newFixedThreadPool(callables.size());
		List<Future<CallableReturn>> results = executor.invokeAll(callables);
		
		for(Future<CallableReturn> future : results) {
			CallableReturn it = future.get();
			
			if(it.isBoolean && it.booleanReturn) closedCallables.add(it.booleanReturn);
			else connections.add(it.connectionReturn);
		}
		
		executor.shutdown();
		
		assertEquals(POOLING_LENGTH,closedCallables.size());
		assertTrue(1 + POOLING_LENGTH >= connections.size());
	}
}
