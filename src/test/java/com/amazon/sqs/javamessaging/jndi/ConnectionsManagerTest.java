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

import javax.naming.directory.InvalidAttributeValueException;

import org.junit.Before;
import org.junit.Test;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;

public class ConnectionsManagerTest {
	private static final int POOLING_LENGTH = 2 * 5;
	private SQSConnectionFactory connectionFactory;
	
	@Before
	public void setUp() throws Exception {
		connectionFactory = mock(SQSConnectionFactory.class);
		
		when(connectionFactory.createConnection()).thenReturn(mock(SQSConnection.class),mock(SQSConnection.class),
			mock(SQSConnection.class),mock(SQSConnection.class),mock(SQSConnection.class),mock(SQSConnection.class),
			mock(SQSConnection.class),mock(SQSConnection.class),mock(SQSConnection.class),mock(SQSConnection.class));
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
	
	@Test
	public void testClose() throws Exception {
		HashSet<SQSConnection> connections = new HashSet<SQSConnection>();
		final ConnectionsManager connectionsManager = new ConnectionsManager(connectionFactory);
		
		connections.add(connectionsManager.getLazyDefaultConnection());
		assertEquals(1,connections.size());
		
		connectionsManager.close();
		
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
		final int poolingLength = POOLING_LENGTH / 2;
		
		callables.addAll(Collections.nCopies(poolingLength,new Callable<CallableReturn>() {
			@Override
			public CallableReturn call() throws Exception {
				connectionsManager.close();
				return new CallableReturn(true);
			}
		}));
		
		callables.addAll(Collections.nCopies(2 * poolingLength,new Callable<CallableReturn>() {
			@Override
			public CallableReturn call() throws Exception {
				return new CallableReturn(connectionsManager.getLazyDefaultConnection());
			}
		}));
		
		assertEquals(3 * poolingLength,callables.size());
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
		
		assertEquals(poolingLength,closedCallables.size());
		assertTrue(1 + poolingLength >= connections.size());
	}
}
