package com.amazon.sqs.javamessaging.jndi;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import javax.jms.Session;
import javax.naming.directory.InvalidAttributeValueException;

import org.junit.Before;
import org.junit.Test;

import com.amazon.sqs.javamessaging.SQSConnection;

public class DestinationResourceTest {
	private final String type = ResourceType.SA.name();
	private final String name = "SQS_Queue-Name_v10";
	
	private ConnectionsManager connectionsManager;
	private Session[] sessions = new Session[ResourceType.values().length];
	
	@Before
	public void setUp() throws Exception {
		SQSConnection newConnection = mock(SQSConnection.class);
		SQSConnection defaultConnection = mock(SQSConnection.class);
		
		for(ResourceType it : ResourceType.values()) {
			sessions[it.ordinal()] = mock(Session.class);
			
			if(it.isSessionPolling)
				when(defaultConnection.createSession(false,it.acknowledgeMode)).thenReturn(sessions[it.ordinal()]);
			else
				when(newConnection.createSession(false,it.acknowledgeMode)).thenReturn(sessions[it.ordinal()]); 
		}
		
		connectionsManager = mock(ConnectionsManager.class);
		when(connectionsManager.createConnection()).thenReturn(newConnection);
		when(connectionsManager.getLazyDefaultConnection()).thenReturn(defaultConnection);
	}
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testNullDestinationResource() throws InvalidAttributeValueException {
		new DestinationResource(null);
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testEmptyDestinationResource() throws InvalidAttributeValueException {
		new DestinationResource("");
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testPrefixDestinationResource() throws InvalidAttributeValueException {
		new DestinationResource(type);
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testSufixDestinationResource() throws InvalidAttributeValueException {
		new DestinationResource(name);
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testDestinationResourceWithoutSeparator() throws InvalidAttributeValueException {
		new DestinationResource(String.format("%s%s",type,name));
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testDestinationResourceWithIncorrectSeparator() throws InvalidAttributeValueException {
		new DestinationResource(String.format("%s@%s",type,name));
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testDestinationResourceWithIncorrectName() throws InvalidAttributeValueException {
		new DestinationResource(String.format("%s:/%s/",type,name));
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testDestinationResourceWithIncorrectType() throws InvalidAttributeValueException {
		new DestinationResource(String.format("AS@%s",name));
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testDestinationResourceWithSeparatorOnly() throws InvalidAttributeValueException {
		new DestinationResource(":");
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testDestinationResourceWithSeparatorAndSpaces() throws InvalidAttributeValueException {
		new DestinationResource(" \n\t : \n\t ");
	}
	
	@Test
	public void testDestinationResource() throws InvalidAttributeValueException {
		DestinationResource resource = new DestinationResource(
			String.format("%s:%s",type,name));
		
		assertEquals(type,resource.type.name());
		assertEquals(name,resource.name);
	}
	@Test
	public void testCompleteDestinationResource() throws InvalidAttributeValueException {
		DestinationResource resource = new DestinationResource(
			String.format(" \n\t %s \n\t : \n\t %s \n\t ",type,name));
		
		assertEquals(type,resource.type.name());
		assertEquals(name,resource.name);
	}
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testNullGetDestination() throws Exception {
		DestinationResource resource = new DestinationResource(
			String.format("%s:%s",type,name));
		
		resource.getDestination(null);
	}
	
	@Test
	public void testGetDestination() throws Exception {
		int poolSize = ResourceType.values().length / 2;
		
		for(ResourceType it : ResourceType.values()) {
			DestinationResource resource = new DestinationResource(String.format("%s:%s",it.name(),name));
			
			resource.getDestination(connectionsManager);
			verify(sessions[it.ordinal()]).createQueue(name);
		}
		
		verify(connectionsManager,times(poolSize)).createConnection();
		verify(connectionsManager,times(poolSize)).getLazyDefaultConnection();
	}
}
