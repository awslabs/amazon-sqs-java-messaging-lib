package com.amazon.sqs.javamessaging.jndi;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashSet;

import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.CompositeName;
import javax.naming.NamingException;
import javax.naming.OperationNotSupportedException;
import javax.naming.directory.InvalidAttributeValueException;

import org.junit.Before;
import org.junit.Test;

import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;

public class SQSContextTest {
	private static final Integer LENGTH_QUEUES = 2;
	private static String lookupString;
	private static CompositeName lookupName;
	
	private SQSContext contextForOperationNotSupported;
	private SQSConnectionFactory connectionFactory;
	
	@Before
	public void setUp() throws Exception {
		lookupString = "lookupString";
		lookupName = new CompositeName("lookupName");
		
		contextForOperationNotSupported = new SQSContext(mock(SQSConnectionFactory.class));
		
		connectionFactory = mock(SQSConnectionFactory.class);
		
		Queue[] queues = new Queue[LENGTH_QUEUES];
		Session[] sessions = new Session[LENGTH_QUEUES];
		SQSConnection[] connections = new SQSConnection[LENGTH_QUEUES];
		
		for(Integer i = 0; i < LENGTH_QUEUES; i++) {
			queues[i] = mock(Queue.class);
			sessions[i] = mock(Session.class);
			connections[i] = mock(SQSConnection.class);
			
			for(Integer j = 0; j < LENGTH_QUEUES; j++)
				when(sessions[i].createQueue(j.toString())).thenReturn(queues[i]);
			
			for(ResourceType it : ResourceType.values()) {
				if(it.isSessionPolling)
					when(connections[i].createSession(false,it.acknowledgeMode)).thenReturn(sessions[i]);
			}
		}
		
		when(connectionFactory.createConnection()).thenReturn(connections[0],connections[1]);
	}
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testSQSContextWithoutConnectionFactory() throws NamingException {	
		new SQSContext(null);
	}
	@Test
	public void testSQSContext() throws NamingException {
		assertNotNull(new SQSContext(connectionFactory));
	}
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testLookupIncorrectString() throws NamingException {
		SQSContext context = new SQSContext(connectionFactory);
		
		assertEquals(connectionFactory,context.lookup(""));
	}
	
	@Test
	public void testLookupString() throws NamingException {
		SQSContext context = new SQSContext(connectionFactory);
		
		assertEquals(connectionFactory,context.lookup(SQSConnectionFactory.class.getName()));
	}
	
	@Test
	public void testLookupName() throws NamingException {
		SQSContext context = new SQSContext(connectionFactory);
		
		assertEquals(connectionFactory,context.lookup(new CompositeName(SQSConnectionFactory.class.getName())));
	}
	
	@Test
	public void testClose() throws NamingException {
		SQSContext context = new SQSContext(connectionFactory);
		HashSet<Queue> queues = new HashSet<Queue>();
		
		for(Integer i = 0; i < LENGTH_QUEUES; i++) {
			for(ResourceType it : ResourceType.values()) {
				if(it.isSessionPolling) {
					Object queue = context.lookup(String.format("%s:%d",it.name(),i));
					
					assertNotNull(queue);
					queues.add((Queue)queue);
				}
			}
		}
		
		assertEquals(1,queues.size());
		
		context.close();
		
		for(Integer i = 0; i < LENGTH_QUEUES; i++) {
			for(ResourceType it : ResourceType.values()) {
				if(it.isSessionPolling) {
					Object queue = context.lookup(String.format("%s:%d",it.name(),i));
					
					assertNotNull(queue);
					queues.add((Queue)queue);
				}
			}
		}
		
		assertEquals(2,queues.size());
	}

	@Test
	public void testBindStringObject() throws NamingException {
		SQSContext context = new SQSContext(connectionFactory);
		
		context.bind(lookupString,lookupName);
		assertEquals(lookupName,context.lookup(lookupString));
	}

	@Test
	public void testBindNameObject() throws NamingException {
		SQSContext context = new SQSContext(connectionFactory);
		
		context.bind(lookupName,lookupString);
		assertEquals(lookupString,context.lookup(lookupName));
	}
	
	@Test(expected = OperationNotSupportedException.class)
	public void testGetEnvironment() throws NamingException {
		contextForOperationNotSupported.getEnvironment();
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testListString() throws NamingException {
		contextForOperationNotSupported.list(lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testListName() throws NamingException {
		contextForOperationNotSupported.list(lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testAddToEnvironment() throws NamingException {
		contextForOperationNotSupported.addToEnvironment(lookupString,lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testComposeNameNameName() throws NamingException {
		contextForOperationNotSupported.composeName(lookupName,lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testComposeNameStringString() throws NamingException {
		contextForOperationNotSupported.composeName(lookupString,lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testCreateSubcontextName() throws NamingException {
		contextForOperationNotSupported.createSubcontext(lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testCreateSubcontextString() throws NamingException {
		contextForOperationNotSupported.createSubcontext(lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testDestroySubcontextName() throws NamingException {
		contextForOperationNotSupported.destroySubcontext(lookupName);;
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testDestroySubcontextString() throws NamingException {
		contextForOperationNotSupported.destroySubcontext(lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testGetNameInNamespace() throws NamingException {
		contextForOperationNotSupported.getNameInNamespace();
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testGetNameParserName() throws NamingException {
		contextForOperationNotSupported.getNameParser(lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testGetNameParserString() throws NamingException {
		contextForOperationNotSupported.getNameParser(lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testListBindingsName() throws NamingException {
		contextForOperationNotSupported.listBindings(lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testListBindingsString() throws NamingException {
		contextForOperationNotSupported.listBindings(lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testLookupLinkName() throws NamingException {
		contextForOperationNotSupported.lookupLink(lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testLookupLinkString() throws NamingException {
		contextForOperationNotSupported.lookupLink(lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testRebindNameObject() throws NamingException {
		contextForOperationNotSupported.rebind(lookupName,lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testRebindStringObject() throws NamingException {
		contextForOperationNotSupported.rebind(lookupString,lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testRemoveFromEnvironment() throws NamingException {
		contextForOperationNotSupported.removeFromEnvironment(lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testRenameNameName() throws NamingException {
		contextForOperationNotSupported.rename(lookupName,lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testRenameStringString() throws NamingException {
		contextForOperationNotSupported.rename(lookupString,lookupString);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testUnbindName() throws NamingException {
		contextForOperationNotSupported.unbind(lookupName);
	}
	@Test(expected = OperationNotSupportedException.class)
	public void testUnbindString() throws NamingException {
		contextForOperationNotSupported.unbind(lookupString);
	}
}
