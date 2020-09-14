package com.amazon.sqs.javamessaging.jndi;

import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.InterruptedNamingException;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.OperationNotSupportedException;
import javax.naming.ServiceUnavailableException;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;

/**
 * Represents a naming context, which consists of a set of name-to-object bindings.
 * <p>
 * It works with a {@link ConnectionsManager connections manager} associated with a {@link SQSConnectionFactory} instance
 * and creates {@link Destination} instances through {@link DestinationResource}.
 * <p>
 * <b>Binded Objects:</b><ul>
 * <li>{@link SQSConnectionFactory} - set of connection configuration parameters.
 * <li>{@link Destination} - JMS administered object that encapsulates a specific provider address.
 * </ul>
 * 
 * @author krloss
 * @since 1.1.0
 * @see Context
 * @see DestinationResource
 */
public class SQSContext implements Context {
	private final SQSConnectionFactory connectionFactory;
	private final ConnectionsManager connectionsManager;
	private final ConcurrentHashMap<String,Object> bindings = new ConcurrentHashMap<>();
	
	/**
	 * Public constructor of a naming context that requires {@link SQSConnectionFactory} parameter.
	 * 
	 * @param connectionFactory - set of connection configuration parameters.
	 * @throws NamingException
	 */
	public SQSContext(final SQSConnectionFactory connectionFactory) throws NamingException {
		this.connectionFactory = connectionFactory;
		this.connectionsManager = new ConnectionsManager(this.connectionFactory);
	}
	
	private synchronized Object getDestination(String name) throws NamingException {
		Object destination = bindings.get(name); // Double-Checked Locking.
		
		if(destination != null) return destination;
		
		DestinationResource resource = new DestinationResource(name);
		
		try {
			destination = resource.getDestination(connectionsManager);
		}
		catch(JMSException e) {
			throw new ServiceUnavailableException(e.getMessage());
		}
		
		bind(name,destination);
		return destination;
	}
	
	/**
	 * Get the {@link SQSConnectionFactory} instance or a {@link Destination} instance.
	 * 
	 * @param name - string with name of the object.
	 * @return {@link SQSConnectionFactory} or {@link Destination}
	 * @throws NamingException
	 */
	@Override
	public Object lookup(String name) throws NamingException {
		if(SQSConnectionFactory.class.getName().equals(name)) return connectionFactory;
		
		Object destination = bindings.get(name);
		
		if(destination != null) return destination;
		
		return getDestination(name);
	}
	
	/**
	 * Get the {@link SQSConnectionFactory} instance or a {@link Destination} instance.
	 * 
	 * @param name - {@link Name name} of the object.
	 * @return {@link SQSConnectionFactory} or {@link Destination}
	 * @throws NamingException
	 */
	@Override
	public Object lookup(Name name) throws NamingException {
		return lookup(name.toString());
	}
	
	/**
	 * Closes this {@link SQSContext context} and its associated {@link ConnectionsManager connection manager}.
	 * 
	 * @throws NamingException
	 * @see {@link ConnectionsManager#close()}
	 */
	@Override
	public void close() throws NamingException {
		try {
			bindings.clear();
			connectionsManager.close();
		}
		catch(JMSException e) {
			throw new InterruptedNamingException(e.getMessage());
		}
	}
	
	/**
	 * Binds a name to an {@link Destination}.
	 * 
	 * @param name - string with name of the {@link Destination}.
	 * @throws NamingException
	 */
	@Override
	public void bind(String name, Object destination) throws NamingException {
		bindings.put(name,destination);
	}
	
	/**
	 * Binds a name to an {@link Destination}.
	 * 
	 * @param name - {@link Name name} of the {@link Destination}.
	 * @throws NamingException
	 */
	@Override
	public void bind(Name name, Object destination) throws NamingException {
		bind(name.toString(),destination);
	}
	
	@Override
	public Hashtable<?, ?> getEnvironment() throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
		throw new OperationNotSupportedException();
	}
	
	@Override
	public Object addToEnvironment(String arg0, Object arg1) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public Name composeName(Name arg0, Name arg1) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public String composeName(String arg0, String arg1) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public Context createSubcontext(Name arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public Context createSubcontext(String arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void destroySubcontext(Name arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void destroySubcontext(String arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public String getNameInNamespace() throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public NameParser getNameParser(Name arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public NameParser getNameParser(String arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public NamingEnumeration<Binding> listBindings(Name arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public NamingEnumeration<Binding> listBindings(String arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public Object lookupLink(Name arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public Object lookupLink(String arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void rebind(Name arg0, Object arg1) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void rebind(String arg0, Object arg1) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public Object removeFromEnvironment(String arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void rename(Name arg0, Name arg1) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void rename(String arg0, String arg1) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void unbind(Name arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
	@Override
	public void unbind(String arg0) throws NamingException {
		throw new OperationNotSupportedException();
	}
}
