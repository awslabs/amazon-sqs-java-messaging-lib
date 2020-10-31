package com.amazon.sqs.javamessaging.jndi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.directory.InvalidAttributeValueException;

import com.amazon.sqs.javamessaging.SQSConnection;

/**
 * Breaks the <b>description string</b> with information about the {@link ResourceType resource type}
 * and {@link com.amazon.sqs.javamessaging.SQSQueueDestination#getQueueName() destination name}
 * to make it an instance of {@link Destination} that encapsulates a specific provider address.
 * <p><ul>
 * <b>Format: </b><q><i>ResourceTypeName<b> : </b>DestinationName</i></q>.<br>
 * <b>Example: </b><q><i>SA:SQS_Queue-Name_v10</i></q>.
 * </ul>
 * 
 * @author krloss
 * @since 1.1.0
 */
public class DestinationResource {
	private static final Pattern RESOURCE_PATTERN = Pattern.compile("^\\s*([CS][ACDU])\\s*:\\s*([-\\w]+)\\s*$");
	
	protected final ResourceType type;
	protected final String name;
	
	/**
	 * Public constructor that requires <i>description</i> parameter.
	 * <p>
	 * <b>Format: </b><q><i>ResourceTypeName<b> : </b>DestinationName</i></q>.
	 * 
	 * @param description - string with information about the {@link ResourceType resource type}
	 * and {@link com.amazon.sqs.javamessaging.SQSQueueDestination#getQueueName() destination name}.
	 * 
	 * @throws InvalidAttributeValueException
	 */
	public DestinationResource(String description) throws InvalidAttributeValueException {
		Matcher matcher;
		
		try {
			matcher = RESOURCE_PATTERN.matcher(description);
		}
		catch(NullPointerException npe) {
			throw new InvalidAttributeValueException("DestinationResource Requires Description.");
		}
		
		if(!matcher.matches()) throw new InvalidAttributeValueException("DestinationResource Pattern Not Acceptable.");
		
		this.name = matcher.group(2);
		this.type = ResourceType.valueOf(matcher.group(1));
	}
	
	/**
	 * Gets the <b>connection</b> according to the <i>pooling type</i> and<br>
	 * creates <b>session</b> according to the <i>acknowledgment mode</i>.
	 */
	private Session createSession(final ConnectionsManager connectionsManager) throws JMSException {
		SQSConnection connection = type.isSessionPooling ?
			connectionsManager.getLazyDefaultConnection() : connectionsManager.createConnection();
		
		return connection.createSession(false,type.acknowledgeMode);
	}
	
	/**
	 * Makes this object in an instance of {@link Destination}.
	 * 
	 * @param connectionsManager - object that manages connections.
	 * @return Destination - JMS administered object that encapsulates a specific provider address.
	 * @throws InvalidAttributeValueException
	 * @throws JMSException
	 * @see ConnectionsManager
	 */
	public Destination getDestination(final ConnectionsManager connectionsManager) throws InvalidAttributeValueException, JMSException {
		if(connectionsManager == null) throw new InvalidAttributeValueException("GetConnection Requires ResourceType.");
		
		return createSession(connectionsManager).createQueue(name);
	}
}
