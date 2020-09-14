package com.amazon.sqs.javamessaging.jndi;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

/**
 * A factory of Amazon Simple Queue Service (SQS) initial context that provides a method for creating instances of
 * {@link SQSContext} that contain a {@link SQSConnectionFactory} instance.
 * <p>
 * It uses {@link Context#PROVIDER_URL PROVIDER_URL} in the {@link ProviderEndpointConfiguration},
 * as well as {@link Context#SECURITY_PRINCIPAL SECURITY_PRINCIPAL} and
 * {@link Context#SECURITY_CREDENTIALS SECURITY_CREDENTIALS} in the {@link CredentialsProvider}.
 * 
 * @author krloss
 * @since 1.1.0
 * @see InitialContextFactory
 */
public class SQSContextFactory implements InitialContextFactory {	
    // Factory method to create a new connection factory from the given environment.
	private static SQSConnectionFactory createConnectionFactory(Hashtable<?,?> environment) throws NamingException {
		ProviderEndpointConfiguration providerEndpoint = new ProviderEndpointConfiguration(environment.get(Context.PROVIDER_URL));
		CredentialsProvider credentials = CredentialsProvider.create(
			environment.get(Context.SECURITY_PRINCIPAL),environment.get(Context.SECURITY_CREDENTIALS));
		
		return new SQSConnectionFactory(new ProviderConfiguration(),AmazonSQSClientBuilder.standard()
			.withEndpointConfiguration(providerEndpoint.createConfiguration()).withCredentials(credentials));
	}
	
	/**
	 * Create instances of context which contains {@link SQSConnectionFactory}.
	 * 
	 * @param environment - set of configuration informations.
	 * @return {@link SQSContext}
	 * @throws NamingException
	 */
	@Override
	public SQSContext getInitialContext(Hashtable<?,?> environment) throws NamingException {
		return new SQSContext(createConnectionFactory(environment));
	}
}
