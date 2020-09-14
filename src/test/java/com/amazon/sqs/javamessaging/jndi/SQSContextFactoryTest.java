package com.amazon.sqs.javamessaging.jndi;

import static org.junit.Assert.*;

import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.directory.InvalidAttributeValueException;

import org.junit.Test;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.regions.Regions;

public class SQSContextFactoryTest {
	private final String signingRegion = Regions.US_EAST_2.getName();
	private final String serviceEndpoint = "https://sqs.us-east-2.amazonaws.com/";
	private final String providerEndpoint = String.format("%s@%s",signingRegion,serviceEndpoint);
	private final String accessKey = "securityPrincipal";
	private final String secretKey = "securityCredentials";
	
	private static Properties getEnvironment(String providerURL, String securityPrincipal, String securityCredentials) {
		Properties environment = new Properties();
		
		environment.put(InitialContext.INITIAL_CONTEXT_FACTORY,SQSContextFactory.class.getName());
		
		if(providerURL != null) environment.put(InitialContext.PROVIDER_URL,providerURL);
		if(securityPrincipal != null) environment.put(InitialContext.SECURITY_PRINCIPAL,securityPrincipal);
		if(securityCredentials != null) environment.put(InitialContext.SECURITY_CREDENTIALS,securityCredentials);
		
		return environment;
	}
	
	
	@Test
	public void testGetInitialContext() throws NamingException {
		SQSContextFactory factory = new SQSContextFactory();
		
		assertNotNull((SQSContext)factory.getInitialContext(getEnvironment(providerEndpoint,accessKey,secretKey)));
	}
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testGetInitialContextWithoutProviderURL() throws NamingException {
		SQSContextFactory factory = new SQSContextFactory();
		
		assertNotNull(factory.getInitialContext(getEnvironment(null,accessKey,secretKey)));
	}
	
	@Test
	public void testGetInitialContextWithoutSecurityPrincipal() throws NamingException {
		SQSContextFactory factory = new SQSContextFactory();
		
		assertNotNull(factory.getInitialContext(getEnvironment(providerEndpoint,null,secretKey)));
	}
	
	@Test
	public void testGetInitialContextWithoutSecurityCredentials() throws NamingException {
		SQSContextFactory factory = new SQSContextFactory();
		
		assertNotNull(factory.getInitialContext(getEnvironment(providerEndpoint,accessKey,null)));
	}
	
	@Test
	public void testGetInitialContextWithoutSecurity() throws NamingException {
		SQSContextFactory factory = new SQSContextFactory();
		
		assertNotNull(factory.getInitialContext(getEnvironment(providerEndpoint,null,null)));
	}
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testGetInitialContextWithoutAttribute() throws NamingException {
		SQSContextFactory factory = new SQSContextFactory();
		
		assertNotNull(factory.getInitialContext(getEnvironment(null,null,null)));
	}
	
	@Test
	public void testInitialContext() throws NamingException {
		InitialContext contextFactory = new InitialContext(getEnvironment(providerEndpoint,accessKey,secretKey));
		
		assertNotNull((SQSConnectionFactory)contextFactory.lookup(SQSConnectionFactory.class.getName()));
	}
}
