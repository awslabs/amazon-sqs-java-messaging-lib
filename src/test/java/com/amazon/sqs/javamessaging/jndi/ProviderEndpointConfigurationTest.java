package com.amazon.sqs.javamessaging.jndi;

import static org.junit.Assert.*;

import javax.naming.directory.InvalidAttributeValueException;

import org.junit.Test;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;

public class ProviderEndpointConfigurationTest {
	private final String signingRegion = Regions.US_EAST_2.getName();
	private final String serviceEndpoint = "https://sqs.us-east-2.amazonaws.com/";
	
	@Test(expected = InvalidAttributeValueException.class)
	public void testNullProviderEndpoint() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration(null);
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testEmptyProviderEndpoint() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration("");
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testPrefixProviderEndpointConfiguration() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration(signingRegion);
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testSufixProviderEndpointConfiguration() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration(serviceEndpoint);
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testProviderEndpointConfigurationWithoutSeparator() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration(String.format("%s%s",signingRegion,serviceEndpoint));
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testProviderEndpointConfigurationWithIncorrectSeparator() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration(String.format("%s:%s",signingRegion,serviceEndpoint));
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testProviderEndpointConfigurationWithSeparatorOnly() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration(String.format("@",signingRegion,serviceEndpoint));
	}
	@Test(expected = InvalidAttributeValueException.class)
	public void testProviderEndpointConfigurationWithSeparatorAndSpaces() throws InvalidAttributeValueException {
		new ProviderEndpointConfiguration(String.format(" \n\t @ \n\t ",signingRegion,serviceEndpoint));
	}
	
	@Test
	public void testCreateEndpointConfiguration() throws InvalidAttributeValueException {
		EndpointConfiguration configuration = new ProviderEndpointConfiguration(
			String.format("%s@%s",signingRegion,serviceEndpoint)).createConfiguration();
		
		assertEquals(signingRegion,configuration.getSigningRegion());
		assertEquals(serviceEndpoint,configuration.getServiceEndpoint());
	}
	@Test
	public void testCompleteCreateEndpointConfiguration() throws InvalidAttributeValueException {
		EndpointConfiguration configuration = new ProviderEndpointConfiguration(
			String.format(" \n\t %s \n\t @ \n\t %s \n\t ",signingRegion,serviceEndpoint)).createConfiguration();
		
		assertEquals(signingRegion,configuration.getSigningRegion());
		assertEquals(serviceEndpoint,configuration.getServiceEndpoint());
	}
}
