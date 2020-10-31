package com.amazon.sqs.javamessaging.jndi;

import static org.junit.Assert.*;

import org.junit.Test;

public class CredentialsProviderTest {
	private final String securityPrincipal = "securityPrincipal";
	private final String securityCredentials = "securityCredentials";
	
	@Test
	public void testNullCredentialsProvider() {
		assertNull(CredentialsProvider.create(null,null));
	}
	@Test
	public void testEmptyCredentialsProvider() {
		assertNull(CredentialsProvider.create("",""));
	}
	@Test
	public void testCredentialsProviderWithPrincipalOnly() {
		assertNull(CredentialsProvider.create(securityPrincipal,null));
	}
	@Test
	public void testCredentialsProviderWithCredentialsOnly() {
		assertNull(CredentialsProvider.create(null,securityCredentials));
	}
	@Test
	public void testCredentialsProviderWithSpaces() {
		assertNull(CredentialsProvider.create(" \n\t "," \n\t "));
	}
	
	@Test
	public void testCreateCredentialsProvider() {
		CredentialsProvider provider = CredentialsProvider.create(securityPrincipal,securityCredentials);
		
		assertEquals(securityPrincipal,provider.getCredentials().getAWSAccessKeyId());
		assertEquals(securityCredentials,provider.getCredentials().getAWSSecretKey());
	}
	@Test
	public void testCompleteCreateCredentialsProvider() {
		CredentialsProvider provider = CredentialsProvider.create(
			String.format(" \n\t %s \n\t ",securityPrincipal),String.format(" \n\t %s \n\t ",securityCredentials));
		
		assertEquals(securityPrincipal,provider.getCredentials().getAWSAccessKeyId());
		assertEquals(securityCredentials,provider.getCredentials().getAWSSecretKey());
	}
}
