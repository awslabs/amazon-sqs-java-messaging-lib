package com.amazon.sqs.javamessaging.jndi;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 * Simple implementation of {@link AWSStaticCredentialsProvider} with {@link BasicAWSCredentials}
 * that use {@link javax.naming.Context#SECURITY_PRINCIPAL identity} as an AWS <b>access key</b>
 * and {@link javax.naming.Context#SECURITY_CREDENTIALS credentials} as an AWS <b>secret access key</b>.
 * 
 * @author krloss
 * @since 1.1.0
 */
public class CredentialsProvider extends AWSStaticCredentialsProvider {
	// Prevents incorrect startup.
	private CredentialsProvider(String accessKey, String secretKey) {
		super(new BasicAWSCredentials(accessKey.trim(),secretKey.trim()));
		
		getCredentials(); // Initialize
	}
	
	private static Boolean assertNotEmpty(String accessKey, String secretKey) {
		try { if(accessKey.trim().isEmpty() || secretKey.trim().isEmpty()) return false; }
		catch(NullPointerException npe) { return false; }
		
		return true;
	}
	
	/**
	 * Public method that create a {@link CredentialsProvider} instance.
	 * 
	 * @param securityPrincipal - {@link javax.naming.Context#SECURITY_PRINCIPAL identity}
	 * as an AWS <i>access key</i>
	 * 
	 * @param securityCredentials - {@link javax.naming.Context#SECURITY_CREDENTIALS credentials}
	 * as an AWS <i>secret access key</i>
	 * 
	 * @return {@link CredentialsProvider}
	 */
	public static CredentialsProvider create(String securityPrincipal, String securityCredentials) {
		if(assertNotEmpty(securityPrincipal,securityCredentials))
			return new CredentialsProvider(securityPrincipal,securityCredentials);
		
		return null;
	}

	/**
	 * Public method that create a {@link CredentialsProvider} instance.
	 * 
	 * @param securityPrincipal - {@link javax.naming.Context#SECURITY_PRINCIPAL identity}
	 * as an AWS <i>access key</i>
	 * 
	 * @param securityCredentials - {@link javax.naming.Context#SECURITY_CREDENTIALS credentials}
	 * as an AWS <i>secret access key</i>
	 * 
	 * @return {@link CredentialsProvider}
	 */
	public static CredentialsProvider create(Object securityPrincipal, Object securityCredentials) {
		return create((String)securityPrincipal,(String)securityCredentials);
	}
}
