package com.amazon.sqs.javamessaging.jndi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.directory.InvalidAttributeValueException;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;

/**
 * Breaks the <b>configuration string</b> to make it an instance of {@link EndpointConfiguration} that
 * enables the use of public IPs on the Internet or VPC endpoints that are powered by AWS PrivateLink.
 * <p><ul>
 * <b>Format: </b><q><i>Region@EndpointURL</i></q>.<br>
 * <b>Example: </b><q><i>us-east-2@https://sqs.us-east-2.amazonaws.com/</i></q>.
 * </ul>
 * 
 * @author krloss
 * @since 1.1.0
 * @see javax.naming.Context#PROVIDER_URL
 */
public class ProviderEndpointConfiguration {
	private static final Pattern CONFIGURATION_PATTERN = Pattern.compile("^\\s*+(.+?)\\s*+@\\s*+(.+?)\\s*+$");
	
	private final String serviceEndpoint;
	private final String signingRegion;
	
	/**
	 * Public constructor that requires <i>configuration</i> parameter.
	 * <p>
	 * <b>Format: </b><q><i>Region@EndpointURL</i></q>.
	 * 
	 * @param configuration - information for the service provider;
	 * @throws InvalidAttributeValueException
	 */
	public ProviderEndpointConfiguration(String configuration) throws InvalidAttributeValueException {
		Matcher matcher;
		
		try {
			matcher = CONFIGURATION_PATTERN.matcher(configuration);
		}
		catch(NullPointerException npe) {
			throw new InvalidAttributeValueException("ProviderEndpointConfiguration Requires Configuration String.");
		}
		
		if(!matcher.matches()) throw new InvalidAttributeValueException("ProviderEndpointConfiguration Pattern Not Acceptable.");
		
		this.serviceEndpoint = matcher.group(2);
		this.signingRegion = matcher.group(1);
	}
	
	/**
	 * Public constructor that requires <i>configuration</i> parameter.
	 * <p>
	 * <b>Format: </b><q><i>Region@EndpointURL</i></q>.
	 * 
	 * @param configuration - information for the service provider;
	 * @throws InvalidAttributeValueException
	 */
	public ProviderEndpointConfiguration(Object configuration) throws InvalidAttributeValueException {
		this((String)configuration);
	}
	
	/**
	 * Makes this object in an instance of {@link EndpointConfiguration}.
	 * 
	 * @return EndpointConfiguration - a container for configuration required to submit requests to an AWS service.
	 */
	public EndpointConfiguration createConfiguration() {
		return new EndpointConfiguration(serviceEndpoint,signingRegion);
	}
}

