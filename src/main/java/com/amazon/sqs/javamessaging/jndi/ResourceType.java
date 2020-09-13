package com.amazon.sqs.javamessaging.jndi;

import static com.amazon.sqs.javamessaging.SQSSession.UNORDERED_ACKNOWLEDGE;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static javax.jms.Session.CLIENT_ACKNOWLEDGE;
import static javax.jms.Session.DUPS_OK_ACKNOWLEDGE;

/**
 * Classifies the resource according to the pooling type and the acknowledgment mode.
 * <p><ul>
 * <li><b>Pooling Type: </b><ul>
 * Connection (<i> CA, CC, CD, CU </i>)<br>
 * Session (<i> SA, SC, SD, SU </i>)
 * </ul>
 * <li><b>Acknowledgment Mode: </b><ul>
 * {@link javax.jms.Session#AUTO_ACKNOWLEDGE AUTO_ACKNOWLEDGE} (<i> CA, SA </i>)<br>
 * {@link javax.jms.Session#CLIENT_ACKNOWLEDGE CLIENT_ACKNOWLEDGE} (<i> CC, SC </i>)<br>
 * {@link javax.jms.Session#DUPS_OK_ACKNOWLEDGE DUPS_OK_ACKNOWLEDGE} (<i> CD, SD </i>)<br>
 * {@link com.amazon.sqs.javamessaging.SQSSession#UNORDERED_ACKNOWLEDGE UNORDERED_ACKNOWLEDGE} (<i> CU, SU </i>)
 * </ul>
 * </ul>
 * 
 * @author krloss
 * @since 1.1.0
 * @see com.amazon.sqs.javamessaging.SQSSession
 */
public enum ResourceType {
	// Types for Connection Pooling
	CA(false,AUTO_ACKNOWLEDGE),
	CC(false,CLIENT_ACKNOWLEDGE),
	CD(false,DUPS_OK_ACKNOWLEDGE),
	CU(false,UNORDERED_ACKNOWLEDGE),
	
	// Types for Session Pooling
	SA(true,AUTO_ACKNOWLEDGE),
	SC(true,CLIENT_ACKNOWLEDGE),
	SD(true,DUPS_OK_ACKNOWLEDGE),
	SU(true,UNORDERED_ACKNOWLEDGE);
	
	public final boolean isSessionPolling;
	public final int acknowledgeMode;
	
	private ResourceType(boolean isSessionPolling,int acknowledgeMode) {
		this.isSessionPolling = isSessionPolling;
		this.acknowledgeMode = acknowledgeMode;
	}
}
