package com.amazon.sqs.javamessaging.jndi;

import static com.amazon.sqs.javamessaging.jndi.ResourceType.*;

import static org.junit.Assert.*;

import org.junit.Test;

import com.amazon.sqs.javamessaging.SQSSession;

public class ResourceTypeTest {
	
	@Test
	public void testIsConnectionPolling() {
		assertFalse(CA.isSessionPolling);
		assertFalse(CC.isSessionPolling);
		assertFalse(CD.isSessionPolling);
		assertFalse(CU.isSessionPolling);
	}
	
	@Test
	public void testIsSessionPolling() {
		assertTrue(SA.isSessionPolling);
		assertTrue(SC.isSessionPolling);
		assertTrue(SD.isSessionPolling);
		assertTrue(SU.isSessionPolling);
	}

	@Test
	public void testGetAcknowledgeMode() {
		assertArrayEquals(new Integer[] {CA.acknowledgeMode,SA.acknowledgeMode},
			new Integer[] {SQSSession.AUTO_ACKNOWLEDGE,SQSSession.AUTO_ACKNOWLEDGE});
		
		assertArrayEquals(new Integer[] {CC.acknowledgeMode,SC.acknowledgeMode},
			new Integer[] {SQSSession.CLIENT_ACKNOWLEDGE,SQSSession.CLIENT_ACKNOWLEDGE});
		
		assertArrayEquals(new Integer[] {CD.acknowledgeMode,SD.acknowledgeMode},
			new Integer[] {SQSSession.DUPS_OK_ACKNOWLEDGE,SQSSession.DUPS_OK_ACKNOWLEDGE});
		
		assertArrayEquals(new Integer[] {CU.acknowledgeMode,SU.acknowledgeMode},
			new Integer[] {SQSSession.UNORDERED_ACKNOWLEDGE,SQSSession.UNORDERED_ACKNOWLEDGE});
	}
}
