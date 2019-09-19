package com.amazon.sqs.javamessaging;

import javax.jms.Message;

/**
 * Mimics {@link javax.jms.CompletionListener} from JMS 2.0 API
 */
public interface CompletionListener {

	/**
	 * Notifies the application that the message has been successfully sent
	 *
	 * @param message
	 *            the message that was sent.
	 */
	void onCompletion(Message message);

	/**
	 * Notifies user that the specified exception was thrown while attempting to
	 * send the specified message. If an exception occurs it is undefined
	 * whether or not the message was successfully sent.
	 *
	 * @param message
	 *            the message that was sent.
	 * @param exception
	 *            the exception
	 *
	 */
	void onException(Message message, Exception exception);
}
