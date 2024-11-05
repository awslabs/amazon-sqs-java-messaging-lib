package com.amazon.sqs.javamessaging;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.QueueConnectionFactory;

public abstract class AbstractSQSConnectionFactory implements ConnectionFactory, QueueConnectionFactory {

    @Override
    public JMSContext createContext() {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        throw new JMSRuntimeException(SQSMessagingClientConstants.UNSUPPORTED_METHOD);
    }
}
