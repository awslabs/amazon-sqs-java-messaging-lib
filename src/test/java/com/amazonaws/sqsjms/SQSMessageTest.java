/*
 * Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.sqsjms;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.jms.JMSException;

import org.junit.Before;
import org.junit.Test;

import javax.jms.Message;

import junit.framework.Assert;

public class SQSMessageTest {
    private SQSSession mockSQSSession;

    @Before
    public void setup() {
        mockSQSSession = mock(SQSSession.class);
    }

    @Test
    public void testProperty() throws JMSException {
        when(mockSQSSession.createMessage()).thenReturn(new SQSMessage());
        Message message = mockSQSSession.createMessage();

        message.setBooleanProperty("myTrueBoolean", true);
        message.setBooleanProperty("myFalseBoolean", false);
        message.setIntProperty("myInteger", 100);
        message.setDoubleProperty("myDouble", 2.1768);
        message.setFloatProperty("myFloat", 3.1457f);
        message.setLongProperty("myLong", 1290772974281L);
        message.setShortProperty("myShort", (short) 123);
        message.setByteProperty("myByteProperty", (byte) 'a');
        message.setStringProperty("myString", "StringValue");

        Assert.assertTrue(message.propertyExists("myTrueBoolean"));
        Assert.assertEquals(message.getObjectProperty("myTrueBoolean"), true);
        
        Assert.assertTrue(message.propertyExists("myFalseBoolean"));
        Assert.assertEquals(message.getObjectProperty("myFalseBoolean"), false);
        
        Assert.assertTrue(message.propertyExists("myInteger"));
        Assert.assertEquals(message.getObjectProperty("myInteger"), 100);
        
        Assert.assertTrue(message.propertyExists("myDouble"));
        Assert.assertEquals(message.getObjectProperty("myDouble"), 2.1768);
        
        Assert.assertTrue(message.propertyExists("myFloat"));
        Assert.assertEquals(message.getObjectProperty("myFloat"), 3.1457f);
        
        Assert.assertTrue(message.propertyExists("myLong"));
        Assert.assertEquals(message.getObjectProperty("myLong"), 1290772974281L);
        
        Assert.assertTrue(message.propertyExists("myShort"));
        Assert.assertEquals(message.getObjectProperty("myShort"), (short) 123);
        
        Assert.assertTrue(message.propertyExists("myByteProperty"));
        Assert.assertEquals(message.getObjectProperty("myByteProperty"), (byte) 'a');
        
        Assert.assertTrue(message.propertyExists("myString"));
        Assert.assertEquals(message.getObjectProperty("myString"), "StringValue");
        
        message.clearProperties();
        Assert.assertFalse(message.propertyExists("myTrueBoolean"));
        Assert.assertFalse(message.propertyExists("myInteger"));
        Assert.assertFalse(message.propertyExists("myDouble"));
        Assert.assertFalse(message.propertyExists("myFloat"));
        Assert.assertFalse(message.propertyExists("myLong"));
        Assert.assertFalse(message.propertyExists("myShort"));
        Assert.assertFalse(message.propertyExists("myByteProperty"));
        Assert.assertFalse(message.propertyExists("myString"));
    }
}