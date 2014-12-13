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
package com.amazonaws.sqsjms.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SQSJMSClientThreadFactory implements ThreadFactory {

    private final String threadBaseName;

    private final AtomicInteger threadCounter;

    private final boolean isDaemon;

    private ThreadGroup threadGroup;

    public SQSJMSClientThreadFactory(String taskName, boolean isDaemon) {
        this(taskName, isDaemon, false);
    }

    public SQSJMSClientThreadFactory(String taskName, boolean isDaemon, boolean createWithThreadGroup) {
        this.threadBaseName = taskName + "Thread-";
        this.threadCounter = new AtomicInteger(0);
        this.isDaemon = isDaemon;
        if (createWithThreadGroup) {
            threadGroup = new ThreadGroup(taskName + "ThreadGroup");
            threadGroup.setDaemon(isDaemon);
        }
    }

    public SQSJMSClientThreadFactory(String taskName, ThreadGroup threadGroup) {
        this.threadBaseName = taskName + "Thread-";
        this.threadCounter = new AtomicInteger(0);
        this.isDaemon = threadGroup.isDaemon();
        this.threadGroup = threadGroup;
    }

    public Thread newThread(Runnable r) {
        Thread t;
        if (threadGroup == null) {
            t = new Thread(r, threadBaseName + threadCounter.incrementAndGet());
            t.setDaemon(isDaemon);
        } else {
            t = new Thread(threadGroup, r, threadBaseName + threadCounter.incrementAndGet());
            t.setDaemon(isDaemon);
        }
        return t;
    }
    
    public boolean wasThreadCreatedWithThisThreadGroup(Thread thread) {
        if (threadGroup == null) {
            return false;
        }
        return thread.getThreadGroup() == threadGroup;
    }
  
}

