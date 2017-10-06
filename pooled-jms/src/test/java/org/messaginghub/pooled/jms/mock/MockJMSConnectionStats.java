/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.messaginghub.pooled.jms.mock;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects statistics about the state and usage of the MockJMSConnection
 */
public class MockJMSConnectionStats {

    private final Set<MockJMSTemporaryQueue> temporaryQueues = new HashSet<>();
    private final Set<MockJMSTemporaryTopic> temporaryTopics = new HashSet<>();

    private final AtomicLong totalTempQueues = new AtomicLong();
    private final AtomicLong totalTempTopics = new AtomicLong();

    public void temporaryDestinationCreated(MockJMSTemporaryDestination destination) {
        if (destination.isQueue()) {
            totalTempQueues.incrementAndGet();
            temporaryQueues.add((MockJMSTemporaryQueue) destination);
        } else {
            totalTempTopics.incrementAndGet();
            temporaryTopics.add((MockJMSTemporaryTopic) destination);
        }
    }

    public void temporaryDestinationDestroyed(MockJMSTemporaryDestination destination) {
        if (destination.isQueue()) {
            temporaryQueues.remove(destination);
        } else {
            temporaryTopics.remove(destination);
        }
    }

    public long getActiveTemporaryQueueCount() {
        return temporaryQueues.size();
    }

    public long getActiveTemporaryTopicCount() {
        return temporaryTopics.size();
    }

    public long getTotalTemporaryQueuesCreated() {
        return totalTempQueues.get();
    }

    public long getTotalTemporaryTopicsCreated() {
        return totalTempTopics.get();
    }
}
