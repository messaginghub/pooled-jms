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

import javax.jms.JMSException;

/**
 * Temporary Destination Object
 */
public abstract class MockJMSTemporaryDestination extends MockJMSDestination {

    private boolean deleted;

    public MockJMSTemporaryDestination() {
        this(null, false);
    }

    public MockJMSTemporaryDestination(String name, boolean topic) {
        super(name, topic, true);
    }

    void setConnection(MockJMSConnection connection) {
        this.connection = connection;
    }

    MockJMSConnection getConnection() {
        return this.connection;
    }

    /**
     * Attempts to delete the destination if there is an assigned Connection object.
     *
     * @throws JMSException if an error occurs or the provider doesn't support
     *         delete of destinations from the client.
     */
    protected void tryDelete() throws JMSException {
        if (connection != null) {
            connection.deleteTemporaryDestination(this);
        }

        deleted = true;
    }

    protected boolean isDeleted() throws JMSException {
        boolean result = deleted;

        if (!result && connection != null) {
            result = connection.isTemporaryDestinationDeleted(this);
        }

        return result;
    }
}
