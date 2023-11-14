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

import jakarta.jms.JMSException;
import jakarta.jms.QueueSession;
import jakarta.jms.Session;
import jakarta.jms.TopicSession;
import jakarta.jms.XAConnection;
import jakarta.jms.XASession;

/**
 * Simple Mock JMS XAConnection implementation for unit tests
 */
public class MockJMSXAConnection extends MockJMSConnection implements XAConnection {

    public MockJMSXAConnection(MockJMSUser user) {
        super(user);
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        checkClosedOrFailed();
        ensureConnected();
        int ackMode = getSessionAcknowledgeMode(true, Session.SESSION_TRANSACTED);
        MockJMSXASession result = new MockJMSXASession(getNextSessionId(), ackMode, this);
        addSession(result);
        if (isStarted()) {
            result.start();
        }
        return result;
    }

    @Override
    public XASession createXASession() throws JMSException {
        return (XASession) createSession(true, Session.SESSION_TRANSACTED);
    }
}
