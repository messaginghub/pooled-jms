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
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAJMSContext;

/**
 * A simple mock JMS XA ConnectionFactory implementation for tests
 */
public class MockJMSXAConnectionFactory extends MockJMSConnectionFactory implements XAConnectionFactory {

    @Override
    public XAConnection createXAConnection() throws JMSException {
        return (XAConnection) super.createConnection();
    }

    @Override
    public XAConnection createXAConnection(String userName, String password) throws JMSException {
        return (XAConnection) super.createConnection(userName, password);
    }

    @Override
    public XAJMSContext createXAContext() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public XAJMSContext createXAContext(String userName, String password) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    protected MockJMSConnection createMockConnectionInstance(MockJMSUser user ) {
        return new MockJMSXAConnection(user);
    }
}
