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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

import org.messaginghub.pooled.jms.util.JMSExceptionSupport;

/**
 * Mock JMS ConnectionFactory used to create Mock Connections
 */
public class MockJMSConnectionFactory implements ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory {

    private final Map<String, MockJMSUser> credentials = new HashMap<>();

    private String clientID;
    private boolean deferAuthenticationToConnection;

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createMockConnection(null, null);
    }

    @Override
    public TopicConnection createTopicConnection(String username, String password) throws JMSException {
        return createMockConnection(username, password);
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createMockConnection(null, null);
    }

    @Override
    public QueueConnection createQueueConnection(String username, String password) throws JMSException {
        return createMockConnection(username, password);
    }

    @Override
    public Connection createConnection() throws JMSException {
        return createMockConnection(null, null);
    }

    @Override
    public Connection createConnection(String username, String password) throws JMSException {
        return createMockConnection(username, password);
    }

    private MockJMSConnection createMockConnection(String username, String password) throws JMSException {
        MockJMSUser user = validateUser(username, password);

        if (!user.isValid() && !deferAuthenticationToConnection) {
            throw new JMSSecurityException(user.getFailureCause());
        }

        MockJMSConnection connection = new MockJMSConnection(user);

        if (clientID != null && !clientID.isEmpty()) {
            connection.setClientID(clientID, true);
        } else {
            connection.setClientID(UUID.randomUUID().toString(), false);
        }

        try {
            connection.initialize();
        } catch (JMSException e) {
            connection.close();
        }

        return connection;
    }

    //----- JMS Context Creation Methods -------------------------------------//

    @Override
    public JMSContext createContext() {
        return createMockContext(null, null, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return createMockContext(null, null, sessionMode);
    }

    @Override
    public JMSContext createContext(String username, String password) {
        return createMockContext(username, password, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String username, String password, int sessionMode) {
        return createMockContext(username, password, sessionMode);
    }

    private MockJMSContext createMockContext(String username, String password, int sessionMode) {
        final MockJMSConnection connection;
        try {
            connection = createMockConnection(username, password);
        } catch (JMSException jmsex) {
            throw JMSExceptionSupport.createRuntimeException(jmsex);
        }

        MockJMSContext context = new MockJMSContext(connection, sessionMode);

        return context;
    }

    //----- Factory Configuration --------------------------------------------//

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    /**
     * Adds a User to the set of Users that this factory will use when performing
     * authentication when given a user and password on connection create.
     *
     * When there are no configured users the factory allows any create connection to
     * succeed.  Once a user is added then only a call to
     * {@link MockJMSConnectionFactory#createConnection(String, String)} with valid
     * user credentials will succeed in creating a new connection.
     *
     * @param userCredentials
     * 		The new user credentials to store for authentication and authorization purposes.
     */
    public void addUser(MockJMSUser userCredentials) {
        this.credentials.put(userCredentials.getUsername(), userCredentials);
    }

    /**
     * @return the true if the factory defers authentication failure to a created Connection.
     */
    public boolean isDeferAuthenticationToConnection() {
        return deferAuthenticationToConnection;
    }

    /**
     * Controls whether the factory or the connection object will perform user authentication
     * when users are configured, if no users are configured then authentication will always
     * succeed regardless.
     *
     * In some cases it is desirable to have the connection create call succeed regardless of
     * the user credentials to simulate client's that do not perform authentication until the
     * connection becomes started.
     *
     * @param deferAuthenticationToConnection
     * 		whether the factory or the connection should perform the authentication.
     */
    public void setDeferAuthenticationToConnection(boolean deferAuthenticationToConnection) {
        this.deferAuthenticationToConnection = deferAuthenticationToConnection;
    }

    //----- Internal Support Methods -----------------------------------------//

    private MockJMSUser validateUser(String username, String password) {
        MockJMSUser user = MockJMSUser.DEFAULT_USER;

        if (!credentials.isEmpty()) {
            if (username == null) {
                user = MockJMSUser.INVALID_ANONYMOUS;
            } else {
                user = credentials.get(username);
                if (user == null || !user.getPassword().equals(password)) {
                    user = MockJMSUser.INVALID_CREDENTIALS;
                }
            }
        }

        return user;
    }
}
