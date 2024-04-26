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
package org.messaginghub.pooled.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import javax.jms.XASession;
import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.junit.Test;
import org.messaginghub.pooled.jms.mock.MockJMSConnectionMetaData;
import org.messaginghub.pooled.jms.mock.MockJMSXAConnectionFactory;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for the XA pooled connection handling.
 */
public class JmsPoolXAConnectionTest extends JmsPoolTestSupport  {

    protected JmsPoolXAConnectionFactory xaCF;

    @Mock
    TransactionManager txManager;

    @Mock
    Transaction txn;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);

        when(txManager.getTransaction()).thenReturn(txn);
        when(txn.enlistResource(any())).thenReturn(true);

        factory = new MockJMSXAConnectionFactory();

        xaCF = new JmsPoolXAConnectionFactory();
        xaCF.setTransactionManager(txManager);
        xaCF.setConnectionFactory(factory);
        xaCF.setMaxConnections(1);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            xaCF.stop();
        } catch (Exception ex) {
            // ignored
        }

        super.tearDown();
    }

    @Test
    public void testGetConnectionMetaData() throws Exception {
        Connection connection = xaCF.createConnection();
        ConnectionMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);
        assertSame(metaData, MockJMSConnectionMetaData.INSTANCE);
    }

    @Test
    public void testCreateXASession() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) xaCF.createConnection();
        XASession session = (XASession) connection.createSession();

        when(txn.enlistResource(any())).thenReturn(true);

        assertNotNull(session);

        assertEquals(0, connection.getNumtIdleSessions());
        session.close();

        // Session should be ignoring close at this stage
        assertEquals(0, connection.getNumtIdleSessions());
    }

    @Test
    public void testCreateXASessionFailsOnAddSynchronization() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) xaCF.createConnection();

        doThrow(RollbackException.class).when(txn).registerSynchronization(any());
        when(txn.enlistResource(any())).thenReturn(true);

        assertThrows(JMSException.class, () -> connection.createSession());

        // Session should be invalidated as we don't know the state after failed register
        assertEquals(0, connection.getNumtIdleSessions());
    }

    @Test
    public void testCreateXASessionFailsOnEnlist() throws Exception {
        JmsPoolConnection connection = (JmsPoolConnection) xaCF.createConnection();

        when(txn.enlistResource(any())).thenReturn(false);

        assertThrows(JMSException.class, () -> connection.createSession());

        // Session should be invalidated as we don't know the state after failed enlist
        assertEquals(0, connection.getNumtIdleSessions());
    }
}
