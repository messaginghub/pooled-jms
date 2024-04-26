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
package org.messaginghub.pooled.jms.pool;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.messaginghub.pooled.jms.JmsPoolSession;

/**
 * An XA-aware connection pool. When a session is created and an xa transaction
 * is active, the session will automatically be enlisted in the current
 * transaction.
 */
public class PooledXAConnection extends PooledConnection {

    private final TransactionManager transactionManager;

    public PooledXAConnection(Connection connection, TransactionManager transactionManager) {
        super(connection);
        this.transactionManager = transactionManager;
    }

    @Override
    protected Session makeSession(PooledSessionKey key) throws JMSException {
        return ((XAConnection) connection).createXASession();
    }

    @Override
    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        try {
            boolean isXa = (transactionManager != null && transactionManager.getStatus() != Status.STATUS_NO_TRANSACTION);

            if (isXa) {
                // if the xa tx aborts inflight we don't want to auto create a
                // local transaction or auto ack
                transacted = false;
                ackMode = Session.CLIENT_ACKNOWLEDGE;
            } else if (transactionManager != null) {
                // cmt or transactionManager managed
                transacted = false;
                if (ackMode == Session.SESSION_TRANSACTED) {
                    ackMode = Session.AUTO_ACKNOWLEDGE;
                }
            }

            final JmsPoolSession session = (JmsPoolSession) super.createSession(transacted, ackMode);

            session.setIgnoreClose(isXa);
            session.setIsXa(isXa);

            if (isXa) {
                incrementReferenceCount();

                final JmsPooledXASessionSynchronization sync = new JmsPooledXASessionSynchronization(session);

                try {
                    transactionManager.getTransaction().registerSynchronization(sync);

                    if (!transactionManager.getTransaction().enlistResource(createXaResource(session))) {
                        throw new JMSException("Enlistment of Pooled Session into transaction failed");
                    }
                } catch (Exception ex) {
                    sync.fail();
                    throw ex;
                }
            }

            return session;
        } catch (RollbackException e) {
            final JMSException jmsException = new JMSException("Rollback Exception");
            jmsException.initCause(e);
            throw jmsException;
        } catch (SystemException e) {
            final JMSException jmsException = new JMSException("System Exception");
            jmsException.initCause(e);
            throw jmsException;
        }
    }

    protected XAResource createXaResource(JmsPoolSession session) throws JMSException {
        return session.getXAResource();
    }

    protected class JmsPooledXASessionSynchronization implements Synchronization {

        private final AtomicBoolean closed = new AtomicBoolean();

        private JmsPoolSession session;

        private JmsPooledXASessionSynchronization(JmsPoolSession session) {
            this.session = session;
        }

        public void fail() throws JMSException {
            if (closed.compareAndSet(false, true)) {
                // Force the session to close and invalidate itself.
                try {
                    session.internalClose(true);
                } finally {
                    session = null;
                    decrementReferenceCount();
                }
            }
        }

        public void close() throws JMSException {
            if (closed.compareAndSet(false, true)) {
                // This will return session to the pool.
                session.setIgnoreClose(false);
                try {
                    session.close();
                } finally {
                    session = null;
                    decrementReferenceCount();
                }
            }
        }

        @Override
        public void beforeCompletion() {
        }

        @Override
        public void afterCompletion(int status) {
            try {
                close();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
