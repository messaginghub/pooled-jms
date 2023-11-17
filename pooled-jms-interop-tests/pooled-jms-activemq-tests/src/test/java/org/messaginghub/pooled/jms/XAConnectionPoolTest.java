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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Hashtable;
import java.util.Vector;

import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAJMSContext;
import javax.naming.spi.ObjectFactory;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.ActiveMQXASession;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

@Timeout(60)
public class XAConnectionPoolTest extends ActiveMQJmsPoolTestSupport {

    @SuppressWarnings("resource")
    @Test
    public void testAfterCompletionCanClose() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        ActiveMQTopic topic = new ActiveMQTopic("test");
        JmsPoolXAConnectionFactory pcf = createXAPooledConnectionFactory();

        // simple TM that is in a tx and will track syncs
        pcf.setTransactionManager(new TransactionManager(){
            @Override
            public void begin() throws NotSupportedException, SystemException {
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException, SystemException {
            }

            @Override
            public int getStatus() throws SystemException {
                return Status.STATUS_ACTIVE;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
                return new Transaction() {
                    @Override
                    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException {
                    }

                    @Override
                    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
                        return false;
                    }

                    @Override
                    public boolean enlistResource(XAResource xaRes) throws IllegalStateException, RollbackException, SystemException {
                        try {
                            final Xid mockXid = Mockito.mock(Xid.class);

                            Mockito.when(mockXid.getBranchQualifier()).thenReturn(new byte[] {0});
                            Mockito.when(mockXid.getGlobalTransactionId()).thenReturn(new byte[] {1});
                            Mockito.when(mockXid.getFormatId()).thenReturn(2);

                            xaRes.start(mockXid, 0);
                        } catch (XAException e) {
                            e.printStackTrace();
                        }
                        return true;
                    }

                    @Override
                    public int getStatus() throws SystemException {
                        return 0;
                    }

                    @Override
                    public void registerSynchronization(Synchronization synch) throws IllegalStateException, RollbackException, SystemException {
                        syncs.add(synch);
                    }

                    @Override
                    public void rollback() throws IllegalStateException, SystemException {
                    }

                    @Override
                    public void setRollbackOnly() throws IllegalStateException, SystemException {
                    }
                };
            }

            @Override
            public void resume(Transaction tobj) throws IllegalStateException, InvalidTransactionException, SystemException {
            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {
            }

            @Override
            public Transaction suspend() throws SystemException {
                return null;
            }
        });

        TopicConnection connection = (TopicConnection) pcf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        assertTrue(session instanceof JmsPoolSession);
        JmsPoolSession pooledSession = (JmsPoolSession) session;
        assertTrue(pooledSession.getInternalSession() instanceof ActiveMQXASession);

        TopicPublisher publisher = session.createPublisher(topic);
        publisher.publish(session.createMessage());

        // simulate a commit
        for (Synchronization sync : syncs) {
            sync.beforeCompletion();
        }
        for (Synchronization sync : syncs) {
            sync.afterCompletion(1);
        }
        connection.close();

        pcf.stop();
    }

    @Test
    public void testAckModeOfPoolNonXAWithTM() throws Exception {
        final Vector<Synchronization> syncs = new Vector<Synchronization>();
        ActiveMQTopic topic = new ActiveMQTopic("test");

        JmsPoolXAConnectionFactory pcf = createXAPooledConnectionFactory();
        ((ActiveMQXAConnectionFactory) pcf.getConnectionFactory()).setXaAckMode(Session.CLIENT_ACKNOWLEDGE);
        pcf.setConnectionFactory(new XAConnectionFactoryOnly((XAConnectionFactory) pcf.getConnectionFactory()));

        // simple TM that is in a tx and will track syncs
        pcf.setTransactionManager(new TransactionManager(){
            @Override
            public void begin() throws NotSupportedException, SystemException {
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException, SystemException {
            }

            @Override
            public int getStatus() throws SystemException {
                return Status.STATUS_NO_TRANSACTION;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
                return new Transaction() {
                    @Override
                    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException {
                    }

                    @Override
                    public boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
                        return false;
                    }

                    @Override
                    public boolean enlistResource(XAResource xaRes) throws IllegalStateException, RollbackException, SystemException {
                        return false;
                    }

                    @Override
                    public int getStatus() throws SystemException {
                        return 0;
                    }

                    @Override
                    public void registerSynchronization(Synchronization synch) throws IllegalStateException, RollbackException, SystemException {
                        syncs.add(synch);
                    }

                    @Override
                    public void rollback() throws IllegalStateException, SystemException {
                    }

                    @Override
                    public void setRollbackOnly() throws IllegalStateException, SystemException {
                    }
                };

            }

            @Override
            public void resume(Transaction tobj) throws IllegalStateException, InvalidTransactionException, SystemException {
            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {
            }

            @Override
            public Transaction suspend() throws SystemException {
                return null;
            }
        });

        TopicConnection connection = (TopicConnection) pcf.createConnection();
        TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        assertEquals(Session.CLIENT_ACKNOWLEDGE, session.getAcknowledgeMode(), "client ack is enforce");
        TopicPublisher publisher = session.createPublisher(topic);
        publisher.publish(session.createMessage());

        // simulate a commit
        for (Synchronization sync : syncs) {
            sync.beforeCompletion();
        }
        for (Synchronization sync : syncs) {
            sync.afterCompletion(1);
        }
        connection.close();
        pcf.stop();
    }

    @Test
    public void testInstanceOf() throws  Exception {
        JmsPoolXAConnectionFactory pcf = createXAPooledConnectionFactory();
        assertTrue(pcf instanceof QueueConnectionFactory);
        assertTrue(pcf instanceof TopicConnectionFactory);
        pcf.stop();
    }

    @Test
    public void testBindable() throws Exception {
        JmsPoolXAConnectionFactory pcf = createXAPooledConnectionFactory();
        assertTrue(pcf instanceof ObjectFactory);
        assertTrue(((ObjectFactory)pcf).getObjectInstance(null, null, null, null) instanceof JmsPoolXAConnectionFactory);
        assertTrue(pcf.isTmFromJndi());
        pcf.stop();
    }

    @Test
    public void testBindableEnvOverrides() throws Exception {
        JmsPoolXAConnectionFactory pcf = createXAPooledConnectionFactory();
        assertTrue(pcf instanceof ObjectFactory);
        Hashtable<String, String> environment = new Hashtable<String, String>();
        environment.put("tmFromJndi", String.valueOf(Boolean.FALSE));
        assertTrue(((ObjectFactory) pcf).getObjectInstance(null, null, null, environment) instanceof JmsPoolXAConnectionFactory);
        assertFalse(pcf.isTmFromJndi());
        pcf.stop();
    }

    @Test
    public void testSenderAndPublisherDest() throws Exception {
        JmsPoolXAConnectionFactory pcf = createXAPooledConnectionFactory();

        QueueConnection connection = pcf.createQueueConnection();
        QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        QueueSender sender = session.createSender(session.createQueue("AA"));
        assertNotNull(sender.getQueue().getQueueName());

        connection.close();

        TopicConnection topicConnection = pcf.createTopicConnection();
        TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicPublisher topicPublisher = topicSession.createPublisher(topicSession.createTopic("AA"));
        assertNotNull(topicPublisher.getTopic().getTopicName());

        topicConnection.close();
        pcf.stop();
    }

    @Test
    public void testSessionArgsIgnoredWithTm() throws Exception {
        JmsPoolXAConnectionFactory pcf = createXAPooledConnectionFactory();

        // simple TM that with no tx
        pcf.setTransactionManager(new TransactionManager() {
            @Override
            public void begin() throws NotSupportedException, SystemException {
                throw new SystemException("NoTx");
            }

            @Override
            public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, RollbackException, SecurityException, SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public int getStatus() throws SystemException {
                return Status.STATUS_NO_TRANSACTION;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
                throw new SystemException("NoTx");
            }

            @Override
            public void resume(Transaction tobj) throws IllegalStateException, InvalidTransactionException, SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {
                throw new IllegalStateException("NoTx");
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {
            }

            @Override
            public Transaction suspend() throws SystemException {
                throw new SystemException("NoTx");
            }
        });

        QueueConnection connection = pcf.createQueueConnection();
        // like ee tck
        assertNotNull(connection.createQueueSession(false, 0), "can create session(false, 0)");

        connection.close();
        pcf.stop();
    }

    static class XAConnectionFactoryOnly implements XAConnectionFactory {
        private final XAConnectionFactory connectionFactory;

        XAConnectionFactoryOnly(XAConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        @Override
        public XAConnection createXAConnection() throws JMSException {
            return connectionFactory.createXAConnection();
        }

        @Override
        public XAConnection createXAConnection(String userName, String password) throws JMSException {
            return connectionFactory.createXAConnection(userName, password);
        }

        public XAJMSContext createXAContext() {
            return null;
        }

        public XAJMSContext createXAContext(String userName, String password) {
            return null;
        }
    }
}
