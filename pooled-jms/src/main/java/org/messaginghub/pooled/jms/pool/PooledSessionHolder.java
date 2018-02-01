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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

import org.messaginghub.pooled.jms.JmsPoolMessageProducer;
import org.messaginghub.pooled.jms.JmsPoolQueueSender;
import org.messaginghub.pooled.jms.JmsPoolSession;
import org.messaginghub.pooled.jms.JmsPoolTopicPublisher;
import org.messaginghub.pooled.jms.util.LRUCache;

/**
 * Used to store a pooled session instance and any resources that can
 * be left open and carried along with the pooled instance such as the
 * anonymous producer used for all MessageProducer instances created
 * from this pooled session when enabled.
 */
public final class PooledSessionHolder {

    private final PooledConnection connection;
    private final Session session;

    private final boolean useAnonymousProducer;
    private final int explicitProducerCacheSize;

    private volatile MessageProducer anonymousProducer;
    private volatile TopicPublisher anonymousPublisher;
    private volatile QueueSender anonymousSender;

    private final ProducerLRUCache<JmsPoolMessageProducer> cachedProducers;
    private final ProducerLRUCache<JmsPoolTopicPublisher> cachedPublishers;
    private final ProducerLRUCache<JmsPoolQueueSender> cachedSenders;

    public PooledSessionHolder(PooledConnection connection, Session session, boolean useAnonymousProducer, int namedProducerCacheSize) {
        this.connection = connection;
        this.session = session;
        this.useAnonymousProducer = useAnonymousProducer;
        this.explicitProducerCacheSize = namedProducerCacheSize;

        if (!useAnonymousProducer && namedProducerCacheSize > 0) {
            cachedProducers = new ProducerLRUCache<>(namedProducerCacheSize);
            cachedPublishers = new ProducerLRUCache<>(namedProducerCacheSize);
            cachedSenders = new ProducerLRUCache<>(namedProducerCacheSize);
        } else {
            cachedProducers = null;
            cachedPublishers = null;
            cachedSenders = null;
        }
    }

    public void close() throws JMSException {
        try {
            session.close();
        } finally {
            anonymousProducer = null;
            anonymousPublisher = null;
            anonymousSender = null;

            if (cachedProducers != null) {
                cachedProducers.clear();
            }
            if (cachedPublishers != null) {
                cachedPublishers.clear();
            }
            if (cachedSenders != null) {
                cachedSenders.clear();
            }
        }
    }

    public Session getSession() {
        return session;
    }

    public void onJmsPoolProducerClosed(JmsPoolMessageProducer producer) throws JMSException {
        synchronized (this) {
            // We cache anonymous producers regardless of the useAnonymousProducer
            // setting so in either of those cases the pooled producer is not closed.
            if (isUseAnonymousProducer() || producer.isAnonymousProducer()) {
                return;
            } else if (producer.getRefCount().decrementAndGet() <= 0) {
                producer.getDelegate().close();
            }
        }
    }

    public JmsPoolMessageProducer getOrCreateProducer(JmsPoolSession jmsPoolSession, Destination destination) throws JMSException {
        MessageProducer delegate = null;
        AtomicInteger refCount = null;

        synchronized (this) {
            if (isUseAnonymousProducer() || destination == null) {
                delegate = anonymousProducer;
                if (delegate == null) {
                    delegate = anonymousProducer = session.createProducer(null);
                }
            } else if (explicitProducerCacheSize > 0) {
                JmsPoolMessageProducer cached = cachedProducers.get(destination);
                if (cached == null) {
                    delegate = session.createProducer(destination);
                    refCount = new AtomicInteger(1);
                    cached = new JmsPoolMessageProducer(jmsPoolSession, delegate, destination, refCount);

                    cachedProducers.put(destination, cached);
                } else {
                    delegate = cached.getDelegate();
                    refCount = cached.getRefCount();
                }

                refCount.incrementAndGet();
            } else {
                delegate = session.createProducer(destination);
                refCount = new AtomicInteger(1);
            }
        }

        return new JmsPoolMessageProducer(jmsPoolSession, delegate, destination, refCount);
    }

    public JmsPoolTopicPublisher getOrCreatePublisher(JmsPoolSession jmsPoolSession, Topic topic) throws JMSException {
        TopicPublisher delegate = null;
        AtomicInteger refCount = null;

        synchronized (this) {
            if (isUseAnonymousProducer() || topic == null) {
                delegate = anonymousPublisher;
                if (delegate == null) {
                    delegate = anonymousPublisher = ((TopicSession) session).createPublisher(null);
                }
            } else if (explicitProducerCacheSize > 0) {
                JmsPoolTopicPublisher cached = cachedPublishers.get(topic);
                if (cached == null) {
                    delegate = ((TopicSession) session).createPublisher(topic);
                    refCount = new AtomicInteger(1);
                    cached = new JmsPoolTopicPublisher(jmsPoolSession, delegate, topic, refCount);

                    cachedPublishers.put(topic, cached);
                } else {
                    delegate = (TopicPublisher) cached.getDelegate();
                    refCount = cached.getRefCount();
                }

                refCount.incrementAndGet();
            } else {
                delegate = ((TopicSession) session).createPublisher(topic);
                refCount = new AtomicInteger(1);
            }
        }

        return new JmsPoolTopicPublisher(jmsPoolSession, delegate, topic, refCount);
    }

    public JmsPoolQueueSender getOrCreateSender(JmsPoolSession jmsPoolSession, Queue queue) throws JMSException {
        QueueSender delegate = null;
        AtomicInteger refCount = null;

        synchronized (this) {
            if (isUseAnonymousProducer() || queue == null) {
                delegate = anonymousSender;
                if (delegate == null) {
                    delegate = anonymousSender = ((QueueSession) session).createSender(null);
                }
            } else if (explicitProducerCacheSize > 0) {
                JmsPoolQueueSender cached = cachedSenders.get(queue);
                if (cached == null) {
                    delegate = ((QueueSession) session).createSender(queue);
                    refCount = new AtomicInteger(1);
                    cached = new JmsPoolQueueSender(jmsPoolSession, delegate, queue, refCount);

                    cachedSenders.put(queue, cached);
                } else {
                    delegate = (QueueSender) cached.getDelegate();
                    refCount = cached.getRefCount();
                }

                refCount.incrementAndGet();
            } else {
                delegate = ((QueueSession) session).createSender(queue);
                refCount = new AtomicInteger(1);
            }
        }

        return new JmsPoolQueueSender(jmsPoolSession, delegate, queue, refCount);
    }

    public PooledConnection getConnection() {
        return connection;
    }

    public boolean isUseAnonymousProducer() {
        return useAnonymousProducer;
    }

    @Override
    public String toString() {
        return session.toString();
    }

    private static class ProducerLRUCache<E> extends LRUCache<Destination, E> {

        private static final long serialVersionUID = -1;

        public ProducerLRUCache(int maximumCacheSize) {
            super(maximumCacheSize);
        }

        @Override
        protected void onCacheEviction(Map.Entry<Destination, E> eldest) {
             JmsPoolMessageProducer producer = (JmsPoolMessageProducer) eldest.getValue();
             try {
                 producer.close();
             } catch (JMSException jmsEx) {}
        }
    }
}
