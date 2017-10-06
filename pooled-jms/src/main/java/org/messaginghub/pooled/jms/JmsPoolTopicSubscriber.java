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

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

/**
 * A {@link TopicSubscriber} which was created by {@link JmsPoolSession}.
 */
public class JmsPoolTopicSubscriber extends JmsPoolMessageConsumer implements TopicSubscriber, AutoCloseable {

    /**
     * Wraps the TopicSubscriber.
     *
     * @param session
     * 		the pooled session that created this object.
     * @param delegate
     * 		the created QueueBrowser to wrap.
     */
    public JmsPoolTopicSubscriber(JmsPoolSession session, TopicSubscriber delegate) {
        super(session, delegate);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return getTopicSubscriber().getTopic();
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        return getTopicSubscriber().getNoLocal();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " { " + getDelegate() + " }";
    }

    public TopicSubscriber getTopicSubscriber() throws JMSException {
        return (TopicSubscriber) super.getMessageConsumer();
    }
}
