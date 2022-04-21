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
import jakarta.jms.Message;

/**
 * Listen in on events in the MockJMSConnection and influence outcomes
 */
public interface MockJMSConnectionListener {

    void onCreateTemporaryQueue(MockJMSTemporaryQueue queue) throws JMSException;

    void onDeleteTemporaryQueue(MockJMSTemporaryQueue queue) throws JMSException;

    void onCreateTemporaryTopic(MockJMSTemporaryTopic topic) throws JMSException;

    void onDeleteTemporaryTopic(MockJMSTemporaryTopic topic) throws JMSException;

    void onCreateSession(MockJMSSession session) throws JMSException;

    void onCloseSession(MockJMSSession session) throws JMSException;

    void onMessageSend(MockJMSSession session, MockJMSMessageProducer producer, Message message) throws JMSException;

    void onCreateMessageConsumer(MockJMSSession session, MockJMSMessageConsumer consumer) throws JMSException;

    void onCloseMessageConsumer(MockJMSSession session, MockJMSMessageConsumer consumer) throws JMSException;

    void onCreateMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException;

    void onCloseMessageProducer(MockJMSSession session, MockJMSMessageProducer producer) throws JMSException;

}
