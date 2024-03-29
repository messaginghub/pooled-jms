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

import jakarta.jms.JMSContext;
import jakarta.jms.XAJMSContext;
import javax.transaction.xa.XAResource;

/**
 * XAJMSContext implementation that wraps a JmsPoolConnection
 */
public class JmsPoolXAJMSContext extends JmsPoolJMSContext implements XAJMSContext {

    public JmsPoolXAJMSContext(JmsPoolConnection connection, int sessionMode) {
        super(connection, sessionMode);
    }

    @Override
    public JMSContext getContext() {
        return this;
    }

    @Override
    public XAResource getXAResource() {
        return getSession().getXAResource();
    }
}
