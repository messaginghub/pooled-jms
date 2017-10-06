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

import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;

/**
 * Mock JMS ObjectMessage implementation.
 */
public class MockJMSObjectMessage extends MockJMSMessage implements ObjectMessage {

    private Serializable object;

    @Override
    public void setObject(Serializable object) throws JMSException {
        this.object = object;
    }

    @Override
    public Serializable getObject() throws JMSException {
        return object;
    }

    @Override
    public boolean isBodyAssignableTo(@SuppressWarnings("rawtypes") Class target) throws JMSException {
        if (object == null) {
            return true;
        }

        return Serializable.class == target || Object.class == target || target.isInstance(getObject());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        try {
            return (T) getObject();
        } catch (JMSException e) {
            throw new MessageFormatException("Failed to read Object: " + e.getMessage());
        }
    }
}
