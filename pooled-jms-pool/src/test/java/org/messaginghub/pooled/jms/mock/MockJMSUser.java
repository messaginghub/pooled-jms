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

import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSSecurityException;

/**
 * Mock JMS User object containing authentication and authorization data.
 */
public class MockJMSUser {

    public static final MockJMSUser DEFAULT_USER = new MockJMSUser(true, null);
    public static final MockJMSUser INVALID_CREDENTIALS = new MockJMSUser(false, "Username or password was invalid");
    public static final MockJMSUser INVALID_ANONYMOUS = new MockJMSUser(false, "Anonymous users not allowed");

    private final String username;
    private final String password;
    private final boolean authenticated;
    private final String failureCause;

    private boolean canConsumeAll = true;
    private boolean canProduceAll = true;
    private boolean canProducerAnonymously = true;
    private boolean canBrowseAll = true;

    private final Set<String> consumableDestinations = new HashSet<>();
    private final Set<String> writableDestinations = new HashSet<>();
    private final Set<String> browsableDestinations = new HashSet<>();

    MockJMSUser(boolean valid, String cause) {
        this.username = null;
        this.password = null;
        this.authenticated = valid;
        this.failureCause = cause;
    }

    public MockJMSUser(String username, String password) {
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Username must not be null or empty");
        }

        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Password must not be null or empty");
        }

        this.username = username;
        this.password = password;
        this.authenticated = true;
        this.failureCause = null;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isValid() {
        return authenticated;
    }

    public String getFailureCause() {
        return failureCause;
    }

    public void addConsumableDestination(String name) {
        this.setCanConsumeAll(false);
        this.consumableDestinations.add(name);
    }

    public void addWritableDestination(String name) {
        this.setCanProduceAll(false);
        this.writableDestinations.add(name);
    }

    public void addBrowsableDestination(String name) {
        this.canBrowseAll = false;
        this.browsableDestinations.add(name);
    }

    public void checkCanConsume(MockJMSDestination destination) throws JMSSecurityException {
        if (!isCanConsumeAll() && !consumableDestinations.contains(destination.getName())) {
            throw new JMSSecurityException("User " + username + " cannot consume from destination: " + destination.getName());
        }
    }

    public void checkCanProduce(MockJMSDestination destination) throws JMSSecurityException {
        if (destination == null) {
            if (isCanProducerAnonymously()) {
                return;
            } else {
               throw new JMSSecurityException("User " + username + " not allowed for create anonymous produders.");
            }
        }

        if (!isCanProduceAll() && !writableDestinations.contains(destination.getName())) {
            throw new JMSSecurityException("User " + username + " cannot read from destination: " + destination.getName());
        }
    }

    public void checkCanBrowse(MockJMSDestination destination) throws JMSSecurityException {
        if (!isCanBrowseAll() && !browsableDestinations.contains(destination.getName())) {
            throw new JMSSecurityException("User " + username + " cannot browse destination: " + destination.getName());
        }
    }

    public boolean isCanProduceAll() {
        return canProduceAll;
    }

    public void setCanProduceAll(boolean canProduceAll) {
        this.canProduceAll = canProduceAll;
    }

    public boolean isCanProducerAnonymously() {
        return canProducerAnonymously;
    }

    public void setCanProducerAnonymously(boolean canProducerAnonymously) {
        this.canProducerAnonymously = canProducerAnonymously;
    }

    public boolean isCanConsumeAll() {
        return canConsumeAll;
    }

    public void setCanConsumeAll(boolean canConsumeAll) {
        this.canConsumeAll = canConsumeAll;
    }

    public boolean isCanBrowseAll() {
        return canBrowseAll;
    }

    public void setCanBrowseAll(boolean canBrowseAll) {
        this.canBrowseAll = canBrowseAll;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        MockJMSUser other = (MockJMSUser) obj;
        if (password == null) {
            if (other.password != null) {
                return false;
            }
        } else if (!password.equals(other.password)) {
            return false;
        }

        if (username == null) {
            if (other.username != null) {
                return false;
            }
        } else if (!username.equals(other.username)) {
            return false;
        }

        return true;
    }
}
