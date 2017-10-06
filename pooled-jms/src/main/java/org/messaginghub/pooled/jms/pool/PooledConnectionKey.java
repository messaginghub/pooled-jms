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

/**
 * A cache key for the connection details
 */
public final class PooledConnectionKey {

    private final String userName;
    private final String password;
    private int hash;

    /**
     * Creates a new ConnectionKey using the supplied values.
     *
     * @param userName
     * 		The user name that this key represents
     * @param password
     * 		The password that this key represents
     */
    public PooledConnectionKey(String userName, String password) {
        this.password = password;
        this.userName = userName;
        hash = 31;
        if (userName != null) {
            hash += userName.hashCode();
        }
        hash *= 31;
        if (password != null) {
            hash += password.hashCode();
        }
    }

    public String getPassword() {
        return password;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + hash;
        result = prime * result + ((password == null) ? 0 : password.hashCode());
        result = prime * result + ((userName == null) ? 0 : userName.hashCode());
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

        PooledConnectionKey other = (PooledConnectionKey) obj;
        if (hash != other.hash) {
            return false;
        }

        if (password == null) {
            if (other.password != null) {
                return false;
            }
        } else if (!password.equals(other.password)) {
            return false;
        }

        if (userName == null) {
            if (other.userName != null) {
                return false;
            }
        } else if (!userName.equals(other.userName)) {
            return false;
        }

        return true;
    }
}
