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
package org.messaginghub.pooled.jms.util;

import java.util.concurrent.TimeUnit;

public class Wait {

    public static final long MAX_WAIT_MILLIS = 30 * 1000;
    public static final long SLEEP_MILLIS = 200;

    public interface Condition {
        boolean isSatisfied() throws Exception;
    }

    public static boolean waitFor(Condition condition) throws Exception {
        return waitFor(condition, MAX_WAIT_MILLIS);
    }

    public static boolean waitFor(final Condition condition, final long duration) throws Exception {
        return waitFor(condition, duration, SLEEP_MILLIS);
    }

    public static boolean waitFor(final Condition condition, final long duration, final long sleepMillis) throws Exception {
        final long expiry = System.currentTimeMillis() + duration;
        boolean conditionSatisfied = condition.isSatisfied();
        while (!conditionSatisfied && System.currentTimeMillis() < expiry) {
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
            conditionSatisfied = condition.isSatisfied();
        }
        return conditionSatisfied;
    }
}
