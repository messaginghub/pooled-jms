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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.jms.JMSException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthenticationUser;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.activemq.security.TempDestinationAuthorizationEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActiveMQJmsPoolTestSupport {

    @Rule public TestName name = new TestName();

    protected static final Logger LOG = LoggerFactory.getLogger(ActiveMQJmsPoolTestSupport.class);

    private static final String ADMIN = "admin";
    private static final String USER = "user";
    private static final String GUEST = "guest";
    private static final String USER_PASSWORD = "password";

    protected BrokerService brokerService;
    protected String connectionURI;
    protected ActiveMQConnectionFactory amqFactory;
    protected ActiveMQXAConnectionFactory amqXAFactory;
    protected ActiveMQConnectionFactory amqFailoverFactory;

    @Before
    public void setUp() throws Exception {
        LOG.info("========== start " + getTestName() + " ==========");

        connectionURI = createBroker();
        amqFactory = new ActiveMQConnectionFactory(connectionURI);
        amqXAFactory = new ActiveMQXAConnectionFactory(connectionURI);
        amqFailoverFactory = new ActiveMQConnectionFactory("failover:" + connectionURI);
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            try {
                brokerService.stop();
                brokerService.waitUntilStopped();
                brokerService = null;
            } catch (Exception ex) {
                LOG.warn("Suppress error on shutdown: {}", ex);
            }
        }

        LOG.info("========== tearDown " + getTestName() + " ==========");
    }

    public String getTestName() {
        return name.getMethodName();
    }

    protected String createBroker() throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.getManagementContext().setCreateMBeanServer(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        brokerService.addConnector("tcp://localhost:0").setName("test");

        ArrayList<BrokerPlugin> plugins = new ArrayList<BrokerPlugin>();

        List<AuthenticationUser> users = new ArrayList<AuthenticationUser>();
        users.add(new AuthenticationUser(USER, USER_PASSWORD, "users"));
        users.add(new AuthenticationUser(GUEST, USER_PASSWORD, "guests"));
        users.add(new AuthenticationUser(ADMIN, ADMIN, "admins"));

        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin(users);
        authenticationPlugin.setAnonymousAccessAllowed(true);

        plugins.add(authenticationPlugin);
        plugins.add(configureAuthorization());

        brokerService.setPlugins(plugins.toArray(new BrokerPlugin[2]));

        brokerService.start();
        brokerService.waitUntilStarted();

        return brokerService.getTransportConnectorByName("test").getPublishableConnectString();
    }

    protected JmsPoolConnectionFactory createPooledConnectionFactory() {
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amqFactory);
        cf.setMaxConnections(1);
        LOG.debug("ConnectionFactory initialized.");
        return cf;
    }

    protected JmsPoolConnectionFactory createFailoverPooledConnectionFactory() {
        JmsPoolConnectionFactory cf = new JmsPoolConnectionFactory();
        cf.setConnectionFactory(amqFailoverFactory);
        cf.setMaxConnections(1);
        LOG.debug("Failover ConnectionFactory initialized.");
        return cf;
    }

    protected JmsPoolXAConnectionFactory createXAPooledConnectionFactory() {
        JmsPoolXAConnectionFactory cf = new JmsPoolXAConnectionFactory();
        cf.setConnectionFactory(amqXAFactory);
        cf.setMaxConnections(1);
        LOG.debug("Failover ConnectionFactory initialized.");
        return cf;
    }

    protected BrokerViewMBean getProxyToBroker() throws MalformedObjectNameException, JMSException {
        ObjectName brokerViewMBean = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName());
        BrokerViewMBean proxy = (BrokerViewMBean) brokerService.getManagementContext()
                .newProxyInstance(brokerViewMBean, BrokerViewMBean.class, true);
        return proxy;
    }

    protected ConnectorViewMBean getProxyToConnectionView(String connectionType) throws Exception {
        ObjectName connectorQuery = new ObjectName(
            "org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName() + ",connector=clientConnectors,connectorName="+connectionType+"_//*");

        Set<ObjectName> results = brokerService.getManagementContext().queryNames(connectorQuery, null);

        if (results == null || results.isEmpty() || results.size() > 1) {
            throw new Exception("Unable to find the exact Connector instance.");
        }

        ConnectorViewMBean proxy = (ConnectorViewMBean) brokerService.getManagementContext()
                .newProxyInstance(results.iterator().next(), ConnectorViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToQueue(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName() + ",destinationType=Queue,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected QueueViewMBean getProxyToTopic(String name) throws MalformedObjectNameException, JMSException {
        ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerService.getBrokerName() + ",destinationType=Topic,destinationName="+name);
        QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext()
                .newProxyInstance(queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

    protected BrokerPlugin configureAuthorization() throws Exception {

        @SuppressWarnings("rawtypes")
        List<DestinationMapEntry> authorizationEntries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        entry.setRead("admins,anonymous");
        entry.setWrite("admins,anonymous");
        entry.setAdmin("admins,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setQueue("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic(">");
        entry.setRead("admins,anonymous");
        entry.setWrite("admins,anonymous");
        entry.setAdmin("admins,anonymous");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("USERS.>");
        entry.setRead("users");
        entry.setWrite("users");
        entry.setAdmin("users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("GUEST.>");
        entry.setRead("guests");
        entry.setWrite("guests,users");
        entry.setAdmin("guests,users");
        authorizationEntries.add(entry);
        entry = new AuthorizationEntry();
        entry.setTopic("ActiveMQ.Advisory.>");
        entry.setRead("guests,users,anonymous");
        entry.setWrite("guests,users,anonymous");
        entry.setAdmin("guests,users,anonymous");
        authorizationEntries.add(entry);

        TempDestinationAuthorizationEntry tempEntry = new TempDestinationAuthorizationEntry();
        tempEntry.setRead("admins,anonymous");
        tempEntry.setWrite("admins,anonymous");
        tempEntry.setAdmin("admins,anonymous");

        DefaultAuthorizationMap authorizationMap = new DefaultAuthorizationMap(authorizationEntries);
        authorizationMap.setTempDestinationAuthorizationEntry(tempEntry);
        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin(authorizationMap);

        return authorizationPlugin;
    }
}