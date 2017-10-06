# JMS Pool configuration

This file details various configuration options for the JMS Pool, such as how to control the number of connection stored in the pool etc.

## Creating a Pooling ConnectionFactory

The JMS Pool operates as a standard JMS ConnectionFactory instance that wraps the ConnectionFactory of your chosen JMS Provider and manages the lifetime of Connections from that provider based on the configuration of the JMS Pool.  The JMS Pool can be configured to share a single JMS Connection or a number of Connections amongst callers to the Pool's createConnection methods.

## Configuration Options

The JMS Pool's ConnectionFactory implementation exposes a number of configuration options that control the behavior of the pool and the JMS resources it manages.  There are settings that control how many Connections are created in the pool and how long those Connections can remain idle etc.  There are also settings that control how many Sessions a loaned Connection from the pool is allowed to create and how the pooled Connection behaves when it cannot create any new Sessions for a given lonaed Connection.

### Connection Related Options

These options affect how the JMS pool creates and manages the Connections in the pool.

+ **maxConnections** Determines the maximum number of Connections the the pool maintains in a single Connection pool (defaults to one).  The pooled ConnectionFactory manages a pool of connection per each unique user + password combination value used to create connection, plus a separate pool for anonymous Connections (those without user-name or password).
+ **createConnectionOnStartup** When true the pool after a call to start() will attempt to create an initial Connection using the default createConnection() method of the configured JMS Provider ConnectionFactory.  This option defaults to true and if the call to createConnection on start fails the pool will wait until the next call to a createConnection method before reporting errors to the client or creating a valid Connection if given proper login credentials.
+ **idleTimeout** The idle timeout (default 30 seconds) controls how long a Connection that hasn't been or currently isn't loaned out to any client will remain idle in the Connection pool before it is eligible to be closed and discarded.  To disable idle timeouts the value should be set to 0 or a negative number.
+ **expiryTimeout** The expiration timeout (default is 0ms or disabled) control how long a Connection can live before being eligible for closure regardless of the Connection being on loan from the pool at the time of expiration.  When set to a non-zero positive value the Connection will be considered expired after that amount of time, but may not be closed until it has either been returned to the pool if on loan or an attempt to borrow a connection encounters the expired Connection.
+ **timeBetweenExpirationCheckMillis** used to establish a periodic check for expired Connections which will close all Connection that have exceeded the set expiration value.  This value is set to 0ms by default and only activates if set to a positive non-zero value.
+ **reconnectOnException** when true (default) this option controls if a Connection that throws an error that is captured by the ExceptionListener registered by the pool on all Connections it creates will trigger the pool to close the connection and attempt immediately add a new Connection to the pool to replace the assumed failed Connection.  A non-idle Connection will still be linked to the Connection, the client must handle Connection errors like any JMS API user would and close the current Connection and create a new one.
+ **useProviderJMSContext** by default the JMS pool will use it's own generic JMSContext classes to wrap a Connection borrowed from the pool instead of using the JMSContext functionality of the JMS ConnectionFactory that was configured.  This generic JMSContext implementation may be limited compared to the Provider version and if that functionality is critical to the application this option can be enabled to force the pool to use the Provider JMSContext implementation.  When enabled the JMSContext API is then not part of the Connections that are pooled by this JMS Connection pooling library.

## Session Related Options

These options affect the behavior of Sessions that are created from the pooled Connections.

+ **maximumActiveSessionPerConnection** For each Connection in the pool there can be a configured maximum number of Sessions that the pooled Connection will loan out before either blocking or throwing an error (based on configuration).  By default this value is 500 meaning that each provider Connection is limited to 500 sessions, this limit can be disabled by setting the value to a negative number.
+ **blockIfSessionPoolIsFull** When true (default) a call to createSession on a Connection from the pool will block until another previously created and loaned out session is closed an thereby becomes available.  When false a call to createSession when no Session is available will throw an IllegalStateException to indicate that the Connection is not able to provide a new Session at that time.
+ **blockIfSessionPoolIsFullTimeout** When the blockIfSessionPoolIsFull option is enabled and this value is set then a call to createSession that has blocked awaiting a Session will wait for the specified number of milliseconds before throwing an IllegalStateException.  By default this value is set to -1 indicating that the createSession call should block forever if configured to wait.
+ **useAnonymousProducers** By default a Session that has been loaned out on a call to createSession will use a single anonymous JMS MessageProducer as the underlying producer for all calls to createProducer.  In some rare cases this is not desirable and this feature can be disabled using this option, when disabled every call to createProducer will result in a new MessageProcuder instance being created.
