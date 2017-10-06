# PooledJMS

JMS Connection pool for messaging applications that provides pooling for JMS Connections, Sessions and MessageProducers.

Below are some quick pointers you might find useful.

## Using the PooledJMS library

To use the PooledJMS library in your projects you can include the maven
dependency in your project pom file:

    <dependency>
      <groupId>org.messaginghub.jms</groupId>
      <artifactId>pooled-jms</artifactId>
      <version>${pooled-jms-version}</version>
    </dependency>

## Building the code

The project requires Maven 3. Some example commands follow.

Clean previous builds output and install all modules to local repository without
running the tests:

    mvn clean install -DskipTests

Install all modules to the local repository after running all the tests:

    mvn clean install

Perform a subset tests on the packaged release artifacts without
installing:

    mvn clean verify -Dtest=TestNamePattern*

Execute the tests and produce code coverage report:

    mvn clean test jacoco:report

## Examples

First build and install all the modules as detailed above (if running against
a source checkout/release, rather than against released binaries) and then
consult the README in the pooled-jms-examples module itself.

## Documentation

There is some basic documentation in the pooled-jms-docs module.


