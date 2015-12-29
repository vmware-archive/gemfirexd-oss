The tests can be setup, built, deployed, and run following steps below:
1. Create a basic WLS domain (e.g. gfxddomain) with server (e.g. AdminServer) with default values
2. Create a JMS Server (gfxdJMSServer) in this domain and a JMS Module (gfxd-jms) with resources of a Connection Factory and a Queue
Connect Factory:
    Name -- weblogic.gfxdtest.jms.QueueConnectionFactory
    JNDI Name -- weblogic.gfxdtest.jms.QueueConnectionFactory
    XA Connection Factory Enabled
    Target -- AdminServer
    Others -- use default values
Queue:
    Name -- gfxdQueue
    JNDI Name -- weblogic.gfxdtest.jms.gfxdQueue
    Target -- gfxdJMSServer
    Others -- use default values
3. Create an XA datasource with the Derby DBMS:
    JNDI Name: derby-thin-loc-xads -- use org.apache.derby.jdbc.ClientXADataSource

4. Create four XA datasources to GemFireXD systems using Derby Template:
    JNDI Name: qlftest-thin-loc-xads -- use ThinClientXADataSource and connect to GemFireXD DS with a locator
    JNDI Name: qlftest-thin-mc-xads -- use ThinClientXADataSource and connect to multicast GemFireXD DS
    JNDI Name: qlftest-peer-loc-xads -- use EmbeddedXADataSource and connect to GemFireXD DS with a locator
    JNDI Name: qlftest-peer-mc-xads -- use EmbeddedXADataSource and connect to multicast GemFireXD DS
5. Go to directory gemfirexd_rebrand_Dec13/tests/sql/ecotests/wls/
6. Change the properties in testenv properties to specify the test properties
7. run setDomainEnv.sh (or setDomainEnv.cmd) under your WLS domain directory
8. ant all (which include targets clean, prepare, build.ear, compile.client, deploy)
9. ant test -Dtestsuite=[testsuiteToRun] -Drunthreads=[numberOfRunThreads] -Diterations=[numberOfRunIterations] 


Current tests consist of 48 cases running GemFireXD XA Txn using client or embedded driver, with JMS, Derby, or 
GemFireXD itself. These tests can be run with specified number of threads and for specified number of iterations
