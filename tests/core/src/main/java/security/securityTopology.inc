INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_5.inc;


hydra.VmPrms-extraClassPaths = fcn "hydra.TestConfigFcns.duplicate
                                  (\"$GEMFIRE/lib/antlr.jar\", ${bridgeHosts}, true)"
                               ncf;
                                                             
hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${writerHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${readerHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${queryHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${invalidHosts})" ncf ;
  
hydra.GemFirePrms-conserveSockets = false;

hydra.ConfigPrms-cacheConfig = cache;
hydra.CachePrms-names = cache;

hydra.RegionPrms-names          = bridge    writer reader query invalid;
hydra.RegionPrms-scope          = dack      local local local local;
hydra.RegionPrms-dataPolicy     = replicate normal normal normal normal;
hydra.RegionPrms-poolName    = none      brloader brloader brloader brloader;

hydra.BridgePrms-names                = bridge;


hydra.PoolPrms-names                       = brloader ;
hydra.PoolPrms-threadLocalConnections         = false;
hydra.PoolPrms-minConnections        = 5;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-multiuserAuthentication = false;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRoundRobinEndpoints;
hydra.Prms-useFixedRandomInMaster = true;
hydra.log.LogPrms-mergeLogFiles = onFailure;

hydra.SecurityPrms-names = dummyScheme ldapScheme pkcsScheme;
hydra.SecurityPrms-clientAuthInit          = templates.security.UserPasswordAuthInit.create templates.security.UserPasswordAuthInit.create templates.security.PKCSAuthInit.create;
hydra.SecurityPrms-clientAuthenticator = templates.security.DummyAuthenticator.create templates.security.LdapUserAuthenticator.create templates.security.PKCSAuthenticator.create;
hydra.SecurityPrms-clientExtraProperties = security.DummyUserPasswordPrms security.LdapUserPasswordPrms security.PKCSUserPasswordPrms;
hydra.SecurityPrms-clientAccessor = templates.security.XmlAuthorization.create;

security.DummyUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-dummy.xml;
security.LdapUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-ldap.xml;
security.PKCSUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-ldap.xml;
security.LdapUserPasswordPrms-ldapServer = "ldap";
security.LdapUserPasswordPrms-ldapBasedn = "ou=ldapTesting,dc=pune,dc=gemstone,dc=com";
security.PKCSUserPasswordPrms-publickeyFilepath =
  fcn "security.SecurityFcns.getKeystore(\"publickeyfile\")" ncf;
security.PKCSUserPasswordPrms-keystorepass="gemfire";

security.SecurityClientsPrms-numKeyRangePerThread = ${nPutKeyRange};
security.SecurityClientsPrms-regionName = Portfolios;
security.SecurityClientsPrms-killInterval = 100;
security.SecurityClientsPrms-restartWaitSec = 60;

hydra.SecurityPrms-logLevel = info;
//hydra.GemFirePrms-logLevel = info;

// prms related to CQ
cq.CQUtilPrms-cqListener = cq.ha.CQOpListener;

THREADGROUP bridge
    totalThreads = fcn ${bridgeHosts} * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"bridge\", ${bridgeHosts}, true)" ncf;                      
                       
THREADGROUP writer
    totalThreads = fcn ${writerHosts} * ${writerVMsPerHost} * ${writerThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"writer\", ${writerHosts}, true)" ncf;
    
THREADGROUP reader
    totalThreads = fcn ${readerHosts} * ${readerVMsPerHost} * ${readerThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"reader\", ${readerHosts}, true)" ncf;
                            
THREADGROUP query
    totalThreads = fcn ${queryHosts} * ${queryVMsPerHost} * ${queryThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"query\", ${queryHosts}, true)" ncf;
    
THREADGROUP invalid
    totalThreads = fcn ${invalidHosts} * ${invalidVMsPerHost} * ${invalidThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"invalid\", ${invalidHosts}, true)" ncf;
                       

    
                                                
                        
                        
