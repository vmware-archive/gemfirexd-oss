INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_2.inc;


hydra.VmPrms-extraClassPaths = fcn "hydra.TestConfigFcns.duplicate
                                  (\"$GEMFIRE/lib/antlr.jar\", ${bridgeHosts}, true)"
                               ncf;
                                                             
hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${edgeHosts})" ncf
  ;
  
hydra.GemFirePrms-conserveSockets = false;

hydra.ConfigPrms-cacheConfig = cache;
hydra.CachePrms-names = cache;

hydra.RegionPrms-names          = bridge    edge;
hydra.RegionPrms-scope          = dack      local;
hydra.RegionPrms-dataPolicy     = replicate empty;
hydra.RegionPrms-poolName       = none      brloader;

hydra.BridgePrms-names                = bridge;
hct.HctPrms-receiveValuesAsInvalidates = false;

hydra.PoolPrms-names                       = brloader ;
hydra.PoolPrms-threadLocalConnections         = false;
hydra.PoolPrms-minConnections        = 5;
hydra.PoolPrms-subscriptionEnabled = false;
hydra.PoolPrms-multiuserAuthentication = true;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRoundRobinEndpoints;
hydra.Prms-useFixedRandomInMaster = true;
hydra.log.LogPrms-mergeLogFiles = onFailure;

hydra.SecurityPrms-names = dummyScheme ldapScheme pkcsScheme;
hydra.SecurityPrms-clientAuthInit          = templates.security.UserPasswordAuthInit.create templates.security.UserPasswordAuthInit.create templates.security.PKCSAuthInit.create;
hydra.SecurityPrms-clientAuthenticator = templates.security.DummyAuthenticator.create templates.security.LdapUserAuthenticator.create templates.security.PKCSAuthenticator.create;
hydra.SecurityPrms-clientExtraProperties = security.DummyUserPasswordPrms security.LdapUserPasswordPrms security.PKCSUserPasswordPrms;
hydra.SecurityPrms-clientAccessor = templates.security.XmlAuthorization.create;
hydra.SecurityPrms-clientAccessorPP = templates.security.XmlAuthorization.create;

security.DummyUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-multiUser-dummy.xml;
security.LdapUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-multiUser-ldap.xml;
security.PKCSUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-multiUser-ldap.xml;
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

// one verify thread per edge vm to verify each vm's view
THREADGROUP verifyThreads
    totalThreads = fcn "${edgeVMsPerHost} * ${edgeHosts}" ncf
    totalVMs = fcn "${edgeVMsPerHost} * ${edgeHosts}" ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"edge\", ${edgeHosts}, true)" ncf;

THREADGROUP bridge
    totalThreads = fcn ${bridgeHosts} * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"bridge\", ${bridgeHosts}, true)" ncf; 
                                            
// edgeThreads are all threads in the edge VMs minus 
// one thread per edge VM for the verifyThreads                       
THREADGROUP edge
    totalThreads = fcn (${edgeHosts} * ${edgeVMsPerHost} * ${edgeThreadsPerVM}) 
                    - (${edgeVMsPerHost} * ${edgeHosts}) ncf
    totalVMs = fcn "${edgeVMsPerHost} * ${edgeHosts}" ncf              
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"edge\", ${edgeHosts}, true)" ncf;

    
                                                
                        
                        
