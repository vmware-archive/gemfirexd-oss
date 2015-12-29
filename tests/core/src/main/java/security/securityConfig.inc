hydra.SecurityPrms-names = dummyScheme ldapScheme	pkcsScheme;
hydra.SecurityPrms-peerAuthInit = templates.security.UserPasswordAuthInit.create templates.security.UserPasswordAuthInit.create templates.security.PKCSAuthInit.create;
hydra.SecurityPrms-peerExtraProperties = security.DummyUserPasswordPrms security.LdapUserPasswordPrms security.PKCSUserPasswordPrms;
hydra.SecurityPrms-peerAuthenticator   = templates.security.DummyAuthenticator.create templates.security.LdapUserAuthenticator.create templates.security.PKCSAuthenticator.create;
hydra.SecurityPrms-clientAuthInit          = templates.security.UserPasswordAuthInit.create templates.security.UserPasswordAuthInit.create templates.security.PKCSAuthInit.create;
hydra.SecurityPrms-clientAuthenticator = templates.security.DummyAuthenticator.create templates.security.LdapUserAuthenticator.create templates.security.PKCSAuthenticator.create;
hydra.SecurityPrms-clientExtraProperties = security.DummyUserPasswordPrms security.LdapUserPasswordPrms security.PKCSUserPasswordPrms;
hydra.SecurityPrms-clientAccessor = templates.security.XmlAuthorization.create;

security.DummyUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-dummy.xml;
security.LdapUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-ldap.xml;
security.PKCSUserPasswordPrms-authzXmlUri = $JTESTS/lib/authz-ldap.xml;
security.LdapUserPasswordPrms-ldapServer = "ldap";
security.LdapUserPasswordPrms-ldapBasedn = "ou=ldapTesting,dc=pune,dc=gemstone,dc=com";
security.SecurityClientsPrms-regionName = Portfolios;
security.PKCSUserPasswordPrms-publickeyFilepath =
  fcn "security.SecurityFcns.getKeystore(\"publickeyfile\")" ncf;
security.PKCSUserPasswordPrms-keystorepass="gemfire";

security.SecurityClientsPrms-killInterval = 100;
security.SecurityClientsPrms-restartWaitSec = 60;

hydra.RegionPrms-names          = bridge edge;
hydra.RegionPrms-poolName    = none brloader;
        
hydra.ConfigPrms-cacheConfig  = cache;
hydra.CachePrms-names         = cache;
          
hydra.PoolPrms-names                       = brloader ;
hydra.PoolPrms-threadLocalConnections         = false;
hydra.PoolPrms-minConnections        = 5;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRoundRobinEndpoints;
