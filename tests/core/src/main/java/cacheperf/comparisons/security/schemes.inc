//------------------------------------------------------------------------------
// SECURITY SCHEME NAMES
//
hydra.SecurityPrms-names =
  noop
  dummy
  ldap
  ;
//------------------------------------------------------------------------------
// CLIENT ACCESSOR
//
hydra.SecurityPrms-clientAccessor =
  cacheperf.comparisons.security.NoopAccessor.create // noop
  templates.security.XmlAuthorization.create // dummy
  templates.security.XmlAuthorization.create // ldap
  ;
//------------------------------------------------------------------------------
// CLIENT ACCESSOR PP
//
hydra.SecurityPrms-clientAccessorPP =
  none // noop
  none // dummy
  none // ldap
  ;
//------------------------------------------------------------------------------
// CLIENT AUTH INIT
//
hydra.SecurityPrms-clientAuthInit =
  cacheperf.comparisons.security.NoopAuthInit.create // noop
  templates.security.UserPasswordAuthInit.create // dummy
  templates.security.UserPasswordAuthInit.create // ldap
  ;
//------------------------------------------------------------------------------
// CLIENT AUTHENTICATOR
//
hydra.SecurityPrms-clientAuthenticator =
  cacheperf.comparisons.security.NoopAuthenticator.create // noop
  templates.security.DummyAuthenticator.create // dummy
  templates.security.LdapUserAuthenticator.create // ldap
  ;
//------------------------------------------------------------------------------
// CLIENT EXTRA PROPERTIES
//
hydra.SecurityPrms-clientExtraProperties =
  none // noop
  cacheperf.comparisons.security.DummyUserPasswordPrms // dummy
  cacheperf.comparisons.security.LdapUserPasswordPrms // ldap
  ;
//------------------------------------------------------------------------------
// PEER AUTH INIT
//
hydra.SecurityPrms-peerAuthInit =
  cacheperf.comparisons.security.NoopAuthInit.create // noop
  templates.security.UserPasswordAuthInit.create // dummy
  templates.security.UserPasswordAuthInit.create // ldap
  ;
//------------------------------------------------------------------------------
// PEER AUTHENTICATOR
//
hydra.SecurityPrms-peerAuthenticator =
  cacheperf.comparisons.security.NoopAuthenticator.create // noop
  templates.security.DummyAuthenticator.create // dummy
  templates.security.LdapUserAuthenticator.create // ldap
  ;
//------------------------------------------------------------------------------
// PEER EXTRA PROPERTIES
//
hydra.SecurityPrms-peerExtraProperties =
  none // noop
  cacheperf.comparisons.security.DummyUserPasswordPrms // dummy
  cacheperf.comparisons.security.LdapUserPasswordPrms // ldap
  ;

//------------------------------------------------------------------------------
// SSL SCHEME NAMES
//
hydra.SSLPrms-names = ssl;
hydra.SSLPrms-sslEnabled = true;
hydra.SSLPrms-sslProtocols = SSLv3;
hydra.SSLPrms-sslCiphers = SSL_RSA_WITH_RC4_128_MD5;
hydra.SSLPrms-sslRequireAuthentication = true;
hydra.SSLPrms-keyStore = $JTESTS/ssl/trusted.keystore;
hydra.SSLPrms-keyStorePassword = password;
// trustStore defaults to keyStore values
