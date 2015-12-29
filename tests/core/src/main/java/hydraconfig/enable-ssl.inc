// turn on SSL and configure to use the checked in keystore...

hydra.GemFirePrms-sslName = globalSSL;

hydra.SSLPrms-names = globalSSL;
hydra.SSLPrms-sslEnabled=true;
hydra.SSLPrms-sslProtocols=SSLv3;
hydra.SSLPrms-sslCiphers=SSL_RSA_WITH_RC4_128_MD5;
hydra.SSLPrms-sslRequireAuthentication=true;

hydra.SSLPrms-keyStore=$JTESTS/ssl/trusted.keystore;
hydra.SSLPrms-keyStorePassword=password;

// trustStore defaults to keyStore values.

// set bridge max-threads to 0 since ssl does not support max-threads > 0
hydra.BridgePrms-maxThreads = 0;
