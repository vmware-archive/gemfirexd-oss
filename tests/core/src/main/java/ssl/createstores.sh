#!/bin/bash

# Create keystore for system

keytool -genkey \
 -alias self \
 -keyalg rsa \
 -dname "CN=trusted" \
 -validity 3650 \
 -keypass password \
 -keystore ./trusted.keystore \
 -storepass password \
 -storetype JKS

# Create keystore for untrusted 

keytool -genkey \
 -alias self \
 -keyalg rsa \
 -dname "CN=Untrusted" \
 -validity 3650 \
 -keypass password \
 -keystore ./untrusted.keystore \
 -storepass password \
 -storetype JKS

exit 0

#### Help ###
# 
# keytool usage:
# 
# -certreq     [-v] [-alias <alias>] [-sigalg <sigalg>]
#              [-file <csr_file>] [-keypass <keypass>]
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# -delete      [-v] -alias <alias>
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# -export      [-v] [-rfc] [-alias <alias>] [-file <cert_file>]
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# -genkey      [-v] [-alias <alias>] [-keyalg <keyalg>]
#              [-keysize <keysize>] [-sigalg <sigalg>]
#              [-dname <dname>] [-validity <valDays>]
#              [-keypass <keypass>] [-keystore <keystore>]
#              [-storepass <storepass>] [-storetype <storetype>]
#              [-provider <provider_class_name>] ...
# 
# -help
# 
# -identitydb  [-v] [-file <idb_file>] [-keystore <keystore>]
#              [-storepass <storepass>] [-storetype <storetype>]
#              [-provider <provider_class_name>] ...
# 
# -import      [-v] [-noprompt] [-trustcacerts] [-alias <alias>]
#              [-file <cert_file>] [-keypass <keypass>]
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# -keyclone    [-v] [-alias <alias>] -dest <dest_alias>
#              [-keypass <keypass>] [-new <new_keypass>]
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# -keypasswd   [-v] [-alias <alias>]
#              [-keypass <old_keypass>] [-new <new_keypass>]
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# -list        [-v | -rfc] [-alias <alias>]
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# -printcert   [-v] [-file <cert_file>]
# 
# -selfcert    [-v] [-alias <alias>] [-sigalg <sigalg>]
#              [-dname <dname>] [-validity <valDays>]
#              [-keypass <keypass>] [-keystore <keystore>]
#              [-storepass <storepass>] [-storetype <storetype>]
#              [-provider <provider_class_name>] ...
# 
# -storepasswd [-v] [-new <new_storepass>]
#              [-keystore <keystore>] [-storepass <storepass>]
#              [-storetype <storetype>] [-provider <provider_class_name>] ...
# 
# 
