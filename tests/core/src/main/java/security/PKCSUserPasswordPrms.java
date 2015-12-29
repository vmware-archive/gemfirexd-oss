/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package security;

import hydra.BasePrms;

/**
 * A class used to store pkcs username, password for hydra test settings.
 * 
 * @author Aneesh Karayil
 * @since 5.5
 */
public class PKCSUserPasswordPrms extends BasePrms {

  public static final String DEFAULT_AUTHZ_XML_URI = "$JTESTS/lib/authz-ldap.xml";

  public static final String DEFAULT_PUBLICKEY_FILE_PATH = "$JTESTS" +
    PKCSCredentialGenerator.keyStoreDir + "/publickeyfile";

  public static final String DEFAULT_KEYSTORE_FILE_PATH = "keystorepath";

  public static Long authzXmlUri;

  public static Long publickeyFilepath;

  public static Long keystorepath;

  public static Long alias;

  public static Long keystorepass;

  static {
    setValues(PKCSUserPasswordPrms.class);
  }

}
