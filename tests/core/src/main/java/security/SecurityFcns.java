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

/**
 * Functions for use in test configuration files. All functions must return
 * Strings suitable for further parsing by hydra.
 */
public class SecurityFcns {

  /**
   * Returns the keystore with the given name.
   */
  public static String getKeystore(String name) {
    return "$JTESTS" + PKCSCredentialGenerator.keyStoreDir + '/' + name;
  }

  public static String getKeystoreForMultiuser(String name) {
    //return "$JTESTS" + PKCSCredentialGenerator.keyStoreDir + '/' + name;
    return PKCSCredentialGenerator.keyStoreDir + '/' + name;
  }
}
