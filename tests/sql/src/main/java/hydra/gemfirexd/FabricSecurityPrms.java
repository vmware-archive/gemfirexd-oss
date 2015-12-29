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

package hydra.gemfirexd;

import hydra.BasePrms;

/**
 * A class used to store keys for security configuration settings for fabric
 * servers.  The settings are used to create instances of {@link
 * FabricSecurityDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values and fields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class FabricSecurityPrms extends BasePrms {

  static {
    setValues(FabricSecurityPrms.class);
  }

  /**
   * (String(s))
   * Logical names of the fabric security descriptions.  Each name must be
   * unique.  Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;
  
  /**
   * (String(s))
   * The "auth-provider" property.  Valid values are
   * "builtin" and "ldap" and a custom class that implements {@link
   * com.pivotal.gemfirexd.auth.callback.UserAuthenticator}.  Can be specified
   * as {@link #NONE}.
   */
  public static Long authProvider;

  /**
   * (String(s))
   * The "gemfirexd.authz-default-connection-mode" property.  Valid
   * values are "noAccess", "readOnlyAccess", and "fullAccess".
   * Can be specified as {@link #NONE}.
   */
  public static Long authzDefaultConnectionMode;

  /**
   * (Comma-separated Lists of String(s))
   * The "gemfirexd.authz-full-access-users" property.
   * Can be specified as {@link #NONE} (default).
   */
  public static Long authzFullAccessUsers;

  /**
   * (Comma-separated Lists of String(s))
   * The "gemfirexd.authz-read-only-access-users" property.
   * Can be specified as {@link #NONE} (default).
   */
  public static Long authzReadOnlyAccessUsers;

  /**
   * (String(s))
   * The password portion of the "gemfirexd.user."<user>".<password>" property.
   * Can be specified as {@link #NONE} (default).
   */
  public static Long password;

  /**
   * (String(s))
   * The "server-auth-provider" property.  Valid values are
   * "builtin" and "ldap" and a custom class that implements {@link
   * com.pivotal.gemfirexd.auth.callback.UserAuthenticator}.  Can be specified
   * as {@link #NONE}.
   */
  public static Long serverAuthProvider;

  /**
   * (boolean(s))
   * The "gemfirexd.sql-authorization" property.  Defaults
   * to false.
   */
  public static Long sqlAuthorization;

  /**
   * (String(s))
   * The user portion of the "gemfirexd.user."<user>".<password>" property.
   * Can be specified as {@link #NONE} (default).
   */
  public static Long user;
}
