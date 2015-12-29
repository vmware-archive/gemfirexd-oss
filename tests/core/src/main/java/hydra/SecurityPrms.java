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

package hydra;

/**
 * A class used to store keys for configuration settings related to
 * security.  The settings are used to create instances of {@link
 * SecurityDescription}, and can be referenced from a gemfire configuration
 * via {@link GemFirePrms#securityName}.
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
 * Values and fields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class SecurityPrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the security descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (String(s))
   * Method creating the client implementation of the interface {@link
   * com.gemstone.gemfire.security.AccessControl} for each security
   * configuration, passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_CLIENT_ACCESSOR_NAME}.  Can be specified as {@link #NONE}, which
   * uses the product default.
   */
  public static Long clientAccessor;

  /**
   * (String(s))
   * Method creating the client implementation of the interface {@link
   * com.gemstone.gemfire.security.AccessControl} for each security
   * configuration, passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_CLIENT_ACCESSOR_PP_NAME}.  Can be specified as {@link #NONE},
   * which uses the product default.
   */
  public static Long clientAccessorPP;

  /**
   * (String(s))
   * Method creating the client implementation of the interface {@link
   * com.gemstone.gemfire.security.AuthInitialize} for each security
   * configuration, passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_CLIENT_AUTH_INIT_NAME}.  Can be specified as {@link #NONE}, which
   * uses the product default.
   */
  public static Long clientAuthInit;

  /**
   * (String(s))
   * Method creating the client implementation of the interface {@link
   * com.gemstone.gemfire.security.Authenticator} for each security
   * configuration, passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_CLIENT_AUTHENTICATOR_NAME}.  Can be specified as {@link #NONE},
   * which uses the product default.
   */
  public static Long clientAuthenticator;

  /**
   * (String(s))
   * Class holding client extra <code>security-</code> properties for {@link
   * #clientAuthInit} for each security configuration.  Can be specified as
   * {@link #NONE} (default).
   * <p>
   * The extra properties are generated from all fields that are declared
   * <code>public static Long</code> in the specified class.  The required
   * <code>security-</code> prefix is automatically added by hydra and must
   * not be included in the field names.  Hydra changes upper case letters
   * to lower case preceded by a hyphen, to follow the GemFire property naming
   * convention.
   * <p>
   * Default values for a property can be specified as <code>public static
   * final String</code> for the field name (in upper case) prefixed with
   * <code>DEFAULT_</code>, with words separated by an underscore.
   * <p>
   * For example:
   *   <blockquote><pre>
   *     public static final String DEFAULT_USERNAME = "DefaultUser";
   *     public static Long username; // maps to "security-username"
   *     public static final String DEFAULT_PASSWORD_FILE = "$PWD/password.txt";
   *     public static Long passwordFile; // maps to "security-password-file"
   *   </pre></blockquote>
   * <p>
   * Parameter values are treated much like other non-hydra parameters.  They
   * are read by the hydra client VM rather than the master.  Use of oneof,
   * range, or robing will cause each VM to obtain a possibly different value.
   * Values can be overridden in task attributes.  Note also that a VM might
   * obtain a different value on restart.
   * <p>
   * If a parameter is left unset, the property value will default to the
   * default given in the parameter class, if any, else to the the empty string.
   * <p>
   * Extra properties and their values are accessible to test code via {@link
   * SecurityDescription#getClientExtraProperties}.
   */
  public static Long clientExtraProperties;

  /**
   * (String(s))
   * Log level for the security log file for each security configuration,
   * passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_LOG_LEVEL_NAME}.  Hydra automatically creates a security log
   * file with the name <code>security.log</code> in the system directory.
   */
  public static Long logLevel;

  /**
   * (String(s))
   * Method creating peer implementation of the interface {@link
   * com.gemstone.gemfire.security.AuthInitialize} for each security
   * configuration, passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_PEER_AUTH_INIT_NAME}.  Can be specified as {@link #NONE}, which
   * uses the product default.
   */
  public static Long peerAuthInit;

  /**
   * (String(s))
   * Method creating peer implementation of the interface {@link
   * com.gemstone.gemfire.security.Authenticator} for each security
   * configuration, passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_PEER_AUTHENTICATOR_NAME}.Can be specified as {@link #NONE}, which
   * uses the product default.
   */
  public static Long peerAuthenticator;

  /**
   * (String(s))
   * Class holding peer extra <code>security-</code> properties for {@link
   * #peerAuthInit} for each security configuration.  Can be specified as
   * {@link #NONE} (default).
   * <p>
   * The extra properties are generated from all fields that are declared
   * <code>public static Long</code> in the specified class.  The required
   * <code>security-</code> prefix is automatically added by hydra and must
   * not be included in the field names.  Hydra changes upper case letters
   * to lower case preceded by a hyphen, to follow the GemFire property naming
   * convention.
   * <p>
   * Default values for a property can be specified as <code>public static
   * final String</code> for the field name (in upper case) prefixed with
   * <code>DEFAULT_</code>, with words separated by an underscore.
   * <p>
   * For example:
   *   <blockquote><pre>
   *     public static final String DEFAULT_USERNAME = "DefaultUser";
   *     public static Long username; // maps to "security-username"
   *     public static final String DEFAULT_PASSWORD_FILE = "$PWD/password.txt";
   *     public static Long passwordFile; // maps to "security-password-file"
   *   </pre></blockquote>
   * <p>
   * Parameter values are treated much like other non-hydra parameters.  They
   * are read by the hydra client VM rather than the master.  Use of oneof,
   * range, or robing will cause each VM to obtain a possibly different value.
   * Values can be overridden in task attributes.  Note also that a VM might
   * obtain a different value on restart.
   * <p>
   * If a parameter is left unset, the property value will default to the
   * default given in the parameter class, if any, else to the the empty string.
   * <p>
   * Extra properties and their values are accessible to test code via {@link
   * SecurityDescription#getPeerExtraProperties}.
   */
  public static Long peerExtraProperties;

  /**
   * (int(s))
   * Peer verifymember timeout for each security configuration, in milliseconds,
   * passed in as the distributed system property {@link
   * com.gemstone.gemfire.distributed.internal.DistributionConfig#
   * SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME}.
   */
  public static Long peerVerifymemberTimeout;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(SecurityPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("securityprms", "info");
    dumpKeys();
  }
}
