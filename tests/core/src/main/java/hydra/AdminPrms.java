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
 * A class used to store keys for admin configuration settings.
 * The settings are used to create instances of {@link AdminDescription}.
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
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class AdminPrms extends BasePrms {

  static {
    setValues(AdminPrms.class);
  }

  /**
   * (String(s))
   * Logical names of the admin descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (int(s))
   * Ack severe alert threshold, in seconds.  Defaults to the value used by the
   * {@link #distributedSystem}.
   */
  public static Long ackSevereAlertThreshold;

  /**
   * (int(s))
   * Ack wait threshold, in seconds.  Defaults to the value used by the
   * {@link #distributedSystem}.
   */
  public static Long ackWaitThreshold;

  /**
   * (String(s))
   * Name of distributed system to administer, as found in {@link GemFirePrms
   * #distributedSystem}.  This is a required parameter with no default value.
   * Used to configure inherited values such as member discovery (locators or
   * multicast), use of TCP, and bind addresses.
   */
  public static Long distributedSystem;

  /**
   * (boolean(s))
   * Whether email notification is enabled.  If false, other email parameters,
   * if set to non-default values, are ignored by the product.
   * Note that email is only configured for JMX agents until Bug 39009 is fixed.
   */
  public static Long emailNotificationEnabled;

  /**
   * (String(s))
   * Email ID from which email notification is sent.  Defaults to no email
   * notification.
   * Note that email is only configured for JMX agents until Bug 39009 is fixed.
   */
  public static Long emailNotificationFrom;

  /**
   * (String(s))
   * Host from which email notification is sent.  Defaults to no email
   * notification.
   * Note that email is only configured for JMX agents until Bug 39009 is fixed.
   */
  public static Long emailNotificationHost;

  /**
   * (Comma-separated String(s))
   * Email IDs to which email notification is sent.  Defaults to no email
   * notification.  Cannot be used with oneof, range, or robing.
   * Note that email is only configured for JMX agents until Bug 39009 is fixed.
   */
  public static Long emailNotificationToList;

  /**
   * (boolean(s))
   * Whether to enable network partition detection.  Defaults to the value used
   * by the {@link #distributedSystem}.
   */
  public static Long enableNetworkPartitionDetection;

  /**
   * (boolean(s))
   * Whether to disable auto reconnect after a forced disconnect.  Defaults to the value used
   * by the {@link #distributedSystem}.
   */
  public static Long disableAutoReconnect;

  /**
   * (int(s))
   * Disk space limit for the log file, in megabytes.
   */
  public static Long logDiskSpaceLimit;

  /**
   * (int(s))
   * File size limit for the log file, in megabytes.
   */
  public static Long logFileSizeLimit;

  /**
   * (String(s))
   * Log level for the log file.
   */
  public static Long logLevel;

  /**
   * (int(s))
   * Member timeout, in milliseconds.  Defaults to the value used by the
   * {@link #distributedSystem}.
   */
  public static Long memberTimeout;

  /**
   * (int(s))
   * Refresh interval, in seconds.
   */
  public static Long refreshInterval;

  /**
   * (String(s))
   * Remote command for this admin distributed system.
   */
  public static Long remoteCommand;

  /**
   * (String(s))
   * Name of logical SSL configuration, as found in {@link SSLPrms#names}.
   * Can be specified as {@link #NONE} (default).
   */
  public static Long sslName;
}
