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

import java.net.*;
import java.util.*;

/**
 * Supports queries about hosts and their operating system types.
 * It also differentiates platforms where needed.
 */
public class HostHelper {

  /**
   * Enumerates the legal values for {@link hydra.HostPrms#osTypes}.  These
   * are the generic types representing more specific O/S types.
   */
  public static enum OSType {
    unix, windows;

    /**
     * Returns the OSType for this JVM, based on system property os.name.
     */
    protected static OSType toLocalOSType() {
      String os = System.getProperty("os.name");
      if (os == null) {
        String s = "System property os.name is missing in this JVM";
        throw new HydraRuntimeException(s);
      }
      return toOSType(os, null);
    }

    /**
     * Returns the OSType for the given O/S.
     */
    protected static OSType toOSType(String os) {
      return toOSType(os, null);
    }

    /**
     * Returns the OSType for the given O/S name which is assumed to have
     * been obtained using the given parameter key.
     */
    protected static OSType toOSType(String os, Long key) {
      String str = os;
      if (str == null) {
        String s = "";
        throw new IllegalArgumentException(s);
      }
      str = str.toLowerCase();
      if (str.contains("windows")) {
        return windows;
      } else if (str.contains("linux") || str.contains("unix") ||
                 str.contains("sunos") || str.contains("solaris") ||
                 str.contains("hpux")  || str.contains("hp-ux") ||
                 str.contains("aix")   || str.contains("darwin") ||
                 str.contains("mac")) {
        return unix;
      } else {
        String s;
        if (key == null) {
          s = "Illegal O/S name: " + os;
        } else {
          s = "Illegal value for " + BasePrms.nameForKey(key) + ": " + os;
        }
        throw new HydraConfigException(s);
      }
    }
  }

  /**
   * The <code>onlyOnPlatforms</code> system property
   * @see #shouldRun
   */
  public static final String ONLY_ON_PLATFORMS_PROP = "onlyOnPlatforms"; 

  private static String CanonicalHost;
  private static String Host;

//------------------------------------------------------------------------------
// host name
//------------------------------------------------------------------------------

  /**
   * Returns the non-canonical name of the local host, based on the IPv4
   * address.  For example, stut.
   *
   * @throws HydraRuntimeException if the IPv4 address is not found.
   */
  public static synchronized String getLocalHost() {
    if (Host == null) {
      String host = getIPv4Address().getHostName();
      int dot = host.indexOf(".");
      if (dot != -1) {
        String suffix = host.substring(dot + 1);
        if (!suffix.equals("local")) {
          host = host.substring(0, dot);
        }
        // else it is necessary to keep the dot in the hostname, since
        // it is of the form "hostname.local", as is found on Mac OS X
      }
      Host = host;
    }
    return Host;
  }

  /**
   * Returns the canonical name of the local host, based on the IPv4 address.
   * For example, stut.gemstone.com.
   *
   * @throws HydraRuntimeException if the IPv4 address is not found.
   */
  public static synchronized String getCanonicalHostName() {
    if (CanonicalHost == null) {
      CanonicalHost = getIPv4Address().getCanonicalHostName();
    }
    return CanonicalHost;
  }

  /**
   * Returns true if the noncanonical hostname of each host is the same or
   * if one contains the other.  For example, "stut.gemstone.com" will match
   * "stut6.gemstone.com";
   */
  protected static boolean compareHosts(String host1, String host2) {
    String tmp1 = getNoncanonicalHostName(host1).toLowerCase();
    String tmp2 = getNoncanonicalHostName(host2).toLowerCase();
    return tmp1.startsWith(tmp2) || tmp2.startsWith(tmp1);
  }

  /**
   * Returns the non-canonical host name of the given host.  For example,
   * "stut.gemstone.com" is returned as "stut".  No attempt is made to validate
   * that the host is real.
   *
   * @throws HydraRuntimeException if no host is specified.
   */
  protected static String getNoncanonicalHostName(String host) {
    if (host == null) {
      throw new IllegalArgumentException("host cannot be null");
    }
    int dot = host.indexOf(".");
    return (dot == -1) ? host : host.substring(0, dot);
  }

  /**
   * Returns the canonical name of the given host, based on the IPv4 address.
   *
   * @throws HydraRuntimeException if the host is not found.
   */
  public static String getCanonicalHostName(String host) {
    if (host == null) {
      throw new IllegalArgumentException("host cannot be null");
    }
    try {
      for(InetAddress addr : InetAddress.getAllByName(host)) {
        if (addr instanceof Inet4Address) {
          return addr.getCanonicalHostName();
        }
      }
    } catch (UnknownHostException e) {
      String s = "Host not found: " + host;
      throw new HydraRuntimeException(s, e);
    }
    String s = "IPv4 address not found for host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Answers whether the specified host is the local host.
   */
  public static boolean isLocalHost(String host) {
    if (host == null) {
      throw new IllegalArgumentException("host cannot be null");
    }
    return getCanonicalHostName(host).equals(getCanonicalHostName());
  }

//------------------------------------------------------------------------------
// host address
//------------------------------------------------------------------------------

  /**
   * Returns the address for the local host.  Distinguishes IPv4 and IPv6.
   * @throws HydraRuntimeException if an address of the appropriate type
   *                               cannot be found.
   */
  public static String getHostAddress() {
    return getHostAddress(getIPAddress());
  }

  /**
   * A wrapper for {@link InetAddress#getHostAddress} that removes the 
   * scope_id from all IPv4 address and all global IPv6 addresses. 
   * We don't need to use {@link #isIPv6LinkLocalAddress} because that 
   * is for the case where the scope_id is missing.
   * @param addr
   * @return the address with the scope_id stripped off  
   * (assuming the address would still be valid with it removed)
   */
  public static String getHostAddress(InetAddress addr) {
    String address = addr.getHostAddress();
    if (addr instanceof Inet4Address
        || (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()))
    {
      int idx = address.indexOf('%');
      if (idx >= 0) {
        address = address.substring(0, idx);
      }
    }
    return address;
  }

  /**
   * Returns the IP address for the local host.  Distinguishes IPv4 and IPv6.
   * @throws HydraRuntimeException if an address of the appropriate type
   *                               cannot be found.
   */
  public static InetAddress getIPAddress() {
    return Boolean.getBoolean(Prms.USE_IPV6_PROPERTY) ? getIPv6Address()
                                                      : getIPv4Address();
  }

  /**
   * Returns the IPv4 address for the local host.
   *
   * @throws HydraRuntimeException if the IPv4 address cannot be found.
   */
  protected static InetAddress getIPv4Address() {
    InetAddress host = null;
    try {
      host = InetAddress.getLocalHost();
      if (host instanceof Inet4Address) {
        // fix for ubuntu/debian setups where hostname points to 127.0.1.1
        if (host.isLoopbackAddress()
            && !"127.0.0.1".equals(host.getHostAddress())) {
          return InetAddress.getByName(null);
        }
        return host;
      }
    }
    catch (UnknownHostException e) {
      String s = "Local host not found";
      throw new HydraRuntimeException(s, e);
    }
    try {
      Enumeration i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface)i.nextElement();
        Enumeration j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress addr = (InetAddress)j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
                                         && (addr instanceof Inet4Address)) {
            return addr;
          }
        }
      }
      String s = "IPv4 address not found";
      throw new HydraRuntimeException(s);
    }
    catch (SocketException e) {
      String s = "Problem reading IPv4 address";
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Returns the IPv6 address for the local host.
   *
   * @throws HydraRuntimeException if the IPv6 address cannot be found.
   */
  public static InetAddress getIPv6Address() {
    /*
    InetAddress host = null;
    try {
      host = InetAddress.getLocalHost();
      if (host instanceof Inet6Address) {
        return host;
      }
    }
    catch (UnknownHostException e) {
      String s = "Local host not found";
      throw new HydraRuntimeException(s, e);
    }
    */
    try {
      Enumeration i = NetworkInterface.getNetworkInterfaces();
      while (i.hasMoreElements()) {
        NetworkInterface ni = (NetworkInterface)i.nextElement();
        Enumeration j = ni.getInetAddresses();
        while (j.hasMoreElements()) {
          InetAddress addr = (InetAddress)j.nextElement();
          // gemfire won't form connections using link-local addresses
          if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
               && (addr instanceof Inet6Address)
               && !isIPv6LinkLocalAddress((Inet6Address) addr)) {
            return addr;
          }
        }
      }
      String s = "IPv6 address not found";
      throw new HydraRuntimeException(s);
    }
    catch (SocketException e) {
      String s = "Problem reading IPv6 address";
      throw new HydraRuntimeException(s, e);
    }
  }

  /**
   * Detect LinkLocal IPv6 address where the interface is missing, ie %[0-9].
   * @see {@link InetAddress#isLinkLocalAddress()}
   */
  private static boolean isIPv6LinkLocalAddress(Inet6Address addr) {
    byte[] addrBytes = addr.getAddress();
    return ((addrBytes[0] == (byte) 0xfe) && (addrBytes[1] == (byte) 0x80));
  }

//------------------------------------------------------------------------------
// host o/s
//------------------------------------------------------------------------------

  /**
   * Returns true if the OSType for this JVM is "windows".
   */
  public static boolean isWindows() {
    return OSType.toLocalOSType() == OSType.windows;
  }

  /**
   * Returns the OSType of the local host O/S.
   */
  public static OSType getLocalHostOS() {
    return OSType.toLocalOSType();
  }

  /**
   * Returns the O/S type for the specified host.
   */
  public static OSType getHostOS(String host) {
    return TestConfig.getInstance().getAnyPhysicalHostDescription(host)
                     .getOSType();
  }

  /**
   * Returns whether or the given value of the
   * <code>onlyOnPlatforms</code> system property applies to this
   * host.  Note that this only applies to the host on which {@link
   * MasterController} runs.  Another mechanism must be used if you
   * want to restrict the platforms on which client VMs can run.
   *
   * @param onlyOnPlatforms
   *        A comma-separated string of platforms (as determined by
   *        the <code>os.name</code> Java {@linkplain
   *        java.lang.System.getProperty system property}) on which a test
   *        should be run.  Examples of platforms are
   *        <code>SunOS</code>, <code>Linux</code>, and
   *        <code>Windows</code>If <code>null</code> or an empty
   *        string (<code>""</code>), then the test should be run on
   *        <b>all</b> platforms.
   *
   * @since 3.5
   * */
  public static boolean shouldRun(String onlyOnPlatforms) {
    if (onlyOnPlatforms == null || "".equals(onlyOnPlatforms)) {
      return true;
    }

    StringTokenizer st = new StringTokenizer(onlyOnPlatforms, ",");
    String os = System.getProperty("os.name");
    while (st.hasMoreTokens()) {
      if (os.startsWith(st.nextToken())) {
        return true;
      }
    }

    return false;
  }

  /**
   * Process the String form of a hostname to make it comply with Jmx URL
   * restrictions. Namely wrap IPv6 literal address with "[", "]"
   * 
   * @param hostname
   *          the name to safeguard.
   * @return a string representation suitable for use in a Jmx connection URL
   */
  public static String applyRFC2732(String hostname) {
    if (hostname.indexOf(":") != -1) {
      // Assuming an IPv6 literal because of the ':'
      return "[" + hostname + "]";
    }
    return hostname;
  }
}
