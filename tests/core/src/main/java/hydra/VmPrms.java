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

import java.util.*;

/**
 *
 * A class used to store keys for vm configuration settings.  The settings
 * are used to create instances of {@link VmDescription}.
 * <p>
 * The number of instances is gated by {@link #names}.  The remaining parameters
 * have the indicated default values.  If fewer non-default values than names
 * are given for these parameters, the remaining instances will use the last
 * value in the list.  See $JTESTS/hydra/hydra.txt for more details.
 *
 */

public class VmPrms extends BasePrms {

    /**
     *  (String(s))
     *  Logical names of the VM descriptions.  Each name must be unique.
     *  Defaults to null.  See the include files in $JTESTS/hydraconfig for
     *  common configurations.
     */
    public static Long names;

    /**
     *  (String(s))
     *  Logical names of the host descriptions to use to create VMs.
     *  Defaults to null, which is a configuration error if any {@link #names}
     *  are defined.  See the include files in $JTESTS/hydraconfig for sample
     *  configurations.
     *
     *  @see HostPrms#names
     *  @see HostDescription
     */
    public static Long hostNames;

    /**
     *  (String(s))
     *  Type of VM to use, "client" or "server" or "d64".  Defaults to "server".
     */
    public static Long type;
    public static final String CLIENT = "client";
    public static final String SERVER = "server";
    public static final String SERVER64 = "d64";
    public static String getType( int i, String vendor ) {
      if ( vendor.equals( HostPrms.SUN ) || vendor.equals( HostPrms.HITACHI )) {
        Long key = type;
        String val = tab().stringAtWild( key, i, SERVER );
        if ( val.equalsIgnoreCase( CLIENT ) ) {
          return CLIENT;
        } else if ( val.equalsIgnoreCase( SERVER ) ) {
          return SERVER;
        } else if ( val.equalsIgnoreCase( SERVER64 ) ) {
          return SERVER64;
        } else {
          throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
        }
      } else if ( vendor.equals( HostPrms.JROCKIT ) ) {
          Long key = type;
          String val = tab().stringAtWild( key, i, SERVER );
          if ( val.equalsIgnoreCase( CLIENT ) ) {
            return CLIENT;
          } else if ( val.equalsIgnoreCase( SERVER ) ) {
            return SERVER;
          } else if ( val.equalsIgnoreCase( SERVER64 ) ) {
            return SERVER64;
          } else {
            throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
          }
      } else {
        return null;
      }
    }
    /** For internal use only. */
    public static String defaultType() {
      String vendor = HostPrms.getJavaVendor();
      if (vendor.equals(HostPrms.SUN)) {
        return SERVER;
      } else {
        return null;
      }
    }

    /**
     *  (String(s) or List of String(s))
     *  Extra classpaths.  Default is the corresponding entry in
     *  {@link hydra.HostPrms#testDirs} for the target host O/S, and
     *  lib/gemfire.jar from the corresponding entry in
     *  {@link hydra.HostPrms#gemfireHomes}, if it is defined.  The extra
     *  classpaths, if any, are prepended to the defaults.  All occurrences of
     *  $JTESTS and $GEMFIRE in a path are expanded relative to -DJTESTS and
     *  -Dgemfire.home on the target host.
     *  Can be specified as {@link #NONE}.
     */
    public static Long extraClassPaths;

    /**
     *  (String(s) or List of String(s))
     *  Extra library paths.  Default is lib from the corresponding entry
     *  in {@link HostPrms#gemfireHomes}, if it is defined, otherwise
     *  it is null.  The extra library paths, if any, are prepended to the
     *  defaults.  All occurrences of $JTESTS and $GEMFIRE in a path are expanded
     *  relative to -DJTESTS and -Dgemfire.home on the target host.
     */
    public static Long extraLibPaths;

    /**
     * Comma-separated list(s) of String(s))
     * Additional Java command line options and system properties to use,
     * together with generic and vendor-specific defaults added by hydra,
     * when starting VMs from this description.
     * <p>
     * Can be specified as {@link BasePrms#NONE}.  Defaults to null.
     * Arguments can be added to an existing configuration using <code>+=<code>.
     * Double quotes must be used for arguments containing <code>=</code>.
     * <p>
     * For example, to start VMs of type <code>vm1</code> with 512m max heap
     * and system property <code>value</code> set to <code>42<code>, and VMs
     * of type <code>vm2</code> with 1024m max heap:
     * <code>
     *   hydra.VmPrms-names = vm1 vm2;
     *   hydra.VmPrms-extraVMArgs = -Xmx512m "-Dvalue=42", -Xmx1024m;
     * Note the comma separating arguments for the different logical VM types.
     * </code>
     * <p>
     * This parameter should be used only for arguments supported by all
     * GemFire-supported VM types.  See {@link #extraVMArgsHitachi}, {@link
     * #extraVMArgsIBM}, {@link #extraVMArgsJRockit}, {@link #extraVMArgsApple}, 
     * and {@link
     * #extraVMArgsSUN} for configuring additional vendor-specific arguments.
     * <p>
     * See {@link hydra.HostPrms#javaVendors} for how to configure the VM
     * vendor.  See {@link #type} for how to configure the VM type.
     */
    public static Long extraVMArgs;

    /**
     * (String(s) or List of String(s))
     * Hitachi-specific JVM command-line options.  Added only to hydra clients
     * whose {@link HostPrms#javaVendors} are {@link HostPrms#HITACHI}.
     * Can be specified as {@link #NONE}.  Defaults to null.
     */
    public static Long extraVMArgsHitachi;

    /**
     * (String(s) or List of String(s))
     * IBM-specific JVM command-line options.  Added only to hydra clients
     * whose {@link HostPrms#javaVendors} are {@link HostPrms#IBM}.
     * Can be specified as {@link #NONE}.  Defaults to null.
     */
    public static Long extraVMArgsIBM;

    /**
     * (String(s) or List of String(s))
     * JRockit-specific JVM command-line options.  Added only to hydra clients
     * whose {@link HostPrms#javaVendors} are {@link HostPrms#JROCKIT}.
     * Can be specified as {@link #NONE}.  Defaults to null.
     */
    public static Long extraVMArgsJRockit;

    /**
     * (String(s) or List of String(s))
     * Apple-specific JVM command-line options.  Added only to hydra clients
     * whose {@link HostPrms#javaVendors} are {@link HostPrms#APPLE}.
     * Can be specified as {@link #NONE}.  Defaults to null.
     */
    public static Long extraVMArgsApple;

    /**
     * (String(s) or List of String(s))
     * SUN-specific JVM command-line options.  Added only to hydra clients
     * whose {@link HostPrms#javaVendors} are {@link HostPrms#SUN}.
     * Can be specified as {@link #NONE}.  Defaults to null.
     */
    public static Long extraVMArgsSUN;

    // hydra-added defaults
    public static final String DEFAULT_ARGS =
      "-Dsun.rmi.dgc.client.gcInterval=600000 -Dsun.rmi.dgc.server.gcInterval=600000 -Dsun.rmi.transport.tcp.handshakeTimeout=3600000 -Xmx250m -Dgemfire.disallowMcastDefaults=true";
    public static final String DEFAULT_SUN_4_ARGS =
      "-XX:+JavaMonitorsInStackTrace";
    public static final String DEFAULT_SUN_5_ARGS =
      "-XX:+JavaMonitorsInStackTrace -XX:+HeapDumpOnOutOfMemoryError";
    public static final String DEFAULT_HITACHI_ARGS =
      "-XX:+JavaMonitorsInStackTrace -XX:+HitachiOutOfMemoryStackTrace -XX:+HitachiOutOfMemoryCause -XX:+HitachiOutOfMemorySize";

    /**
     * Returns the VM arguments for the logical VM at the given index
     * with the specified vendor and type, consisting of:
     * 1) hydra-added generic defaults, then
     * 2) hydra-added vendor-specific defaults, then
     * 3) user-specified generic values from {@link #extraVMArgs}, then
     * 4) user-specified vendor-specific values from vendor-specific variants
     *    of <code>extraVMArgs</code>, then
     * 5) if {Prms#useIPv6} is true, turn on {@link Prms#USE_IPV6_PROPERTY}
     * 6) if {Prms#disableCreateBucketRandomness} is true, turn on {@link
     *    {Prms#DISABLE_CREATE_BUCKET_RANDOMNESS_PROPERTY}
     */
    public static String getExtraVMArgs( int i, String vendor, String type ) {

      // 1) hydra-added generic defaults
      String args = DEFAULT_ARGS;

      // 2) hydra-added vendor-specific defaults
      if ( vendor.equals( HostPrms.SUN ) ) {
        if (System.getProperty("java.version").startsWith("1.4")) {
          args += " " + DEFAULT_SUN_4_ARGS;
        } else {
          args += " " + DEFAULT_SUN_5_ARGS;
        }
      }
      if ( vendor.equals( HostPrms.HITACHI ) ) {
          args += " " + DEFAULT_HITACHI_ARGS;
      }

      // 3) user-specified generic values from extraVMArgs
      args += " " + getVMArgs(extraVMArgs, i);

      // 4) user-specified vendor-specific values from extraVMArgs for vendors.
      if (vendor.equals(HostPrms.HITACHI)) {
        args += " " + getVMArgs(extraVMArgsHitachi, i);
      } else if (vendor.equals(HostPrms.IBM)) {
        args += " " + getVMArgs(extraVMArgsIBM, i);
      } else if (vendor.equals(HostPrms.JROCKIT)) {
        args += " " + getVMArgs(extraVMArgsJRockit, i);
      } else if (vendor.equals(HostPrms.SUN)) {
        args += " " + getVMArgs(extraVMArgsSUN, i);
      } else if (vendor.equals(HostPrms.APPLE)) {
        args += " " + getVMArgs(extraVMArgsApple, i);
      } else {
        String s = "Unknown JVM vendor: " + vendor;
        throw new HydraInternalException(s);
      }

      // 5) use IPv6 addresses if asked
      if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
        if (args == null) args = "";
        args += " -D" + Prms.USE_IPV6_PROPERTY + "=true";
      }

      // 6) disable create bucket randomness if asked
      if (TestConfig.tab().booleanAt(Prms.disableCreateBucketRandomness, false)) {
        if (args == null) args = "";
        args += " -D" + Prms.DISABLE_CREATE_BUCKET_RANDOMNESS_PROPERTY + "=true";
      }

      return args;
    }

    private static String getVMArgs(Long key, int i) {
      String args = "";
      Vector val = tab().vecAtWild(key, i, null);
      if ( val != null ) {
        for ( Iterator it = val.iterator(); it.hasNext(); ) {
          String arg = (String) it.next();
          if ( arg.equals( "=" ) ) {
            String s = "Malformed value in " + key
                     + ", use quotes to include '='";
            throw new HydraConfigException(s);
          } else {
            if (!val.equals(BasePrms.NONE)) {
              args += " " + arg;
            }
          }
        }
      }
      return args;
    }

    static {
        setValues( VmPrms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "vmprms", "info" );
        dumpKeys();
    }
}
