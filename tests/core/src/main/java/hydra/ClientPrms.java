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
 *
 * A class used to store keys for client configuration settings.  The settings
 * are used to create instances of {@link ClientDescription}.
 * <p>
 * The number of instances is gated by {@link #names}.  The remaining parameters
 * have the indicated default values.  If fewer non-default values than names
 * are given for these parameters, the remaining instances will use the last
 * value in the list.  See $JTESTS/hydra/hydra.txt for more details.
 *
 */

public class ClientPrms extends BasePrms {

    public static final String CLIENT_NAME_PROPERTY = "clientName";

    /**
     *  (String(s))
     *  Logical names of the client descriptions.  Each name must be unique.
     *  Defaults to null.  See the include files in $JTESTS/hydraconfig for
     *  common configurations.
     */
    public static Long names;

    /**
     *  (String(s))
     *  Logical names of the VM descriptions to use to create client VMs.
     *  Defaults to null, which is a configuration error if any {@link #names}
     *  are defined.  See the include files in $JTESTS/hydraconfig for sample
     *  configurations.
     *
     *  @see VmPrms#names
     *  @see VmDescription
     */
    public static Long vmNames;

    /**
     *  (String(s))
     *  Logical names of the GemFire systems used by client VMs.  Defaults to
     *  {@link #NONE}.  Can be specified as {@link #NONE} when not using
     *  GemFire or when using it administratively through {@link AdminPrms}.
     *  See the include files in $JTESTS/hydraconfig for sample configurations.
     *
     *  @see GemFirePrms#names
     *  @see GemFireDescription
     */
    public static Long gemfireNames;

    /**
     *  (String(s))
     *  Logical names of the JProbe configurations used by client VMs.
     *  Defaults to null.
     *
     *  @see JProbePrms#names
     *  @see JProbeDescription
     */
    public static Long jprobeNames;

  /**
   * (String(s))
   * Name of logical JDK version configuration to use in each VM, as found in
   * {@link JDKVersionPrms#names}. Can be specified as {@link #NONE} (default),
   * which does not use JDK versioning. Ignored for STARTTASK and ENDTASK VMs.
   *
   * @see JDKVersionPrms
   */
  public static Long jdkVersionNames;

  /**
   * (String(s))
   * Name of logical version configuration to use in each VM, as found in
   * {@link VersionPrms#names}.  Can be specified as {@link #NONE} (default),
   * which does not use versioning.  Ignored for STARTTASK and ENDTASK VMs.
   *
   * @see VersionPrms
   */
  public static Long versionNames;

    /**
     *  (int(s))
     *  Number of client VMs to create.  Defaults to 1.
     */
    public static Long vmQuantities;

    /**
     *  (int(s))
     *  Number of client threads to create in each VM.  Defaults to 1.
     */
    public static Long vmThreads;

    static {
        setValues( ClientPrms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "clientprms", "info" );
        dumpKeys();
    }
}
