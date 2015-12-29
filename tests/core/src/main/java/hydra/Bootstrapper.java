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

import java.rmi.RemoteException;
import java.rmi.registry.Registry;

/**
 * Starts the RMI registry used for hydra cross-platform support.  Runs
 * in-process in the hydra {@link hydra.bootstrapper.Bootstrapper}.
 */
public class Bootstrapper {

  public static final String RMI_NAME = "bootstrapper";

  public static void main(String args[]) throws NumberFormatException {
    if (args.length != 1) {
      System.out.println( "Usage: hydra.Bootstrapper <port>" );
      System.exit(1);
    }
    int port = Integer.valueOf(args[0]);
    int pid = ProcessMgr.getProcessId();
    Log.createLogWriter("bootstrapper",
              "bootstrapper_" + port + "_" + pid, "all", false);
    bootstrap(port);
  }

  private static void bootstrap(int port) {
    Log.getLogWriter().info("Starting the bootstrapper.");
    Registry registry = RmiRegistryHelper.startRegistry(RMI_NAME, port);

    // create and register the bootstrapper proxy
    Log.getLogWriter().info("Creating the bootstrapper proxy");
    BootstrapperProxy bootstrapper;
    try {
      bootstrapper = new BootstrapperProxy();
    } catch (RemoteException e) {
      String s = "Bootstrapper proxy could not be created";
      throw new HydraRuntimeException(s, e);
    }
    Log.getLogWriter().info("Registering the bootstrapper proxy");
    RmiRegistryHelper.bind(registry, RMI_NAME, bootstrapper);
    Log.getLogWriter().info("Ready to roll");
  }
}
