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
import java.rmi.Naming;
import java.util.Arrays;

/**
 * Client of the Bootstrapper used for multi-host and cross-platform Windows
 * testing.  The client is invoked in scripts such as nukerun.sh.
 */
public class BootstrapperClient {

  public static void main(String args[]) {
    if (args.length < 2) {
      err("Usage: hydra.BootstrapperClient bootstrapperURL command [args]");
    }
    String url = args[0];

    String[] cmd = new String[args.length-1];
    for (int i = 0; i < args.length-1; i++) {
      cmd[i] = args[i+1];
    }
    System.out.println("Executing " + Arrays.asList(cmd) + " at " + url);

    BootstrapperProxyIF bootstrapper = lookup(url);
    if (bootstrapper == null) {
      err("Unable to find bootstrapper at " + url);
    }
    String output = null;
    try {
      if (cmd.length == 1) {
        output = bootstrapper.fgexec(cmd[0], 120);
      } else {
        output = bootstrapper.fgexec(cmd, 120);
      }
      if (output != null && output.length() > 0) {
        System.out.println(output);
      }
    } catch (Throwable t) {
      if (output != null && output.length() > 0) {
        System.out.println(output);
      }
      err("Unable to run command.  See also bootstrapper log at " + url, t);
    }
    System.out.println("Executed " + Arrays.asList(cmd) + " at " + url);
  }

  private static BootstrapperProxyIF lookup(String url) {
    System.out.println("Looking up bootstrapper at " + url);
    try {
      return (BootstrapperProxyIF)Naming.lookup(url);
    } catch (Exception e) {
      return null;
    }
  }

  private static void err(String msg) {
    err(msg, null);
  }

  private static void err(String msg, Throwable t) {
    System.out.println(msg);
    if (t != null) {
      t.printStackTrace();
    }
    System.exit(1);
  }
}
