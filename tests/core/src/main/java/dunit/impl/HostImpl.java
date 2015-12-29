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
package dunit.impl;

import com.gemstone.gemfire.internal.Assert;
import dunit.*;
import hydra.*;
import java.rmi.*;
import java.util.*;

/**
 * This class provides the implementation of a distributed unit test
 * host.  More importantly, it initializes the representations of the
 * hosts and the GemFire systems and VMs running on those hosts.
 */
public class HostImpl extends Host {

  /** Has this class already been initialized? */
  private static boolean initialized = false;

  /** Maps the (machine) name of a host to its Host object */
  private static Map hosts;

  /**
   * Initializes information about the known hosts, GemFire systems,
   * and VMs.
   *
   * @throws IllegalStateException
   *         <code>initialized</code> has already been called
   * @throws RemoteException
   *         had problems accessing the hydra master controller vm
   *
   * @see hydra.UnitTestController#scheduleUnitTests()
   */
  public static void initialize() throws RemoteException {
    if (initialized) {
      String s = "Already been initialized";
      throw new IllegalStateException(s);

    } else {
      initialized = true;
    }

    hosts = new HashMap();

    TestConfig config = TestConfig.getInstance();

    String myLogicalHostName = RemoteTestModule.getMyLogicalHost();
    // Get the host information first.  This way the Hosts will be
    // order by their name (which is most likely the way that they
    // appear in the file).
    Iterator iter = config.getHostDescriptions().values().iterator();
    while (iter.hasNext()) {
      HostDescription hd = (HostDescription) iter.next();
      if ( myLogicalHostName.equals( hd.getName() ) ) {
        hostNamed(hd.getCanonicalHostName());
      }
    }

    // Get information about the gemfire systems
    iter = config.getGemFireDescriptions().values().iterator();
    while (iter.hasNext()) {
      GemFireDescription desc = (GemFireDescription) iter.next();
      HostDescription hd = desc.getHostDescription();
      if ( myLogicalHostName.equals( hd.getName() ) ) {
        HostImpl host = hostNamed(hd.getCanonicalHostName());
        host.addSystem(desc.getName(), desc.getSystemDirectory().getAbsolutePath());
      }
    }
    
    // Get the Hydra vm records and sort them by client name
    Map vms = RemoteTestModule.Master.getClientVms();
    Map vmsort = new TreeMap();
    iter = vms.values().iterator();
    while (iter.hasNext()) {
      ClientVmRecord cvmr = (ClientVmRecord) iter.next();
      String clientName = cvmr.getClientName();
      //Log.getLogWriter().info("DEBUG: clientName="+clientName + " vmr="+cvmr + " name=" + cvmr.getClientName());
      List vmsWithName = (List)vmsort.get(clientName);
      if (vmsWithName == null) {
        vmsWithName = new Vector();
        vmsort.put(clientName, vmsWithName);
      }
      vmsWithName.add(cvmr);
    }
    // Add the vms to their hosts
    iter = vmsort.values().iterator();
    while (iter.hasNext()) {
      List vmsWithName = (List)iter.next();
      for (Iterator it = vmsWithName.iterator(); it.hasNext();) {
        ClientVmRecord vm = (ClientVmRecord)it.next();
        String hostName = vm.getClientDescription().getVmDescription()
                            .getHostDescription().getCanonicalHostName().trim();
        String logicalHostName = vm.getClientDescription().getVmDescription()
                            .getHostDescription().getName().trim();
        //Only addVms belonging to the same logical host
        //allowing the calls to Host.getHost(0) to still function as expected.
        if ( logicalHostName.equals(myLogicalHostName)) {
          HostImpl host = hostNamed(hostName);
          int vmPid = vm.getPid();
          ClientRecord cr = vm.getRepresentativeClient();
          if (! cr.getThreadGroupName().startsWith( UnitTestController.NAME ))
          {
            String gemfireName = vm.getClientDescription()
                                   .getGemFireDescription().getName();
            if (vm.getClientName().startsWith("locator")) {
              host.addLocator(vmPid, gemfireName, cr.getTestModule());
            } else {
              host.addVM(vmPid, gemfireName, cr.getTestModule());
            }
          } 
        }
      }
    }

    for (int h = 0; h < getHostCount(); h++) {
      Host host = getHost(h);

      StringBuffer sb = new StringBuffer();
      sb.append("DUnit VMs on host ");
      sb.append(host.getHostName());
      sb.append("\n");
    
      int vmCount = host.getVMCount();
      for (int i = 0; i < vmCount; i++) {
        VM vm = host.getVM(i);
        sb.append("  VM ");
        sb.append(i);
        sb.append(" PID ");
        sb.append(vm.getPid());
        sb.append('\n');
      }

      Log.getLogWriter().info(sb.toString());
    }
  }
  
  /* Return a new or previously encountered HostImpl
     for the given name */
  private static HostImpl hostNamed(String hostName) {
    HostImpl host = (HostImpl) hosts.get(hostName);
    if (host == null) {
      host = new HostImpl(hostName);
      hosts.put(hostName, host);
      Host.addHost(host);
    }
    return host;
  }

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>HostImpl</code> with the given name
   */
  HostImpl(String name) {
    super(name);
  }

}
