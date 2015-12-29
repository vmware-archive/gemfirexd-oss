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
package com.gemstone.gemfire.management.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.AlreadyRunningException;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfile;

public class JmxManagerLocator implements TcpHandler {
  private GemFireCacheImpl cache;

  public JmxManagerLocator(GemFireCacheImpl gfc) {
    this.cache = gfc;
  }

  @Override
  public Object processRequest(Object request) throws IOException {
    assert request instanceof JmxManagerLocatorRequest;
    return findJmxManager((JmxManagerLocatorRequest)request);
  }

  @Override
  public void endRequest(Object request, long startTime) {
    // nothing needed
  }

  @Override
  public void endResponse(Object request, long startTime) {
    // nothing needed
  }

  @Override
  public void shutDown() {
    // nothing needed
  }

  public void restarting(DistributedSystem ds, GemFireCache cache) {
    this.cache = (GemFireCacheImpl)cache;
  }

  @Override
  public void init(TcpServer tcpServer) {
    // nothing needed
  }

  private JmxManagerLocatorResponse findJmxManager(JmxManagerLocatorRequest request) {
    JmxManagerLocatorResponse result = null;
    this.cache.getLogger().fine("Locator requested to find or start jmx manager");
    List<JmxManagerProfile> alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
    if (alreadyManaging.isEmpty()) {
      List<JmxManagerProfile> willingToManage = this.cache.getJmxManagerAdvisor().adviseWillingToManage();
      if (!willingToManage.isEmpty()) {
        synchronized (this) {
          alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
          if (alreadyManaging.isEmpty()) {
            willingToManage = this.cache.getJmxManagerAdvisor().adviseWillingToManage();
            if (!willingToManage.isEmpty()) {
              JmxManagerProfile p = willingToManage.get(0);
              if (p.getDistributedMember().equals(this.cache.getMyId())) {
                this.cache.getLogger().fine("Locator starting jmx manager in its JVM");
                try {
                  ManagementService.getManagementService(this.cache).startManager();
                } catch (CancelException ex) {
                  // ignore
                } catch (Throwable t) {
                  Error err;
                  if (t instanceof Error
                      && SystemFailure.isJVMFailureError(err = (Error)t)) {
                    SystemFailure.initiateFailure(err);
                    // If this ever returns, rethrow the error. We're poisoned
                    // now, so don't let this thread continue.
                    throw err;
                  }
                  // Whenever you catch Error or Throwable, you must also
                  // check for fatal JVM error (see above). However, there is
                  // _still_ a possibility that you are dealing with a cascading
                  // error condition, so you also need to check to see if the
                  // JVM is still usable:
                  SystemFailure.checkFailure();
                  return new JmxManagerLocatorResponse(null, 0, false, t);
                }
              } else {
                p = startJmxManager(willingToManage);
                if (p != null) {
                  this.cache.getLogger().fine("Locator started jmx manager in " + p.getDistributedMember());
                }
                // bug 46041 is caused by a race in which the function reply comes
                // before we have received the profile update. So pause for a bit
                // if our advisor still does not know about a manager and the member
                // we asked to start one is still in the ds.
                alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
                int sleepCount = 0;
                while (sleepCount < 20 && alreadyManaging.isEmpty() && this.cache.getDistributionManager().getDistributionManagerIds().contains(p.getDistributedMember())) {
                  sleepCount++;
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
                }
              }
              if (alreadyManaging.isEmpty()) {
                alreadyManaging = this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
              }
            }
          }
        } // sync
      }
    }
    if (!alreadyManaging.isEmpty()) {
      JmxManagerProfile p = alreadyManaging.get(0);
      result = new JmxManagerLocatorResponse(p.getHost(), p.getPort(), p.getSsl(), null);
      this.cache.getLogger().fine("Found jmx manager: " + p);
    } else {
      this.cache.getLogger().fine("Did not find a jmx manager");
      result = new JmxManagerLocatorResponse();
    }
    return result;
  }

  private JmxManagerProfile startJmxManager(List<JmxManagerProfile> willingToManage) {
    for (JmxManagerProfile p: willingToManage) {
      if (sendStartJmxManager(p.getDistributedMember())) {
        return p;
      }
    }
    return null;
  }

  private boolean sendStartJmxManager(InternalDistributedMember distributedMember) {
    try {
    ArrayList<Object> resultContainer = (ArrayList<Object>)FunctionService.onMember(distributedMember).execute(new StartJmxManagerFunction()).getResult();
    Object result = resultContainer.get(0);
    if (result instanceof Boolean) {
      return ((Boolean)result).booleanValue();
    } else {
      this.cache.getLogger().info("Could not start jmx manager on " + distributedMember + " because " + result);
      return false;
    }
    } catch (RuntimeException ex) {
      if (!this.cache.getDistributionManager().getDistributionManagerIdsIncludingAdmin().contains(distributedMember)) {
        // if the member went away then just return false
        this.cache.getLogger().info("Could not start jmx manager on " + distributedMember + " because " + ex);
        return false;
      } else {
        throw ex;
      }
    }
  }
  
  public static class StartJmxManagerFunction implements Function, InternalEntity {
    private static final long serialVersionUID = -2860286061903069789L;
    public static final String ID = StartJmxManagerFunction.class.getName();

    @Override
    public void execute(FunctionContext context) {
      
      try {
        Cache cache = CacheFactory.getAnyInstance();
        if (cache != null) {
          ManagementService ms = ManagementService.getExistingManagementService(cache);
          if (ms != null) {
            if (!ms.isManager()) { // see bug 45922
              ms.startManager();
            }
            context.getResultSender().lastResult(Boolean.TRUE);
          }
        }
        context.getResultSender().lastResult(Boolean.FALSE);
      } catch (AlreadyRunningException ok) {
        context.getResultSender().lastResult(Boolean.TRUE);
      }catch (Exception e) {
        context.getResultSender().lastResult(
            "Exception in StartJmxManager =" + e.getMessage());
      }
    }

    @Override
    public String getId() {
      return StartJmxManagerFunction.ID;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      // no need of optimization since read-only.
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }

  }
}
