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
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Function used by the 'destroy disk-store' gfsh command to destroy a disk
 * store on each member.
 * 
 * @author David Hoots
 * @since 7.5
 */

public class DestroyDiskStoreFunction extends FunctionAdapter implements InternalEntity {
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    LogWriter logger = null;

    try {
      final Object[] args = (Object[]) context.getArguments();
      final String diskStoreName = (String) args[0];

      GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();

      logger = cache.getLogger();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      DiskStore diskStore = cache.findDiskStore(diskStoreName);

      CliFunctionResult result;
      if (diskStore != null) {
        diskStore.destroy();
        result = new CliFunctionResult(memberId, true, "Success");
      } else {
        result = new CliFunctionResult(memberId, false, "Disk store not found on this member");
      }
      context.getResultSender().lastResult(result);

    } catch (IllegalStateException isex) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, isex.getMessage());
      context.getResultSender().lastResult(result);
      
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      if (logger != null) {
        logger.error("Could not destroy disk store", th);
      }

      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public String getId() {
    return CreateDiskStoreFunction.class.getName();
  }
}
