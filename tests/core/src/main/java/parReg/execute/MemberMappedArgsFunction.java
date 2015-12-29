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
package parReg.execute;

import java.util.ArrayList;

import util.TestException;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public class MemberMappedArgsFunction extends FunctionAdapter {

  public void execute(FunctionContext context) {
    ArrayList list = new ArrayList();

    // Executed with member args
    if (context.getArguments() instanceof InternalDistributedMember) {
      DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
      DistributedMember localVM = ds.getDistributedMember();
      if (context.getArguments().equals(localVM)) {
        hydra.Log.getLogWriter().info(
            "Executed on node " + localVM + "with args "
                + context.getArguments());
        list.add(Boolean.TRUE);
      }
      else {
        throw new TestException(
            "This function was supposed to be executed in the node "
                + context.getArguments() + " but got executed on node "
                + localVM);
      }
    }
    else {// executed with default args
      hydra.Log.getLogWriter().info(
          "Executed on node with args " + context.getArguments());
      list.add(Boolean.FALSE);
    }

    context.getResultSender().lastResult(list);
  }

  public String getId() {
    return this.getClass().getName();
  }
  
  public boolean isHA() {
    return true;
  }

}
