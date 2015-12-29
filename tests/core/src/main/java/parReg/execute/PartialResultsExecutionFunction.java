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

import java.io.Serializable;
import java.util.ArrayList;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;


public class PartialResultsExecutionFunction implements Function {

  public void execute(FunctionContext context) {
    long now = System.currentTimeMillis();
    if (now % 2 == 0) {
      for (int i = 0; i < 10; i++) {
        ArrayList list = new ArrayList();
        list.add(new Integer(i));
        context.getResultSender().sendResult(list);
      }
      ExecuteExceptionBB.getBB().getSharedCounters().increment(
          ExecuteExceptionBB.exceptionNodes);
      SenderIdWithOpId arg = (SenderIdWithOpId)context.getArguments();
      throwException(arg.getSenderId(), arg.getOpId());
    }
    else {
      hydra.Log.getLogWriter().info("Sending results");
      ExecuteExceptionBB.getBB().getSharedCounters().increment(
          ExecuteExceptionBB.sendResultsNodes);
      // Set keySet = localDataSet.keySet();
      ArrayList list = new ArrayList();
      list.add(InternalDistributedSystem.getAnyInstance().getMemberId());
      context.getResultSender().lastResult(list);
    }
  }

  public String getId() {
    return "PartialResultsExecutionFunction";
  }

  public boolean hasResult() {
    return true;
  }

  public boolean isHA() {
    return false;
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public Exception throwException(int senderId, int opId) {
    long now = System.currentTimeMillis();
    if (now % 3 == 0) {
      hydra.Log.getLogWriter().info(
          "Throwing fabricated Function exception [id=" + opId + "] sender="
              + senderId);
      throw new FunctionException("Fabricated Checked Exception [id=" + opId
          + "] myId: " + GemFireCacheImpl.getInstance().getMyId() + " sender="
          + senderId);
    }
    else if (now % 3 == 1) {
      hydra.Log.getLogWriter().info(
          "Throwing fabricated Runtime exception [id=" + opId + "] sender="
              + senderId);
      throw new RuntimeException("Fabricated Runtime Exception [id=" + opId
          + "] myId: " + GemFireCacheImpl.getInstance().getMyId() + " sender="
          + senderId);
    }
    else {
      hydra.Log.getLogWriter().info(
          "Throwing fabricated NullPointerException [id=" + opId + "] sender="
              + senderId);
      throw new NullPointerException("Fabricated NullPointerException [id="
          + opId + "] myId: " + GemFireCacheImpl.getInstance().getMyId()
          + " sender=" + senderId);
    }
  }

  public static final class SenderIdWithOpId implements Serializable {

    private static final long serialVersionUID = -6371596340222874760L;

    private int senderId;

    private int opId;

    /** for deserialization */
    public SenderIdWithOpId() {
    }

    public SenderIdWithOpId(int id, int opId) {
      this.senderId = id;
      this.opId = opId;
    }

    public final int getSenderId() {
      return this.senderId;
    }

    public final int getOpId() {
      return this.opId;
    }
  }
}
