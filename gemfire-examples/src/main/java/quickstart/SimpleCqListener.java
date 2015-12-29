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
package quickstart;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqListener;

/**
 * A simple CqListener implementation.
 * 
 * @author GemStone Systems, Inc.
 */
public class SimpleCqListener implements CqListener {

  StringBuffer eventLog = new StringBuffer();

  private String userName = "";

  public SimpleCqListener() {
  }

  public SimpleCqListener(String userName) {
    if (userName != null && userName.length() > 0) {
      this.userName = "[" + userName + "]";
    }
  }

  @Override
  public void onEvent(CqEvent cqEvent) {
    Operation baseOperation = cqEvent.getBaseOperation();
    Operation queryOperation = cqEvent.getQueryOperation();

    String baseOp = "";
    String queryOp = "";

    if (baseOperation.isUpdate()) {
      baseOp = " Update";
    } else if (baseOperation.isCreate()) {
      baseOp = " Create";
    } else if (baseOperation.isDestroy()) {
      baseOp = " Destroy";
    } else if (baseOperation.isInvalidate()) {
      baseOp = " Invalidate";
    }

    if (queryOperation.isUpdate()) {
      queryOp = " Update";
    } else if (queryOperation.isCreate()) {
      queryOp = " Create";
    } else if (queryOperation.isDestroy()) {
      queryOp = " Destroy";
    }

    eventLog = new StringBuffer();
    eventLog.append("\n    " + this.userName
        + " CqListener:\n    Received cq event for entry: " + cqEvent.getKey()
        + ", " + ((cqEvent.getNewValue()) != null ? cqEvent.getNewValue() : "")
        + "\n" + "    With BaseOperation =" + baseOp + " and QueryOperation ="
        + queryOp + "\n");
    System.out.print(eventLog.toString());

  }

  @Override
  public void onError(CqEvent cqEvent) {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }

  public void printEventLog() {
    System.out.println(eventLog.toString());
  }
}
