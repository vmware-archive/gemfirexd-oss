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
package hct.ha;

import hydra.Log;
import com.gemstone.gemfire.cache.EntryEvent;

/**
 * This class extends a <code>HANoFailoverClientListener</code>. The callback
 * methods are overwritten to validate the order of the data coming via the
 * events.
 * 
 * @author aingle
 * 
 */
public class HAMemEvictionListener extends HANoFailoverClientListener {

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterCreate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always does
   * create() on a key with a Long value. <br>
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterCreate(EntryEvent event) {
    Validator.createCount++;
    String key = (String)event.getKey();
    if (key.equals(Feeder.LAST_KEY)) {
      HAClientQueue.lastKeyReceived = true;
      Log.getLogWriter().info("'last_key' received at client");
    }
    if (event.getNewValue() == null) {
      throwException("newValue in afterUpdate cannot be null : key ="
          + event.getKey());
    }

    Log.getLogWriter().info("Received afterCreate for key " + event.getKey());
  }
  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterUpdate in Validator<br>
   * 2)Verify that value received in the event is not null as Feeder always
   * generates update on a key with a Long value one more than the previous one
   * on this key.<br>
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterUpdate(EntryEvent event) {
    Validator.updateCount++;
    if (event.getNewValue() == null) {
      throwException("newValue in afterUpdate cannot be null : key ="
        + event.getKey());
    }
    Log.getLogWriter().info("Received afterUpdate for key "+event.getKey());
  }

  /**
   * This method performs the following tasks:<br>
   * 1)Increment the count of afterInvalidate in Validator<br>
   * @param event -
   *          the entry event received in callback
   */
  public void afterInvalidate(EntryEvent event) {
    Validator.invalidateCount++;
    if (event.getNewValue() != null) {
      throwException("newValue in afterInvalidate should be null : key ="
          + event.getKey());
    }
    // Here is event old value is null implies that invalidate operation happened on
    // this event - durable clients
    Log.getLogWriter().info("Received afterInvalidate for key "+event.getKey());
  }

  /**
   * This method performs the following tasks:<br>
   * If the old value is null, it implies
   * that Feeder did a invalidate() in its previous operation on this key and
   * hence no need to update the local <code>latestValues</code> map.
   * 1)Increment the count of afterDestroy in Validator<br>
   * 
   * @param event -
   *          the entry event received in callback
   */
  public void afterDestroy(EntryEvent event) {
    Validator.destroyCount++;
    if (event.getNewValue() != null) {
      throwException("NewValue in afterDestroy should be null : key ="
          + event.getKey());
    }
    // value is null indicates invalidate operation performed on this event
    Log.getLogWriter().info("Received afterDestroy for key "+event.getKey());
  }
}
