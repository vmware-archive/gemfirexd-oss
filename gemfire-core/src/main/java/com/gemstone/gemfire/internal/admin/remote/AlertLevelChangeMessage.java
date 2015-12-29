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
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.admin.AlertLevel;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import java.io.*;

/**
 * A message that is sent to make members of the distributed system
 * aware that a manager agent wants alerts at a new level.
 *
 * @see AlertLevel
 *
 * @author David Whitlock
 * @since 3.5
 */
public final class AlertLevelChangeMessage
  extends PooledDistributionMessage  {

  /** The new alert level */
  private int newLevel;

  ///////////////////////  Static Methods  ///////////////////////

  /**
   * Creates a new <code>AlertLevelChangeMessage</code> 
   */
  public static AlertLevelChangeMessage create(int newLevel) {
    AlertLevelChangeMessage m = new AlertLevelChangeMessage();
    m.newLevel = newLevel;
    return m;
  }

  //////////////////////  Instance Methods  //////////////////////

  @Override
  public void process(DistributionManager dm) {
    //application vm
//    InternalDistributedSystem sys = dm.getSystem();
//    DistributionConfig config = sys.getConfig();
    ManagerLogWriter mlw = (ManagerLogWriter)dm.getLoggerI18n();
    mlw.removeAlertListener(this.getSender());

    if (this.newLevel != Alert.OFF) {  
      mlw.addAlertListener(this.getSender(), this.newLevel);
      if (DistributionManager.VERBOSE) {
        dm.getLoggerI18n().info(LocalizedStrings.AlertLevelChangeMessage_ADDED_NEW_ALERTLISTENER_TO_APP_LOG_WRITER);
      }
    }
  }

  public int getDSFID() {
    return ALERT_LEVEL_CHANGE_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.newLevel);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.newLevel = in.readInt();
  }

  @Override
  public String toString() {
    return LocalizedStrings.AlertLevelChangeMessage_CHANGING_ALERT_LEVEL_TO_0.toLocalizedString(AlertLevel.forSeverity(this.newLevel)); 
  }
}
