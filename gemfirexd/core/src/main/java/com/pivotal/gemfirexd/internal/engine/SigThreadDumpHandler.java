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

package com.pivotal.gemfirexd.internal.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.internal.process.signal.Signal;
import com.pivotal.gemfirexd.Property;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

import sun.misc.SignalHandler;

/**
 * Signal handler for UNIX like systems to dump locks and thread stacks on
 * receiving SIGURG signal. Now also logs any other unexpected OS signals.
 * 
 * @author swale
 */
public class SigThreadDumpHandler implements SignalHandler {

  private static final SigThreadDumpHandler instance =
      new SigThreadDumpHandler();

  private final Map<Signal, SignalHandler> oldHandlers;

  private SigThreadDumpHandler() {
    this.oldHandlers = Collections
        .synchronizedMap(new HashMap<Signal, SignalHandler>());
    // set of signals to be ignored because they are not fatal or informational
    List<String> ignoredSignals = new ArrayList<String>() {{
      add("WINCH");
      add("CHLD");
      add("CONT");
      add("CLD");
      add("BUS");  // Specifically for Mac OS X
      add("USR1");
      add("USR2");
    }};
    mergeIgnoredSignalsFromProps(ignoredSignals);

    if (!ignoredSignals.contains("*")) {
      for (final Signal signal : Signal.values) {
        if (ignoredSignals.contains(signal.getName())) {
          continue;
        }
        try {
          sun.misc.Signal osSignal = new sun.misc.Signal(signal.getName());
          this.oldHandlers.put(signal, sun.misc.Signal.handle(osSignal, this));
        } catch (IllegalArgumentException iae) {
          // move on to next one
        }
      }
    }
  }

  private List<String> mergeIgnoredSignalsFromProps(List<String> ignored) {
    String sigsIgnored = PropertyUtil.getSystemProperty(Property.SIGNALS_NOT_LOGGED);
    if (sigsIgnored != null) {
      for (String s : sigsIgnored.split(",")) {
        s = s.trim().toUpperCase();
        if (s.equals("*")) {
          ignored.clear();
          ignored.add("*");
          break;
        }
        ignored.add(s);
      }
    }
    return ignored;
  }

  public static SigThreadDumpHandler install() {
    return instance;
  }

  /**
   * Handle the caught signal dumping all locks and threads.
   */
  public void handle(sun.misc.Signal sig) {
    try {
      final GemFireStore memStore = GemFireStore.getBootingInstance();
      final Signal signal = Signal.valueOfName(sig.getName());
      if (Signal.SIGURG.equals(signal)) {
        final String header = "SIGURG received, full state dump";
        GemFireXDUtils.dumpStacks(memStore, header);
      }
      else {
        // just log any other unexpected signals
        SanityManager.DEBUG_PRINT("warning:SignalHandler",
            "received explicit OS signal SIG" + sig.getName(), new Throwable());
      }

      // Chain back to previous handler, if one exists
      SignalHandler oldHandler = this.oldHandlers.get(signal);
      if (oldHandler != null && oldHandler != SIG_DFL
          && oldHandler != SIG_IGN) {
        oldHandler.handle(sig);
      }
    } catch (Throwable t) {
      System.err.println("SigThreadDumpHandler failed to dump to log for SIG"
          + sig.getName() + ": " + SanityManager.getStackTrace(t));
    }
  }
}
