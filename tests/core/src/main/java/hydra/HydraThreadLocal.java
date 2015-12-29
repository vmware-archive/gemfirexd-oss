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

import java.util.*;

/**
 * This class provides {@link java.lang.ThreadLocal} variables for hydra
 * threads.  A HydraThreadLocal is available to all threads hosted by the
 * current hydra thread, for example, RMI threads invoked to execute tasks.
 *
 * HydraThreadLocal objects are typically private static variables in
 * classes that wish to associate state with a hydra client.  For example,
 * they can be initialized in an INITTASK, used and modified in a TASK,
 * and validated in a CLOSETASK.
 */

public class HydraThreadLocal {

    /**
     * Creates a HydraThreadLocal variable.
     */
    public HydraThreadLocal() {
        key = this;
    }
    /**
     *  @see java.lang.ThreadLocal#initialValue
     */
    protected Object initialValue() {
        return null;
    }
    /**
     *  @see java.lang.ThreadLocal#get
     */
    public Object get() {
        RemoteTestModule mod = RemoteTestModule.getCurrentThread();
        Map map = mod.hydraThreadLocals;
        if (map == null)
          map = mod.hydraThreadLocals = new HashMap(INITIAL_CAPACITY);
        Object value = map.get(key);
        if (value==null && !map.containsKey(key)) {
            value = initialValue();
            map.put(key, value);
        }
        return value;
    }
    /**
     *  @see java.lang.ThreadLocal#set
     */
    public void set(Object value) {
        RemoteTestModule mod = RemoteTestModule.getCurrentThread();
        Map map = mod.hydraThreadLocals;
        if (map == null)
            map = mod.hydraThreadLocals = new HashMap(INITIAL_CAPACITY);
        map.put(key, value);
    }
    Object key;
    private static final int INITIAL_CAPACITY = 11;
}
