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
package cacheLoader.smoke;

import hydra.blackboard.*;

/**
 * A hydra <code>Blockboard</code> used for the
 * <code>cacheLoader</code> tests.
 */
public class BB extends Blackboard {
   
    public static int NUM_READ_CALLS;
    public static int NUM_TIMED_READ_CALLS;
    public static int TIME_READ_CALLS;
    public static int NUM_INVALIDATE_UPDATE_CALLS;
    public static int NUM_TIMED_PUT_CALLS;
    public static int TIME_PUT_CALLS;
    public static int NUM_LOAD_CALLS;
    public static int NUM_INVALIDATE_CALLBACKS;
    public static int NUM_UNLOAD_CALLBACKS;
    public static int NUM_DESTROY_CALLBACKS;

    public static int NUM_UPDATE_CALLS;
    public static int NUM_ADD_CALLBACKS;
    public static int NUM_PUT_CALLBACKS;
    public static int NUM_REPLACE_CALLBACKS;

    private static String BLACKBOARD_NAME = "CacheLoader_blackboard";
    private static String BLACKBOARD_TYPE = "RMI";

    private static BB blackboard;
   
    /**
     *  Zero-arg constructor for remote method invocations.
     */
    public BB() {
    }
   
    /**
     *  Creates a blackboard using the specified name and transport type.
     */
    public BB(String name, String type) {
	super(name, type, BB.class );
    }

  /**
   * Invoked by subclasses to initialize themselves
   */
  protected BB(String name, String type, Class bb) {
    super(name, type, bb);
  }

    /**
     *  If blackboard already exists, return it.
     *  Else, create one.
     */
    public static Blackboard getInstance() {
    if (blackboard == null)
      synchronized(BB.class) {
        if (blackboard == null)
	    blackboard = new BB(BLACKBOARD_NAME, BLACKBOARD_TYPE);
      }
    return blackboard;
    }

}
