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
package delta;

import com.gemstone.gemfire.cache.util.ObjectSizerImpl;

import hydra.Log;

import java.io.Serializable;

import util.TestException;
import util.BaseValueHolder;

/**
 * Used by eviction tests to cause serialized values to be a very different size
 * than deserialized. This sizer does not accurately reflect the size of the object
 * rather it just claims an object is a particular size so the test can ensure that
 * the product is using the sizer to determine sizes. 
 */
public class PretendSizer extends ObjectSizerImpl implements Serializable {

  public static long numberOfCalls = 0; // the number of sizeof calls in this vm

  public int sizeof( Object o ) {
    synchronized (PretendSizer.class) {
      numberOfCalls++;
      Log.getLogWriter().info("In PretendSizer, number of sizeof calls is now " + numberOfCalls);
    }
    return getSize(o);
  }

  /** Just return the myValue field of the expected ValueHolder argument. This
   *  is not really the object's size, but the product should use it as the
   *  size anyway.
   *  
   * @param o The object to return the pretend size for (must be an instance
   *          of util.ValueHolder.
   * @return The pretend size. 
   */
  public int getSize(Object o) {
    if (o instanceof BaseValueHolder) {
      BaseValueHolder vh = (BaseValueHolder)o;
      Log.getLogWriter().info("In PretendSizer: returning " + vh.myValue + " as the pretend size of this object");
      return (Integer)(vh.myValue);
    } else {
      throw new TestException("Expected " + o + " to be a ValueHolder");
    }
  }
  
}
