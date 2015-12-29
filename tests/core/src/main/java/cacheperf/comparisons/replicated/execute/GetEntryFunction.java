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
package cacheperf.comparisons.replicated.execute;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.execute.*;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;
/**
 * A function to mimic the behavior of a simple get() on a PartitionedRegion.  Use of this function
 * assures that get() operations always occur on the Primary node for the given key, and addresses
 * two issues:
 * <p>
 * <li> Work-around for PR Expiration limitation, whereby get() operations that occur on a secondary copy
 * of an Entry do not reset the Expiration Timer TTL.
 * <p>
 * <li> Work-around for PR Expiration problem discovered during SWA Session Caching application testing.
 * The problem discovered does not happen when get operations always occur on the Primary node for
 * the target key.
 * <p>
 * To register this function declaratively in a GFE peer, server, or client process, add the following to the
 * cache.xml file:
 * <p>
    "
    <function-service>
	    <function>"com.swacorp.session.poc.gemfire.GetEntryFunction"</function>
    </function-service>
    "
 * <p>
 * On the Java client, the function should be invoked by the String methodId, rather than by passing-in an actual instance of the function
 * (this would cause the function itself to be serialized and sent to the invocation target, which is only useful for dynamically created
 * functions or functions not previously registered on the server).
 * <p<
 * @author Barry Oglesby (GSL Comments)
 *
 */

public class GetEntryFunction implements Function, Declarable {
  /**
   * The well-known String function identifier used to make invocations.
   */
  public static final String ID = "get-entry-function";  
  /**
   * Returns the Entry Value that corresponds with the given Key (as passed-in via the function Filter).
   * Note that passing in the key by this mechanism (rather than as a user argument to the function) lets
   * the "data-aware" function routing hint simultaneously act as BOTH a hint AND the function argument.
   */
  public void execute(FunctionContext context) {
    // Get the key argument (also used as the "filter" for data-aware function call routing . . .
    RegionFunctionContext rfc = (RegionFunctionContext) context;
    Region primaryEntries = PartitionRegionHelper.getLocalDataForContext(rfc);
    Set keys = rfc.getFilter();
    Serializable key = (Serializable) keys.iterator().next();
    Serializable value = RegionEntryHelper.getValue(primaryEntries, key);

    // Return the value with the ResultSender (lastResult() tells GFE function execution is finished).
    context.getResultSender().lastResult(value);
  }
  /**
   * Return the well-known String function identifier used to make invocations.
   */
  public String getId() {
    return ID;
  }
  /**
   * Returning TRUE instructs GemFire to always invoke the function on the node that owns the Primary copy of
   * the Entry Key filter passed into the function invocation.
   */
  public boolean optimizeForWrite() {
    return true;
  }
  /**
   * Enables automated re-tries if the invocation fails from the caller's perspective (for example, if the
   * remote server or a connection fails mid-execution).  This simulates the behavior of a generic "get()"
   * operation.
   */
  public boolean isHA() {
    return true;
  }
  /**
   * True as we always return the Entry Value (or null if not found).  FYI: If not TRUE, then the
   * invocation fires-and-forgets from the caller's perspective. This method is thus a way that GFE
   * optimizes processing for the calling thread.
   */
  public boolean hasResult() {
    return true;
  }
  /**
   * Required by Declarable Interface, but not used in this case as all resources, context, and inputs provided at invocation time.
   */
  public void init(Properties properties) {
  }
}

