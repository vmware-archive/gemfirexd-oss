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

package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.NoMemberFoundException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.membership.
    InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.NoDataStoreAvailableException;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.cache.execute.
    DistributedRegionFunctionExecutor;
import com.gemstone.gemfire.internal.cache.execute.InternalExecution;
import com.gemstone.gemfire.internal.cache.execute.MemberFunctionExecutor;
import com.gemstone.gemfire.internal.cache.execute.MemberMappedArgument;
import com.gemstone.gemfire.internal.cache.execute.MultiRegionFunctionExecutor;
import com.gemstone.gemfire.internal.cache.execute.
    PartitionedRegionFunctionExecutor;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * Utility methods for function execution on region, server groups or members.
 * These methods include failover with retries and flags set as appropriate to
 * deal with GemFireXD level failure detection.
 */
public final class FunctionUtils {

  private FunctionUtils() {
    // disallow construction
  }

  /**
   * Interface to obtain the set of members on which function execution has to
   * be done. This is used for retry logic in the HA related code in GemFireXD
   * since the set of members will likely change in case of retries.
   */
  public static interface GetFunctionMembers {

    /**
     * Get the set of members on which function execution has to be done. This
     * must return a set of members that is modifiable.
     */
    Set<DistributedMember> getMembers();

    /**
     * Get the set of server groups of the members, if any.
     */
    Set<String> getServerGroups();

    /**
     * Callback invoked where sanity checks related to the state of the system
     * can be done.
     */
    public void postExecutionCallback();
  }

  private static ResultCollector<?, ?> executeFunction(
      final GfxdExecution exec, final Object args,
      final String functionId, ResultCollector<?, ?> rc,
      final boolean enableStreaming, final boolean isPossibleDuplicate) {
    assert !enableStreaming || rc != null;

    final GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder
        .getInstance();
    if (observer != null) {
        observer.beforeQueryDistribution(null, enableStreaming);
    }
    // ignore the returned result collector and instead use own one for proper
    // streaming support when enabled
    boolean execSuccess = false;
    try {
      exec.setWaitOnExceptionFlag(true);
      if (isPossibleDuplicate) {
        exec.setIsReExecute();
      }
      rc = exec.execute(functionId);
      execSuccess = true;
    } finally {
      // in case of any exception during execute, clear the ResultCollector
      // to also invoke GfxdLockSet#rcEnd() if required (see bug #41661)
      if (!execSuccess) {
        rc.clearResults();
      }
      if (observer != null) {
        observer.afterQueryDistribution(null, enableStreaming);
      }
    }
    return rc;
  }

  public static GfxdExecution onRegion(Region<?, ?> region) {
    if (region == null) {
      throw new FunctionException(
          LocalizedStrings.FunctionService_0_PASSED_IS_NULL
              .toLocalizedString("Region instance "));
    }
    if (PartitionRegionHelper.isPartitionedRegion(region)) {
      return new GfxdPRFunctionExecutor(region);
    }
    return new GfxdDRFunctionExecutor(region);
  }

  public static GfxdExecution onRegions(
      @SuppressWarnings("rawtypes") Set<Region> regions) {
    if (regions == null) {
      throw new FunctionException(LocalizedStrings
          .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
              .toLocalizedString("regions set"));
    }
    if (regions.contains(null)) {
      throw new FunctionException(LocalizedStrings
          .OnRegionsFunctions_THE_REGION_SET_FOR_ONREGIONS_HAS_NULL
              .toLocalizedString());
    }
    if (regions.isEmpty()) {
      throw new FunctionException(LocalizedStrings
          .OnRegionsFunctions_THE_REGION_SET_IS_EMPTY_FOR_ONREGIONS
              .toLocalizedString());
    }
    return new GfxdMultiRegionFunctionExecutor(regions);
  }

  public static GfxdExecution onMembers(DistributedSystem dsys,
      GetFunctionMembers fMembers, boolean flushTXPendingOps) {
    if (dsys == null) {
      throw new FunctionException(LocalizedStrings
          .FunctionService_0_PASSED_IS_NULL
              .toLocalizedString("DistributedSystem instance "));
    }
    return new GetMembersFunctionExecutor(dsys, fMembers, flushTXPendingOps, false);
  }

  public static Object executeFunction(final GfxdExecution exec,
      Object args, String functionId, String logFunction, int retryCnt,
      final ResultCollector<?, ?> collector, boolean enableStreaming,
      boolean isPossibleDuplicate, boolean isHA, MemberMappedArgument mma,
      Set<Object> routingObjects, boolean requireRC, AbstractGemFireResultSet rs)
      throws StandardException {

    ResultCollector<?, ?> rc;
    if (collector != null) {
      exec.withCollector(collector);
    }
    if (mma != null) {
      // ignore args for this case
      exec.withMemberMappedArgument(mma);
    }
    else if (args != null) {
      exec.withArgs(args);
    }
    if (routingObjects != null) {
      exec.withRoutingObjects(routingObjects);
    }
    try {
      rc = executeFunction(exec, args, functionId, collector,
          enableStreaming, isPossibleDuplicate);
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TraceProcedureExecution) {
          // [sumedh] the GfxdExecution implementations always returns "this"
          // itself as result of "with*" methods unlike GFE executors, so it
          // is safe to use it to get the members used for execution
          logRoutingObjectsAndMembers(routingObjects, exec.getExecutionNodes(),
              functionId, logFunction);
        }
      }
      if (!enableStreaming) {
        final Object rcRes = rc.getResult();
        if (rs != null) {
          final Collection<InternalDistributedMember> recipients = exec
              .getExecutionNodes();
          rs.setup(rcRes, recipients != null ? recipients.size() : 0);
        }
        return requireRC ? rc : rcRes;
      }
      else {
        return rc;
      }
    } catch (RuntimeException e) {
      // first check this VM itself for shutdown
      Misc.getGemFireCache().getCancelCriterion().checkCancelInProgress(e);
      // check whether this query needs to be cancelled due to 
      // timeout or low memory
      if (rs != null) {
        rs.checkCancellationFlag();
      }
      // check if retry needs to be done
      int cnt = 0;
      RuntimeException retryEx = e;
      if (isHA && retryCnt > 0 && GemFireXDUtils.retryToBeDone(e, cnt)) {
        // what is supposed to be done here for MemberMappedArgument?
        // Reset member specific args?
        // [sumedh] nothing to be done; members that have seen the query string
        // need not be changed for retries
        while (cnt++ < retryCnt) {
          GemFireXDUtils.sleepForRetry(cnt);
          if (GemFireXDUtils.TraceFunctionException) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_FUNCTION_EX,
                "executeFunction: retry cnt: " + retryCnt + " retry is true",
                retryEx);
          }
          if (collector != null) {
            collector.clearResults();
          }
          try {
            rc = executeFunction(exec, args, functionId, collector,
                enableStreaming, true);
            if (SanityManager.DEBUG) {
              if (GemFireXDUtils.TraceProcedureExecution) {
                logRoutingObjectsAndMembers(routingObjects,
                    exec.getExecutionNodes(), functionId, logFunction);
              }
            }
            if (!enableStreaming) {
              final Object rcRes = rc.getResult();
              if (rs != null) {
                final Collection<InternalDistributedMember> recipients = exec
                    .getExecutionNodes();
                rs.setup(rcRes, recipients != null ? recipients.size() : 0);
              }
              return requireRC ? rc : rcRes;
            }
            else {
              return rc;
            }
          } catch (RuntimeException ex) {
            // first check this VM itself for shutdown
            Misc.getGemFireCache().getCancelCriterion()
                .checkCancelInProgress(ex);
            // check whether this query needs to be cancelled due to 
            // timeout or low memory
            if (rs != null) {
              rs.checkCancellationFlag();
            }
            if (!GemFireXDUtils.retryToBeDone(ex, cnt) || cnt >= retryCnt) {
              if (GemFireXDUtils.TraceFunctionException) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_FUNCTION_EX,
                    "executeFunction: retry cnt: " + retryCnt
                        + " retry is false", ex);
              }
              throw ex;
            }
            retryEx = ex;
          }
        }
        SanityManager.DEBUG_PRINT("info:" + GfxdConstants.TRACE_FUNCTION_EX,
            "executeFunction: retry cnt: " + retryCnt
                + " exhausted and throwing exception", e);
      }
      throw e;
    }
  }

  private static void logRoutingObjectsAndMembers(Set<Object> routingObjects,
      Collection<InternalDistributedMember> members, String functionId,
      String logFunction) {
    final StringBuilder sb = new StringBuilder();
    if (routingObjects != null) {
      sb.append("The routing objects are: ");
      for (Object routingObject : routingObjects) {
        sb.append(" ");
        sb.append(routingObject.toString());
      }
    }
    if (members == null) {
      sb.append(" No member is selected to execute the function ").append(
          functionId);
      if (logFunction != null && !logFunction.equals(functionId)) {
        sb.append('[').append(logFunction).append(']');
      }
    }
    else {
      sb.append("The members that executed function ").append(functionId);
      if (logFunction != null && !logFunction.equals(functionId)) {
        sb.append('[').append(logFunction).append(']');
      }
      sb.append(" are: ");
      for (InternalDistributedMember member : members) {
        sb.append(' ').append(member.getId());
      }
    }
    SanityManager
        .DEBUG_PRINT(GfxdConstants.TRACE_PROCEDURE_EXEC, sb.toString());
  }

  public static ResultCollector<?, ?> executeFunction(GfxdExecution exec,
      Object args, String functionId, String logFunction,
      ResultCollector<?, ?> rc, boolean enableStreaming,
      boolean isPossibleDuplicate, boolean isHA, MemberMappedArgument mma,
      Set<Object> routingObjects) throws StandardException {
    return (ResultCollector<?, ?>)executeFunction(exec, args, functionId,
        logFunction, GfxdConstants.HA_NUM_RETRIES, rc, enableStreaming,
        isPossibleDuplicate, isHA, mma, routingObjects, true, null);
  }

  public static Object executeFunctionOnRegionWithArgs(Region<?, ?> rgn,
      Object args, String functionId, ResultCollector<?, ?> rc,
      boolean enableStreaming, boolean isPossibleDuplicate, boolean isHA,
      MemberMappedArgument mma, Set<Object> routingObjects)
      throws StandardException {
    final GfxdExecution exec = onRegion(rgn);
    return executeFunction(exec, args, functionId, functionId,
        GfxdConstants.HA_NUM_RETRIES, rc, enableStreaming, isPossibleDuplicate,
        isHA, mma, routingObjects, false, null);
  }

  public static Collection<?> executeFunctionOnRegionWithArgs(Region<?, ?> rgn,
      Object args, String functionId, ResultCollector<?, ?> rc,
      boolean enableStreaming, boolean isPossibleDuplicate, boolean isHA,
      MemberMappedArgument mma, Set<Object> routingObjects,
      AbstractGemFireResultSet rs) throws StandardException {
    final GfxdExecution exec = onRegion(rgn);
    return (Collection<?>)executeFunction(exec, args, functionId, functionId,
        GfxdConstants.HA_NUM_RETRIES, rc, enableStreaming, isPossibleDuplicate,
        isHA, mma, routingObjects, false, rs);
  }

  public static Object executeFunctionOnRegionsWithArgs(
      @SuppressWarnings("rawtypes") Set<Region> regions, Object args,
      String functionId, ResultCollector<?, ?> rc, boolean enableStreaming,
      boolean isHA, MemberMappedArgument mma, Set<Object> routingObjects,
      boolean isPossibleDuplicate) throws StandardException {
    final GfxdExecution exec = onRegions(regions);
    return executeFunction(exec, args, functionId, functionId,
        GfxdConstants.HA_NUM_RETRIES, rc, enableStreaming, isPossibleDuplicate,
        isHA, mma, routingObjects, false, null);
  }

  public static Object executeFunctionOnMembers(DistributedSystem dsys,
      GetFunctionMembers fMembers, Object args, String functionId,
      ResultCollector<?, ?> collector, boolean enableStreaming,
      boolean isPossibleDuplicate, boolean isHA, boolean flushTXPendingOps)
      throws StandardException {
    final GfxdExecution exec = onMembers(dsys, fMembers, flushTXPendingOps);
    try {
      return executeFunction(exec, args, functionId, functionId,
          GfxdConstants.HA_NUM_RETRIES, collector, enableStreaming,
          isPossibleDuplicate, isHA, null, null, false, null);
    } catch (NoMemberFoundException ex) {
      // convert to standard exception
      final Set<String> serverGroups = fMembers.getServerGroups();
      throw StandardException.newException(
          SQLState.LANG_INVALID_MEMBER_REFERENCE, ex, serverGroups == null
              || serverGroups.size() == 0 ? "the distributed system"
              : "server groups '" + serverGroups + "'", "internal function '"
              + functionId + "'");
    } catch (NoDataStoreAvailableException ex) {
      // convert to standard exception
      final Set<String> serverGroups = fMembers.getServerGroups();
      throw StandardException.newException(SQLState.NO_DATASTORE_FOUND, ex,
          " executing function " + functionId + " with args: " + args
              + " on server groups " + serverGroups);
    }
  }

  /**
   * An intermediate interface for GemFireXD that adds methods specific to
   * GemFireXD to GFE {@link InternalExecution}. Implementations are also
   * expected to optimize the "with*" methods to return "this" itself instead of
   * making a new copy.
   * 
   * @author swale
   */
  public interface GfxdExecution extends InternalExecution {

    /**
     * Get the member IDs of nodes where the function was executed.
     */
    public Collection<InternalDistributedMember> getExecutionNodes();

    /**
     * Sets a listener to keep track of nodes where the function was executed.
     */
    public void setRequireExecutionNodes(
        AbstractExecution.ExecutionNodesListener listener);

    /**
     * Used to set any member specific arguments.
     * 
     * @see MemberMappedArgument
     */
    public GfxdExecution withMemberMappedArgument(MemberMappedArgument argument);

    /**
     * Specifies a data filter of routing objects for selecting the members to
     * execute the function that are not GemFire keys rather routing objects as
     * determined by resolver. Currently used by GemFireXD for passing routing
     * objects obtained from the custom resolvers.
     * <p>
     * If the set is empty the function is executed on all members that have the
     * region defined.
     * </p>
     * 
     * @param routingObjects
     *          Set defining the routing objects to be used for executing the
     *          function.
     * 
     * @return an Execution with the routing objects
     * 
     * @throws IllegalArgumentException
     *           if the set of routing objects passed is null.
     * @throws UnsupportedOperationException
     *           if not called after
     *           {@link FunctionService#onRegion(com.gemstone.gemfire.cache.Region)}
     */
    public GfxdExecution withRoutingObjects(Set<Object> routingObjects);

    /**
     * Set the flag that denotes that this execution is being retried.
     */
    public GfxdExecution setIsReExecute();

    /** base interface override to force return type to be GfxdExecution */
    public GfxdExecution withArgs(Object arguments);

    /** base interface override to force return type to be GfxdExecution */
    public GfxdExecution withCollector(ResultCollector<?, ?> rc);

    /** base interface override to force return type to be GfxdExecution */
    public GfxdExecution withFilter(Set<?> filter);
  }

  /**
   * A partitioned region executor extension to
   * {@link PartitionedRegionFunctionExecutor} for GemFireXD that implements
   * {@link GfxdExecution} interface.
   * 
   * Unlike the GFE <code>Execution<code>s, this implementation avoids creating
   * a new object for every with*() method invocation and returns "this" itself.
   * 
   * @author swale
   */
  @SuppressWarnings("unchecked")
  public static class GfxdPRFunctionExecutor extends
      PartitionedRegionFunctionExecutor implements GfxdExecution {

    public GfxdPRFunctionExecutor(Region<?, ?> region) {
      super(region);
      setWaitOnExceptionFlag(true);
    }

    // Not changing the object
    @Override
    public final GfxdPRFunctionExecutor withArgs(Object arguments) {
      if (arguments == null) {
        throw new FunctionException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("args"));
      }
      this.args = arguments;
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdPRFunctionExecutor withCollector(
        @SuppressWarnings("rawtypes") ResultCollector collector) {
      if (collector == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("Result Collector"));
      }
      this.rc = collector;
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdPRFunctionExecutor withFilter(
        @SuppressWarnings("rawtypes") Set filter) {
      if (filter == null) {
        throw new FunctionException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("filter"));
      }
      this.filter.clear();
      this.filter.addAll(filter);
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdPRFunctionExecutor withRoutingObjects(
        Set<Object> routingObjects) {
      super.withRoutingObjects(routingObjects);
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdPRFunctionExecutor withMemberMappedArgument(
        MemberMappedArgument argument) {
      if (argument == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("MemberMappedArgs"));
      }
      this.memberMappedArg = argument;
      this.isMemberMappedArgument = true;
      return this;
    }

    @Override
    public final GfxdPRFunctionExecutor setIsReExecute() {
      this.isReExecute = true;
      return this;
    }
  }

  /**
   * A distributed region executor extension to
   * {@link DistributedRegionFunctionExecutor} for GemFireXD that implements
   * {@link GfxdExecution} interface.
   * 
   * Unlike the GFE <code>Execution<code>s, this implementation avoids creating
   * a new object for every with*() method invocation and returns "this" itself.
   * 
   * @author swale
   */
  @SuppressWarnings("unchecked")
  public static class GfxdDRFunctionExecutor extends
      DistributedRegionFunctionExecutor implements GfxdExecution {

    public GfxdDRFunctionExecutor(Region<?, ?> region) {
      super(region);
      setWaitOnExceptionFlag(true);
    }

    // Not changing the object
    @Override
    public final GfxdDRFunctionExecutor withArgs(Object arguments) {
      if (arguments == null) {
        throw new FunctionException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("args"));
      }
      this.args = arguments;
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdDRFunctionExecutor withCollector(
        @SuppressWarnings("rawtypes") ResultCollector collector) {
      if (collector == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("Result Collector"));
      }
      this.rc = collector;
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdDRFunctionExecutor withFilter(
        @SuppressWarnings("rawtypes") Set filter) {
      if (filter == null) {
        throw new FunctionException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("filter"));
      }
      this.filter.clear();
      this.filter.addAll(filter);
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdDRFunctionExecutor withRoutingObjects(
        Set<Object> routingObjects) {
      super.withRoutingObjects(routingObjects);
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdDRFunctionExecutor withMemberMappedArgument(
        MemberMappedArgument argument) {
      if (argument == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("MemberMappedArgs"));
      }
      this.memberMappedArg = argument;
      this.isMemberMappedArgument = true;
      return this;
    }

    @Override
    public final GfxdDRFunctionExecutor setIsReExecute() {
      this.isReExecute = true;
      return this;
    }
  }

  /**
   * A member executor extension to {@link MemberFunctionExecutor} for GemFireXD
   * that uses the {@link GetFunctionMembers} interface to obtain the set of
   * members for initial message and failover.
   * 
   * Unlike the GFE <code>Execution<code>s, this implementation avoids creating
   * a new object for every with*() method invocation and returns "this" itself.
   * 
   * @author swale
   */
  @SuppressWarnings("unchecked")
  public static class GetMembersFunctionExecutor extends MemberFunctionExecutor
      implements GfxdExecution {

    protected final GetFunctionMembers getMembers;

    protected final boolean flushTXPendingOps;

    protected final boolean onlyLocal;

    public GetMembersFunctionExecutor(DistributedSystem dsys,
        GetFunctionMembers fMembers, boolean flushTXPendingOps,
        boolean onlyLocal) {
      super(dsys, (Set<?>)null);
      this.getMembers = fMembers;
      this.flushTXPendingOps = flushTXPendingOps;
      setWaitOnExceptionFlag(true);
      this.onlyLocal = onlyLocal;
    }

    @Override
    protected ResultCollector<?, ?> executeFunction(Function function) {
      final Set<DistributedMember> members;
      if (this.getMembers != null) {
        members = this.getMembers.getMembers();
      }
      else {
        // don't include locators, only datastores and peer clients
        if (!this.onlyLocal) {
          members = GemFireXDUtils.getGfxdAdvisor().adviseOperationNodes(null);
        }
        else {
          members = Collections.singleton((DistributedMember)this.ds
              .getDistributedMember());
        }
      }
      checkMembers(members, function);
      this.members = members;
      ResultCollector<?, ?> resultsCollector = super.executeFunction(function);
      if (this.getMembers != null) {
        this.getMembers.postExecutionCallback();
      }
      return resultsCollector;
    }

    protected void checkMembers(final Set<DistributedMember> members,
        final Function function) {
      if (members == null || members.isEmpty()) {
        throw new NoMemberFoundException(LocalizedStrings
            .MemberFunctionExecutor_NO_MEMBER_FOUND_FOR_EXECUTING_FUNCTION_0
                .toLocalizedString(function.getId() + " in server groups ["
                    + (this.getMembers != null ?
                        this.getMembers.getServerGroups() : "") + ']'));
      }
    }

    @Override
    protected final TXStateInterface flushTXPendingOps(final DM dm) {
      final TXStateInterface tx = TXManagerImpl.getCurrentTXState();
      if (tx != null && this.flushTXPendingOps) {
        // flush any pending TXStateProxy ops
        tx.flushPendingOps(dm);
      }
      return tx;
    }

    // Not changing the object
    @Override
    public final GetMembersFunctionExecutor withArgs(Object arguments) {
      if (arguments == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("args"));
      }
      this.args = arguments;
      return this;
    }

    // Not changing the object
    @Override
    public final GetMembersFunctionExecutor withCollector(
        @SuppressWarnings("rawtypes") ResultCollector collector) {
      if (collector == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("Result Collector"));
      }
      this.rc = collector;
      return this;
    }

    @Override
    public final GetMembersFunctionExecutor withFilter(
        @SuppressWarnings("rawtypes") Set filter) {
      throw new FunctionException(LocalizedStrings
          .ExecuteFunction_CANNOT_SPECIFY_0_FOR_DATA_INDEPENDENT_FUNCTIONS
              .toLocalizedString("filter"));
    }

    @Override
    public final GetMembersFunctionExecutor withRoutingObjects(
        Set<Object> routingObjects) {
      throw new FunctionException(LocalizedStrings
          .ExecuteFunction_CANNOT_SPECIFY_0_FOR_DATA_INDEPENDENT_FUNCTIONS
              .toLocalizedString("routing objects"));
    }

    // Not changing the object
    @Override
    public final GetMembersFunctionExecutor withMemberMappedArgument(
        MemberMappedArgument argument) {
      if (argument == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("MemberMappedArgs"));
      }
      this.memberMappedArg = argument;
      this.isMemberMappedArgument = true;
      return this;
    }

    @Override
    public final GetMembersFunctionExecutor setIsReExecute() {
      this.isReExecute = true;
      return this;
    }
  }

  /**
   * A distributed region executor extension to
   * {@link DistributedRegionFunctionExecutor} for GemFireXD that implements
   * {@link GfxdExecution} interface.
   * 
   * Unlike the GFE <code>Execution<code>s, this implementation avoids creating
   * a new object for every with*() method invocation and returns "this" itself.
   * 
   * @author swale
   */
  @SuppressWarnings("unchecked")
  public static class GfxdMultiRegionFunctionExecutor extends
      MultiRegionFunctionExecutor implements GfxdExecution {

    public GfxdMultiRegionFunctionExecutor(
        @SuppressWarnings("rawtypes") Set<Region> regions) {
      super(regions);
      setWaitOnExceptionFlag(true);
    }

    // Not changing the object
    @Override
    public final GfxdMultiRegionFunctionExecutor withArgs(
        Object arguments) {
      if (arguments == null) {
        throw new FunctionException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("args"));
      }
      this.args = arguments;
      return this;
    }

    // Not changing the object
    @Override
    public final GfxdMultiRegionFunctionExecutor withCollector(
        @SuppressWarnings("rawtypes") ResultCollector collector) {
      if (collector == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("Result Collector"));
      }
      this.rc = collector;
      return this;
    }

    @Override
    public GfxdMultiRegionFunctionExecutor withFilter(Set<?> filter) {
      throw new FunctionException(LocalizedStrings
          .ExecuteFunction_CANNOT_SPECIFY_0_FOR_ONREGIONS_FUNCTION
              .toLocalizedString("filter"));
    }

    @Override
    public GfxdMultiRegionFunctionExecutor withRoutingObjects(
        Set<Object> routingObjects) {
      throw new FunctionException(LocalizedStrings
          .ExecuteFunction_CANNOT_SPECIFY_0_FOR_ONREGIONS_FUNCTION
              .toLocalizedString("routing objects"));
    }

    // Not changing the object
    @Override
    public final GfxdMultiRegionFunctionExecutor withMemberMappedArgument(
        MemberMappedArgument argument) {
      if (argument == null) {
        throw new IllegalArgumentException(LocalizedStrings
            .ExecuteRegionFunction_THE_INPUT_0_FOR_THE_EXECUTE_FUNCTION_REQUEST_IS_NULL
                .toLocalizedString("MemberMappedArgs"));
      }
      this.memberMappedArg = argument;
      this.isMemberMappedArgument = true;
      return this;
    }

    @Override
    public final GfxdMultiRegionFunctionExecutor setIsReExecute() {
      this.isReExecute = true;
      return this;
    }
  }
}
