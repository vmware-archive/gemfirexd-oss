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
package management.operations.ops;

import static management.util.HydraUtil.logInfo;
import hydra.HydraVector;
import hydra.Log;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import management.cli.CLIBlackboard;
import management.operations.OperationPrms;
import management.operations.OperationsBlackboard;
import management.operations.events.FunctionOperationEvents;
import management.util.HydraUtil;
import management.util.ManagementUtil;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * This class provides following services * registering of various functions
 * (controlled by Prms) * execution of functions * listing of functions *
 * execute of random function (fire-and-forget)
 * 
 * Types of functions : fireAndForget sleep regionFunction
 * queryPartitionFunction
 * 
 * Configuration
 * 
 * 
 * TODO : Add recorder : DONE support onMember. currently dictated by region
 * RegionOPS = Add recorder and return value of region operation
 * 
 * @author tushark
 * 
 */
@SuppressWarnings("rawtypes")
public class FunctionOperations {

  public final static int FUNCTION_OP_REGISTER_NEW_FUNCTION = 0;
  public final static int FUNCTION_OP_UNREGISTER_FUNCTION = 2;
  public final static int FUNCTION_OP_EXECUTE_REGISTERED_FUNCTION = 3;

  public static final String FUNCTION_OP_REGISTER = "register";
  public static final String FUNCTION_OP_UNREGISTER = "unregister";
  public static final String FUNCTION_OP_EXEC = "exec";

  public static final String FUNCTION_TYPE_SLEEP = "sleepFunction";
  public static final String FUNCTION_TYPE_REGION = "regionFunction";
  public static final String FUNCTION_TYPE_FIRENFORGET = "fireNforget";

  private FunctionOperationEvents opRecorder = null;
  private Region region = null;
  private Cache cache = null;

  public void setCache(Cache cache) {
    this.cache = cache;
  }

  protected static String opPrefix = "FunctionOperations: ";

  public FunctionOperations() {
    this.opRecorder = OperationsBlackboard.getBB();
  }

  public FunctionOperations(Region r) {
    this.opRecorder = OperationsBlackboard.getBB();
    this.region = r;
  }

  public FunctionOperations(FunctionOperationEvents recorder) {
    this.opRecorder = recorder;
  }

  public FunctionOperations(Region r, FunctionOperationEvents recorder) {
    this.opRecorder = recorder;
    this.region = r;
  }
  
  public void registerFunction(){
    HydraVector vector = TestConfig.tab().vecAt(OperationPrms.functionRegisterList);
    int functionNum = TestConfig.tab().getRandGen().nextInt(vector.size()-1);
    int index=0;
    String kind = null;
    Iterator iterator = vector.iterator();
    while (iterator.hasNext()) {
      String k = (String) iterator.next();
      if(index==functionNum){
        kind = k; 
      }
      index++;
    }
    if(kind!=null)
      _registerFunction(kind);
    else throw new TestException("Unkown kind " + kind);
  }
  
  public void registerFunction(String type){
    _registerFunction(type);
  }

  private void _registerFunction(String givenKind) {
    String functionRegistered = null;
    if (FUNCTION_TYPE_SLEEP.equals(givenKind)) {
      long delay = TestConfig
          .tab()
          .getRandGen()
          .nextLong(
              TestConfig.tab().intAt(OperationPrms.functionSleepDelay, 10000));
      String id = OperationsBlackboard.getBB().getNextSleepFunctionId();
      SleepFunction func = new SleepFunction(delay, id);
      FunctionService.registerFunction(func);
      functionRegistered = id;
    } else if (FUNCTION_TYPE_REGION.equals(givenKind)){
      RegionFunctionOp rFunc = new RegionFunctionOp();
      if (!FunctionService.isRegistered(rFunc.getId())) {
        FunctionService.registerFunction(rFunc);
        functionRegistered = rFunc.getId();
      }
    } else {
      GenericFunctionOp rFunc = new GenericFunctionOp(givenKind);
      if (!FunctionService.isRegistered(rFunc.getId())) {
        FunctionService.registerFunction(rFunc);
        functionRegistered = rFunc.getId();
      }
    }
    opRecorder.functionRegistered(functionRegistered);
  }

  public void doFunctionOperation() {
    String opStr = TestConfig.tab().stringAt(OperationPrms.functionOps);

    int op = -1;

    if (FUNCTION_OP_EXEC.equals(opStr))
      op = FUNCTION_OP_EXECUTE_REGISTERED_FUNCTION;
    else if (FUNCTION_OP_REGISTER.equals(opStr))
      op = FUNCTION_OP_REGISTER_NEW_FUNCTION;
    else if (FUNCTION_OP_UNREGISTER.equals(opStr))
      op = FUNCTION_OP_UNREGISTER_FUNCTION;

    switch (op) {
    case FUNCTION_OP_EXECUTE_REGISTERED_FUNCTION:
      executeFunction();
      break;
    case FUNCTION_OP_REGISTER_NEW_FUNCTION:
      /*List<String> registerFunctionList = registerFunction();
      for (String id : registerFunctionList)
        opRecorder.functionRegistered(id);*/      
      registerFunction();
      break;
    case FUNCTION_OP_UNREGISTER_FUNCTION:
      unRegisterFunction();
      break;
    default:
      logInfo(opPrefix + "Unknown operation code " + op);
      break;
    }
  }

  /*public static void HydraInitTask_registerFunctions() {
    registerFunctions();
  }*/

  private void unRegisterFunction() {
    int currentRegisteredFunctions = OperationsBlackboard.getBB()
        .getSleepFunctionCounter();
    int randomFunction = TestConfig.tab().getRandGen()
        .nextInt(currentRegisteredFunctions);
    FunctionService.unregisterFunction("SLEEP_" + randomFunction);
    opRecorder.functionUnregistered("SLEEP_" + randomFunction);
  }

  public List<String> registerFunctions() {
    HydraVector vector = TestConfig.tab().vecAt(
        OperationPrms.functionRegisterList);
    List<String> functionList = new ArrayList<String>();
    Iterator iterator = vector.iterator();
    while (iterator.hasNext()) {
      String str = (String) iterator.next();
      if (FUNCTION_TYPE_SLEEP.equals(str)) {
        long delay = TestConfig
            .tab()
            .getRandGen()
            .nextLong(
                TestConfig.tab().intAt(OperationPrms.functionSleepDelay, 10000));
        String id = OperationsBlackboard.getBB().getNextSleepFunctionId();
        SleepFunction func = new SleepFunction(delay, id);
        FunctionService.registerFunction(func);
        functionList.add(id);
      } else if (FUNCTION_TYPE_REGION.equals(str)) {
        RegionFunctionOp rFunc = new RegionFunctionOp();
        if (!FunctionService.isRegistered(rFunc.getId())) {
          FunctionService.registerFunction(rFunc);
          functionList.add(rFunc.getId());
        }
      } else {
        GenericFunctionOp rFunc = new GenericFunctionOp(str);
        if (!FunctionService.isRegistered(rFunc.getId())) {
          FunctionService.registerFunction(rFunc);
          functionList.add(rFunc.getId());
        }
      }
    }
    
    for(String id : functionList){
      opRecorder.functionRegistered(id);
    }
    
    return functionList;
  } 
  

  private void executeFunction() {
    String function = getFunction();
    if (FUNCTION_TYPE_SLEEP.equals(function)) {
      execSleep();
    } else if (FUNCTION_TYPE_REGION.equals(function)) {
      execRegion();
    } else if (FUNCTION_TYPE_FIRENFORGET.equals(function)) {
      execFireNForget();
    }
  }

  private Execution getExecutionObject() {
    DistributedSystem ds = cache.getDistributedSystem();
    InternalDistributedMember localVM = ((InternalDistributedSystem) ds)
        .getDistributionManager().getDistributionManagerId();
    if (region == null)
      return FunctionService.onMembers(ds);
    else
      return FunctionService.onRegion(region);
    // TODO Add more types!!!
  }

  private void execFireNForget() {
    Execution dataSet;
    FireAndForgetFunction ffFunction = null;
    List argumentList = new ArrayList();

    dataSet = getExecutionObject();
    argumentList.add(RemoteTestModule.getCurrentThread().getThreadId());
    // Dont care about filter
    try {
      logInfo(opPrefix + "Starting execution of ffFunction "
          + ffFunction.getId());
      ResultCollector drc = dataSet.withArgs(argumentList).execute(
          new FunctionAdapter() {
            @Override
            public String getId() {
              return "" + hashCode();
            }

            @Override
            public void execute(FunctionContext context) {
              Log.getLogWriter().info(
                  opPrefix + "Invoking fire and forget onMembers() function");
              ArrayList list = new ArrayList();
              DistributionConfig dc = ((InternalDistributedSystem) InternalDistributedSystem
                  .getAnyInstance()).getConfig();
              list.add(dc.getCacheXmlFile());
              Log.getLogWriter().info(opPrefix + "Updating the BB list");
            }
          });
      opRecorder.functionExecuted("FIRENFORGET", null);
      logInfo(opPrefix + "Finished execution of ffFunction "
          + ffFunction.getId());
    } catch (Exception e) {
      logInfo(opPrefix + "Error running function " + e.getMessage());
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private void execRegion() {
    Execution dataSet;
    RegionFunctionOp regionFunction = new RegionFunctionOp();
    List argumentList = new ArrayList();

    dataSet = getExecutionObject();
    argumentList.add(RemoteTestModule.getCurrentThread().getThreadId());
    // Dont care about filter
    try {
      logInfo(opPrefix + "Starting execution of regionFunction "
          + regionFunction.getId());
      ResultCollector drc = dataSet.withArgs(argumentList).execute(
          regionFunction.getId());
      logInfo(opPrefix + "Finished execution of regionFunction "
          + regionFunction.getId());
      Object result = drc.getResult();
      opRecorder.functionExecuted(regionFunction.getId(), result);
    } catch (Exception e) {
      logInfo("Error running function " + e.getMessage());
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private void execSleep() {
    Execution dataSet;
    SleepFunction sleepFunction = null;
    List argumentList = new ArrayList();

    boolean flag = false;
    while (!flag) {
      int currentRegisteredFunctions = OperationsBlackboard.getBB()
          .getSleepFunctionCounter();
      int randomFunction = TestConfig.tab().getRandGen()
          .nextInt(currentRegisteredFunctions);
      if (FunctionService.isRegistered("SLEEP_" + randomFunction)) {
        flag = true;
        sleepFunction = (SleepFunction) FunctionService.getFunction("SLEEP_"
            + randomFunction);
      }
    }

    dataSet = getExecutionObject();
    argumentList.add(RemoteTestModule.getCurrentThread().getThreadId());
    // Dont care about filter
    try {
      logInfo(opPrefix + "Starting execution of sleepFunction "
          + sleepFunction.getId());
      ResultCollector drc = dataSet.withArgs(argumentList).execute(
          sleepFunction.getId());
      logInfo(opPrefix + "Finished execution of sleepFunction "
          + sleepFunction.getId());
      Object result = drc.getResult();
      opRecorder.functionExecuted(sleepFunction.getId(), result);
    } catch (Exception e) {
      logInfo("Error running function " + e.getMessage());
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private String getFunction() {
    return TestConfig.tab().stringAt(OperationPrms.functionList);
  }

  public static class FireAndForgetFunction extends FunctionAdapter {

    private String functionId = null;
    private Callable runnable = null;

    public FireAndForgetFunction(String id, Callable r) {
      this.functionId = id;
      this.runnable = r;
    }

    @Override
    public void execute(FunctionContext context) {
      logInfo("Executing sleep function " + functionId);

      Object returnValue = null;
      try {
        returnValue = runnable.call();
        context.getResultSender().lastResult(returnValue);
      } catch (Exception e) {
        context.getResultSender().sendException(e);
      }

      logInfo("Completed Function Execution" + functionId);
    }

    @Override
    public String getId() {
      return functionId;
    }

  }

  public static class SleepFunction extends FunctionAdapter {

    private long delay = 0;
    private String functionId = null;

    public SleepFunction(long delay, String id) {
      this.delay = delay;
      this.functionId = id;
    }

    @Override
    public void execute(FunctionContext context) {
      // TODO Auto-generated method stub
      logInfo("Executing sleep function " + functionId + " sleeping for "
          + delay);

      long l1 = System.currentTimeMillis();
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        // ignore
      }

      long l2 = System.currentTimeMillis();

      logInfo("Completed Function Execution" + functionId + " in " + (l2 - l1)
          + " ms");

      context.getResultSender().lastResult((l2 - l1));
    }

    @Override
    public String getId() {
      return functionId;
    }

  }

  public static class RegionFunctionOp extends FunctionAdapter {

    @Override
    public void execute(FunctionContext context) {
      ArrayList arguments = (ArrayList) (context.getArguments());
      // String operation = (String)arguments.get(0);
      String operation = "functionExecOp";
      Object initiatingThreadID = arguments.get(1);
      String aStr = opPrefix + "In execute with context " + context
          + " with operation " + operation + " initiated in hydra thread thr_"
          + initiatingThreadID + "_";

      for (int i = 2; i < arguments.size(); i++) {
        aStr = aStr + " additional arg: " + arguments.get(i);
      }

      logInfo(aStr);

      final boolean isRegionContext = context instanceof RegionFunctionContext;
      boolean isPartitionedRegionContext = false;
      RegionFunctionContext regionContext = null;
      if (isRegionContext) {
        regionContext = (RegionFunctionContext) context;
        isPartitionedRegionContext = PartitionRegionHelper
            .isPartitionedRegion(regionContext.getDataSet());
      }
      Log.getLogWriter().info(
          opPrefix + "isPartitionedRegionContext: "
              + isPartitionedRegionContext);

      Region region = null;
      PartitionedRegion pRegion = null;
      if (isPartitionedRegionContext) {
        pRegion = (PartitionedRegion) regionContext.getDataSet();
        region = pRegion;
      } else
        region = regionContext.getDataSet();

      // TODO : Add recorder and return value of region operation
      EntryOperations eOps = new EntryOperations(region);
      eOps.doEntryOperation();

    }

    public String getId() {
      return this.getClass().getName();
    }

    public boolean hasResult() {
      return true;
    }

    public boolean optimizeForWrite() {
      return true;
    }

    public void init(Properties props) {
    }

    public boolean isHA() {
      return true;
    }
  }
  
  
  
  public static class GenericFunctionOp extends FunctionAdapter {

    private String functionId = null;
    
    public GenericFunctionOp(String str) {
      this.functionId = str;
    }

    @Override
    public void execute(FunctionContext context) {
      String arg = null;
      Object arguments = (context.getArguments());
      if(arguments!=null && arguments instanceof String)      
        arg = (String) arguments;
      
      //Object array[] = {this.functionId, RemoteTestModule.getMyClientName(), arg};
      String array = ManagementUtil.getMemberID();
      CLIBlackboard.getBB().addToList(CLIBlackboard.FUNCTION_EXEC_LIST, array);      
      context.getResultSender().lastResult(this.functionId + "-" + RemoteTestModule.getMyClientName());
      HydraUtil.logInfo("Executed function " + this.functionId + " on member " + RemoteTestModule.getMyClientName());
    }

    public String getId() {
      return functionId;
    }

    public boolean hasResult() {
      return true;
    }

    public boolean optimizeForWrite() {
      return true;
    }

    public void init(Properties props) {
    }

    public boolean isHA() {
      return true;
    }
  }



  public FunctionOperationEvents getOperationRecorder() {    
    return opRecorder;
  }
  
  public static class CustomResultCollector implements ResultCollector{

    private ArrayList resultList = new ArrayList(); 

    public void addResult(DistributedMember memberID,
        Object result) {
      this.resultList.add(result);
    }
    
    public void endResults() {
    }

    public Object getResult() throws FunctionException {
      CLIBlackboard.getBB().addToList(CLIBlackboard.FUNCTION_EXEC_RESULTCOLLECTOR, RemoteTestModule.getMyClientName());      
      return resultList;
    }

    public Object getResult(long timeout, TimeUnit unit)
        throws FunctionException {
      return resultList;
    }
    
    public void clearResults() {
      resultList.clear();
    }
    
  }
  
  
}
