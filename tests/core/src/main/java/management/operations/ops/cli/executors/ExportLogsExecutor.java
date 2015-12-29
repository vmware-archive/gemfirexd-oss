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
package management.operations.ops.cli.executors;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import util.TestException;

import management.cli.CommandOutputValidator;
import management.cli.TestableGfsh;
import management.cli.CommandOutputValidator.CommandOutputValidatorResult;
import management.operations.ops.cli.TestCommand.CommandOption;
import management.operations.ops.cli.TestCommandInstance;
import management.util.HydraUtil;

import com.gemstone.gemfire.management.internal.cli.commands.MiscellaneousCommands;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

public class ExportLogsExecutor extends AbstractTestCommandExecutor {
  
  
  /*
  Steps
    1. List down all command modes  :Done
    2. Check command generated with dummy executeMethod : Done
    3. Run mini-regression to testout all modes execute correctly : Done
    4. Add command output validators : Done
    5. Add bad-inputs for mode : ***Remaining***
    6. Add bad-combinations for mode : ***Remaining***
 */
  
  /*
  @Override
  public Object executeAndVerify(TestCommandInstance instance) {
    HydraUtil.logInfo("Executed command : " + instance.toString());
    return null;
  }*/
  
  @Override
  public Object verifyCommand(TestableGfsh gfsh, Object object) {
    Object[] outputs = (Object[])object;
    CommandOutputValidator validator = new CommandOutputValidator(gfsh, outputs);
    validator.addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__CANNOT_EXECUTE).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__CANNOT_MERGE).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__SPECIFY_ONE_OF_OPTION).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__FUNCTION_EXCEPTION).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__FILE_DOES_NOT_EXIST).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__TARGET_DIR_CANNOT_BE_CREATED).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__SPECIFY_ENDTIME).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__SPECIFY_STARTTIME).
      addUnExpectedErrorString(CliStrings.EXPORT_LOGS__MSG__INVALID_TIMERANGE).       
      addUnExpectedErrorString("No member found for executing function")
      .addExpectedString(directory);
      
    CommandOutputValidatorResult result = validator.validate();
    if(!result.result){
      //throw new TestException(result.getExceptionMessage());
      addFailure(result.getExceptionMessage());
    }
    
    
    if(!checkDirectory(directory)){
      //throw new TestException("Export logs command failed to create logs in directory "  +directory);
      addFailure("Export logs command failed to create logs in directory "  +directory);
    }
    
    //reset calendar
    calendar = null;
    directory = null;
    
    return result;
  }

  @Override
  protected void fillOption(TestCommandInstance instance, CommandOption op) {
    //NOOP
  }

  @Override
  protected void fillMandatoryOption(TestCommandInstance instance, String name) {
    if (CliStrings.EXPORT_LOGS__MEMBER.equals(name)) {
      instance.addOption(name, getMemberId());
    } else if (CliStrings.EXPORT_LOGS__GROUP.equals(name)) {
      instance.addOption(name, getGroup());
    } else if (CliStrings.EXPORT_LOGS__LOGLEVEL.equals(name)) {
      instance.addOption(name, getLogLevel());
    } else if (CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL.equals(name)) {
      instance.addOption(name, String.valueOf(HydraUtil.getRandomBoolean()));
    } else if (CliStrings.EXPORT_LOGS__MERGELOG.equals(name)) {
      instance.addOption(name, "true");
    } else if (CliStrings.EXPORT_LOGS__STARTTIME.equals(name)) {
      String startTime =  getStartTime();
      HydraUtil.logInfo("Adding start-time=" + startTime);
      instance.addOption(name, startTime);
    } else if (CliStrings.EXPORT_LOGS__ENDTIME.equals(name)) {
      String endTime = getEndTime() ;
      HydraUtil.logInfo("Adding end-time=" + endTime);
      instance.addOption(name, endTime);
    }else if(name.equals(CliStrings.EXPORT_LOGS__DIR)){
      directory = getDirectory(instance.getMode());
      instance.addOption(name, directory);      
    }
  }
  
  static SimpleDateFormat formatter = new SimpleDateFormat(MiscellaneousCommands.FORMAT);
  
  private Calendar calendar = null;

  private String getEndTime() {
    Date date = null;
    if(calendar==null){ //only end-time
      calendar = Calendar.getInstance();
      int rollback = 1 + HydraUtil.getnextRandomInt(10); //in minutes fair towards near past
      HydraUtil.logFine("Adding diff=" + rollback + " minutes to " + calendar.getTime());
      calendar.add(Calendar.MINUTE, (-1)*rollback);
      date = calendar.getTime();
    }else{
      int rollback = 1 + HydraUtil.getnextRandomInt(10); //in minutes fair towards near past
      HydraUtil.logFine("Adding diff=" + rollback + " minutes to " + calendar.getTime());
      calendar.add(Calendar.MINUTE, rollback);
      date = calendar.getTime();
    }
    HydraUtil.logFine("Final End Time " + date);
    return formatter.format(date);
  }

  private String getStartTime() {
    calendar = Calendar.getInstance();
    int rollback = -1*HydraUtil.getnextRandomInt(60); //in minutes fair towards distant past
    calendar.add(Calendar.MINUTE, rollback);
    Date date = calendar.getTime();
    HydraUtil.logFine("Final Start Time " + date);
    return formatter.format(date);
  }

  private Object getLogLevel() {
    String logLevels[] = {"ALL", "FINEST", "FINER", "FINE", "INFO", "CONFIG", "WARNING", "ERROR", "SEVERE" };    
    return HydraUtil.getRandomElement(logLevels);
  }

  private String directory = null;
  
  @Override
  protected void fillArgument(TestCommandInstance instance, String name) {
    
  }

}
