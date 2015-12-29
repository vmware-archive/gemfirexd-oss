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
package com.gemstone.gemfire.management.internal.cli.exceptions;

import java.util.logging.Logger;

import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;
import com.gemstone.gemfire.management.internal.cli.util.CLIConsoleBufferUtil;
/**
 * @author njadhav
 * 
 *         Prints the warning according the CliException
 * 
 */
public class ExceptionHandler {

  private static Logger LOGGER = Logger.getLogger(ExceptionHandler.class
      .getCanonicalName());
  

  //FIXME define handling when no match is present
  public static void handleException(CliException ce) {
    if (ce instanceof CliCommandException) {
      if (ce instanceof CliCommandNotAvailableException) {
        handleCommandNotAvailableException((CliCommandNotAvailableException) ce);
      } else if (ce instanceof CliCommandInvalidException) {
        handleCommandInvalidException((CliCommandInvalidException) ce);
      } else if (ce instanceof CliCommandOptionException) {
        handleOptionException((CliCommandOptionException) ce);
      }
    }
  }

  private static void handleCommandInvalidException(
      CliCommandInvalidException ccie) {   
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(ccie.getCommandTarget().getGfshMethodTarget().getKey()
        + " is not a valid Command"));   
  }

  private static void handleCommandNotAvailableException(
      CliCommandNotAvailableException ccnae) {    
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(ccnae.getCommandTarget().getGfshMethodTarget().getKey()
        + " is not available at the moment"));    
  }

  private static void handleOptionException(CliCommandOptionException ccoe) {
    if (ccoe instanceof CliCommandOptionNotApplicableException) {
      handleOptionInvalidExcpetion((CliCommandOptionNotApplicableException) ccoe);
    } else if (ccoe instanceof CliCommandOptionValueException) {
      handleOptionValueException((CliCommandOptionValueException) ccoe);
    }
  }

  private static void handleOptionInvalidExcpetion(
      CliCommandOptionNotApplicableException cconae) {  
    String messege = "Parameter " + cconae.getOption().getLongOption()
    + " is not applicable for "
    + cconae.getCommandTarget().getGfshMethodTarget().getKey();    
    LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(messege));  
  }

  private static void handleOptionValueException(
      CliCommandOptionValueException ccove) {
    if (ccove instanceof CliCommandOptionHasMultipleValuesException) {
      String messege = "Parameter " + ccove.getOption().getLongOption() + " can only be specified once";
      LOGGER.warning(CLIConsoleBufferUtil.processMessegeForExtraCharactersFromConsoleBuffer(messege));
    }
  }

  public static Logger getExceptionHanlderLogger(){
    return LOGGER;   
  }


}
