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
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.util.ReadWriteFile;

/**
 * 
 * @author Ajay Pande
 * @since 7.0
 */
public class LogFileFunction implements Function, InternalEntity {
  public static final String ID = LogFileFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(final FunctionContext context) {

    Thread waiting = new Thread(new Runnable() {
      public void run() {
        try {
          Cache cache = CacheFactory.getAnyInstance();
          cache.getLogger().fine("Exporting logs LogFileFunction");
          Object[] args = (Object[]) context.getArguments();
          String targetDirName = ((String) args[0]);
          String logLevel = ((String) args[1]);
          String onlyLogLevel = ((String) args[2]);          
          int numOfLogFilesForTesting = ((Number)args[5]).intValue();          

          InternalDistributedSystem ds = (InternalDistributedSystem) cache
              .getDistributedSystem();
          if (ds != null && ds.isConnected()) {
            // write all the file in the same directory with extension .log
            String filterString = ds.getConfig().getLogFile().getName();
            final String filterStr = filterString.substring(0, filterString
                .lastIndexOf(".") > 0 ? filterString.lastIndexOf(".")
                : filterString.length() - 1);
            
            File dir = ds.getConfig().getLogFile();  // get log file object
            if(dir==null) {
              context.getResultSender().lastResult(CliStrings.format(
                  CliStrings.EXPORT_LOGS__MSG__FAILED_TO_EXPORT_LOG_FILES_FOR_MEMBER_0, ds.getMemberId()));
              return;
            }
            try {
              dir = dir.getAbsoluteFile();   // get absolute log file
            } catch (SecurityException se) {
              context.getResultSender().lastResult(se.getMessage());
              return;              
            }
            String logFileDir = dir.getParent();  // get log file directory
            if(logFileDir==null) logFileDir="/";  // this works in Windows too
            cache.getLogger().fine("for member=" + ds.getMemberId()+ " Exporting logs LogFileFunction logFileDir=" + logFileDir + " filterStr=" + filterStr);
            
            dir = new File(logFileDir);  // get log file directory object
            FilenameFilter filter = new FilenameFilter() {
              public boolean accept(File dir, String name) {
                // get those files which start with the name of the log file
                return name.startsWith(filterStr) && name.endsWith(".log");
              }
            };
            // create target directory if does not exists
            cache.getLogger().fine( "Exporting logs LogFileFunction targetDirName=" + targetDirName);
            File targetDir = new File(targetDirName);            
            if (targetDir.exists() == false) {
              if (targetDir.mkdirs() == true) {
                String logsWritten = processLogs(dir, logFileDir, targetDirName, cache, logLevel, onlyLogLevel, filter, ((Number) args[3]).toString(), ((Number) args[4]).toString(), numOfLogFilesForTesting);
                cache.getLogger().fine("for member="+ ds.getMemberId() + "Done with Exporting logs LogFileFunction targetDirName="+ targetDirName);
                context.getResultSender().lastResult(logsWritten);
              } else {
                cache.getLogger().fine("for member="+ ds.getMemberId()+ CliStrings.EXPORT_LOGS__MSG__TARGET_DIR_CANNOT_BE_CREATED + " " + targetDirName);
                context.getResultSender().lastResult(ResultBuilder.createInfoResult(CliStrings.format(CliStrings.EXPORT_LOGS__MSG__TARGET_DIR_CANNOT_BE_CREATED,targetDir)));
              }
            } else {
              String logsWritten = processLogs(dir, logFileDir, targetDirName,cache, logLevel, onlyLogLevel, filter, ((Number) args[3]).toString(), ((Number) args[4]).toString(), numOfLogFilesForTesting);
              cache.getLogger().fine("for member="+ ds.getMemberId()+ "Done with Exporting logs LogFileFunction targetDirName="+ targetDirName + " logsWritten=" + logsWritten);
              context.getResultSender().lastResult(logsWritten);
            }
          } else {
            context.getResultSender().lastResult(LocalizedStrings.InternalDistributedSystem_THIS_CONNECTION_TO_A_DISTRIBUTED_SYSTEM_HAS_BEEN_DISCONNECTED.toLocalizedString());
          }

        } catch (Exception e) {
          context.getResultSender().lastResult(e.getMessage());          
        }
      }
    });
    try {
      CacheFactory.getAnyInstance().getLogger().fine("for member="+ ((InternalDistributedSystem) CacheFactory.getAnyInstance().getDistributedSystem() ).getMemberId() 
          + "started copying log files");
      waiting.start();
      waiting.join();
      CacheFactory.getAnyInstance().getLogger().fine("for member="+ ((InternalDistributedSystem) CacheFactory.getAnyInstance().getDistributedSystem() ).getMemberId() 
          + "done with Exporting all log files");
    } catch (Exception e) {
      context.getResultSender().lastResult(e.getMessage());
    } finally {
      if (waiting.isAlive()) {
        waiting.interrupt();
      }
    }
  }

  public String processLogs(File dir, String logFileDir, String targetDirName,
      Cache cache, String logLevel, String onlyLogLevel, FilenameFilter filter, String startTime, String endTime, int numOfLogFilesForTesting) {
    try {
      String[] logsInDir = dir.list(filter);
      cache.getLogger().fine("LogFileFunction  processLogs logsInDir="+logsInDir.length + " sample ="+ logsInDir[0]);
      StringBuilder logsWritten = new StringBuilder();
      if (logsInDir.length > 0) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-ms");
        //numOfLogFilesForTesting is used to limit the files that get copied while running entire dunit suite
        //from user perspective numOfLogFilesForTesting is of no use
        cache.getLogger().fine("LogFileFunction  before copy  numOfLogFilesForTesting="+numOfLogFilesForTesting);
        
        for (int i = 0, j= 0 ; i < logsInDir.length && (j < numOfLogFilesForTesting || numOfLogFilesForTesting == 0 ); i++,j++) {
          String fileName = new String(logFileDir + File.separator
              + logsInDir[i]);

        String logToBeWritten = targetDirName
            + File.separator
            + MBeanJMXAdapter.getMemberNameOrId(cache
                .getDistributedSystem().getDistributedMember())
            + "_"
            + logsInDir[i].substring(0, logsInDir[i].length() - 4)
            + "_"
            + sdf.format(new java.util.Date())
            + logsInDir[i].substring(logsInDir[i].length() - 4, logsInDir[i]
                .length());

        // create a new process for log read and write
        cache.getLogger().fine("LogFileFunction  processLogs fileName="+fileName +" logToBeWritten="+logToBeWritten);
        List<String> commandList = new ArrayList<String>();
        commandList.add(System.getProperty("java.home") + File.separatorChar
            + "bin" + File.separatorChar + "java");
        commandList.add("-classpath");
        commandList.add(System.getProperty("java.class.path", "."));
        commandList.add(ReadWriteFile.class.getName());
        commandList.add(fileName);
        commandList.add(logToBeWritten);
        commandList.add(logLevel);
        commandList.add(onlyLogLevel);
        commandList.add(startTime);
        commandList.add(endTime);
        ProcessBuilder procBuilder = new ProcessBuilder(commandList);
        
        StringBuilder output = new StringBuilder();
        String errorString = new String(), resultString = new String();
        try {
          Process copyLogProcess = procBuilder.redirectErrorStream(true)
              .start();
          cache.getLogger().fine("LogFileFunction  processLogs fileName before process waitfor");
          int compacterProcessStatus= copyLogProcess.waitFor();
          cache.getLogger().fine("LogFileFunction  processLogs fileName after process waitfor destroy compacterProcessStatus="+compacterProcessStatus);
          InputStream inputStream = copyLogProcess.getInputStream();
          BufferedReader br = new BufferedReader(new InputStreamReader(
              inputStream));
          String line = null;

          while ((line = br.readLine()) != null) {
            output.append(line).append(GfshParser.LINE_SEPARATOR);
          }          
          copyLogProcess.destroy();
          cache.getLogger().fine("LogFileFunction  processLogs fileName after process waitfor after destroy compacterProcessStatus="+compacterProcessStatus);
          
        } catch (IOException e) {
          errorString = (new GemFireIOException(
              " Exception in LogFileFunction is " + e, e)).toString();
        } finally {
          if (errorString != null) {
            output.append(errorString).append(GfshParser.LINE_SEPARATOR);
          }
          resultString = output.toString();
        }
          // merge files which are written successfully
          if (resultString.contains("Sucessfully written file")) {
            cache.getLogger().fine("LogFileFunction  wrote file logToBeWritten="+logToBeWritten);
            logsWritten.append(logToBeWritten + ";");
          }else{
            cache.getLogger().fine("LogFileFunction  wrote file logToBeWritten="+logToBeWritten+ " resultString="+resultString);            
          }
        }

      }else{
        cache.getLogger().fine(CliStrings.format("No file was found for exporting in export logs function"));        
      }
      cache.getLogger().fine(CliStrings.format("logsWritten="+logsWritten.toString()));        
      return logsWritten.toString();
    } catch (Exception e) {
      return ("Exception in LogFileFunction processLogs " + e.getMessage());
    }
  }

  @Override
  public String getId() {
    return LogFileFunction.ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    // no need of optimization since read-only.
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}