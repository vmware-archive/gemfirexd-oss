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
package management.cli;

import hydra.Log;
import hydra.RemoteTestModule;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import jline.ConsoleReader;
import jline.ConsoleReaderWrapper;
import management.test.cli.CLITest;
import management.util.HydraUtil;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.ExecutionStrategy;
import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.core.JLineCompletorAdapter;
import org.springframework.shell.core.JLineLogHandler;
import org.springframework.shell.core.Parser;
import org.springframework.shell.core.Shell;
import org.springframework.shell.event.ParseResult;
import org.springframework.shell.event.ShellStatus.Status;

import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.commands.ShellCommands;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.shell.GfshConfig;
import com.gemstone.gemfire.management.internal.cli.util.spring.StringUtils;

/**
 *
 * GfshShell implementation implementing ShellEvent interface
 *
 * @author tushark
 *
 */

public class TestableGfsh extends Gfsh implements ShellEvent{

  protected static final Object EXIT_SHELL_COMMAND = ShellCommands.class.getCanonicalName() + ".exit";
  private EventedInputStream eis = null;
  private ByteArrayOutputStream output = null;
  private JLineCompletorAdapter completorAdaptor = null;
  private Map<String, List> completorOutput = new HashMap<String, List>();
  private Map<String, Object> commandOutput = new HashMap<String, Object>();
  private ConsoleReader newConsoleReader = null;
  private String name=null;
  private String commandExecError = null;
  private CountDownLatch resultLatch =null;

  private Writer wrappedOut = null;  // saj

  final private Parser oldParser;
  private Parser parserHook = null;
  private long timeout = 60;
  public static boolean useVirtualTerm = true;

  public void setTimeout(long timeout) {
	this.timeout = timeout;
  }

  static{
	String osName = System.getProperty("os.name").toLowerCase();
	if(osName.indexOf("windows")==-1 && useVirtualTerm){
	  System.setProperty("jline.terminal", jline.VirtualUnixTerminal.class.getCanonicalName());
	}
	if(System.getProperty("gfsh.disable.color")==null) {
          System.setProperty("gfsh.disable.color", "true");
        }
	System.setProperty(Gfsh.ENV_APP_QUIET_EXECUTION, "true");
  }

  public static void setVirtualTerminal(boolean yes){
	  if(yes){
		String osName = System.getProperty("os.name").toLowerCase();
		if(osName.indexOf("windows")==-1 && useVirtualTerm){
		  System.setProperty("jline.terminal", jline.VirtualUnixTerminal.class.getCanonicalName());
		}
	  }else{
		  System.setProperty("jline.terminal", "");
	  }

	  if(System.getProperty("gfsh.disable.color")==null) {
	    System.setProperty("gfsh.disable.color", "true");
	  }

  }

  public TestableGfsh(String name, boolean launchShell, String[] args) throws ClassNotFoundException, IOException {
    super(launchShell, args, new TestableGfshConfig(name));
    oldParser = super.getParser();
    resultLatch = new CountDownLatch(1);
    parserHook = new Parser() {

      @Override
      public ParseResult parse(String buffer) {
        Util.debug("In parsing hook .... with input buffer <" + buffer + ">");
        ParseResult result = null;
        try{
          result = oldParser.parse(buffer);
        }catch(Exception e){
          String reason = e.getMessage() != null ? " Reason: "+e.getMessage() + " Exception: " + String.valueOf(e) : " Exception: " + String.valueOf(e);
          addError("Parsing failed...."  + reason + " buffer returned by EIS " + eis.getBufferFormdAfterReading(),e);
          return null;
        }
        if(result==null){
        	addError("Parsing failed....",null);
        }else{
          Util.log("Parse Result is " + result);
        }
        return result;
      }

//      @Override
      public int completeAdvanced(String buffer, int cursor, List<Completion> candidates) {
        return oldParser.completeAdvanced(buffer, cursor, candidates);
      }

      @Override
      public int complete(String buffer, int cursor, List<String> candidates) {
        return oldParser.complete(buffer, cursor, candidates);
      }
    };

    this.name = name;
    Util.debug("Using CommandManager configured with commands "+ this.getCommandNames(""));
    eis = new EventedInputStream();
    output = new ByteArrayOutputStream(1024 * 10);
    PrintStream sysout = new PrintStream(output, true);
    TestableGfsh.setGfshOutErr(sysout);

//    Writer out = new BufferedWriter(new OutputStreamWriter(sysout));  // saj
    wrappedOut = new BufferedWriter(new OutputStreamWriter(sysout));

    try {
      ConsoleReaderWrapper wrapper = new ConsoleReaderWrapper(eis, wrappedOut);
      Util.debug("Reader created is " + wrapper);
      newConsoleReader = wrapper;
      reader = newConsoleReader;
      completorAdaptor = new JLineCompletorAdapterWrapper(getParser(), this);
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    }
  }

  private static void setGfshOutErr(PrintStream outToUse) {
    Gfsh.gfshout = outToUse;
    Gfsh.gfsherr = outToUse;
  }

  private static void restoreGfshOutErr() {
    Gfsh.gfshout = System.out;
    Gfsh.gfsherr = System.err;
  }

  protected void addError(String string, Exception e) {
	if(e!=null)
		Util.error(string ,e);
	else Util.error(string);
	commandExecError = string;
	resultLatch.countDown();
}

public void addCompletorOutput(String buffer, List candidates) {
	completorOutput.put(buffer, candidates);
	resultLatch.countDown();
  }

  @Override
  protected ExecutionStrategy getExecutionStrategy() {

    final ExecutionStrategy oldStrategy = super.getExecutionStrategy();
    return new ExecutionStrategy() {
      public Object execute(ParseResult parseResult) throws RuntimeException {
        Object obj = null;
        String command = null;
        try {
          String method = parseResult.getMethod().getName();
          String className = parseResult.getInstance().getClass().getCanonicalName();
          command = className + "." + method;
          Util.log("Executing command " + command + " with Gfsh instance " + gfshThreadLocal.get());
          long l1 = System.currentTimeMillis();
          obj = oldStrategy.execute(parseResult);
          long l2 = System.currentTimeMillis();
          Util.log("Completed execution of command " + command + " in " + (l2-l1) + " ms.");
          if(obj!=null){
            Util.log("Command output class is " + obj.getClass());
          }else{
            obj =  "VOID";
          }
        } catch (Exception e) {
          addError("Error running command " , e);
          throw new RuntimeException(e);
        }
        Util.debug("Adding outuut and notifying threads ..");
        addOutput(command, obj);
        if(command.equals(EXIT_SHELL_COMMAND))
          resultLatch.countDown();
        return obj;
      }

      public boolean isReadyForCommands() {
        return oldStrategy.isReadyForCommands();
      }

      public void terminate() {
        oldStrategy.terminate();
        Util.log("GFSH is exiting now, thankyou for using");
      }
    };
  }

  @Override
  protected ConsoleReader createConsoleReader() {
    Util.debug("Returning wrapper reader -->" + newConsoleReader);
    return newConsoleReader;
  }

  private void myremoveHandlers(final Logger l) {
    Handler[] handlers = l.getHandlers();
    if (handlers != null && handlers.length > 0) {
      for (Handler h : handlers) {
        l.removeHandler(h);
      }
    }
  }

  /**
   * Following code is copied from JLineShell of spring shell
   * to manipulate consoleReader.
   * JLineShell adds its own completorAdaptor.
   *
   * Another addition is using of ThreadLocal for storing Gfsh
   * instead of static singleton instance
   *
   */
  @Override
  public void run() {


    gfshThreadLocal.set(this);
    Util.debug("Setting threadLocal " + gfshThreadLocal.get());

    reader = createConsoleReader();

    setPromptPath(null);

    JLineLogHandler handler = new JLineLogHandler(reader, this);
    JLineLogHandler.prohibitRedraw(); // Affects this thread only
    Logger mainLogger = Logger.getLogger("");
    myremoveHandlers(mainLogger);
    mainLogger.addHandler(handler);

    //reader.addCompletor(new JLineCompletorAdapter(getParser()));
    reader.addCompletor(completorAdaptor);

    reader.setBellEnabled(true);
    if (Boolean.getBoolean("jline.nobell")) {
      reader.setBellEnabled(false);
    }

    // reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));

    /*-
    openFileLogIfPossible();
    this.reader.getHistory().setMaxSize(getHistorySize());
    // Try to build previous command history from the project's log
    String[] filteredLogEntries = filterLogEntry();
    for (String logEntry : filteredLogEntries) {
      reader.getHistory().addToHistory(logEntry);
    }*/

    //flashMessageRenderer();
    flash(Level.FINE, this.getProductName() + " " + this.getVersion(), Shell.WINDOW_TITLE_SLOT);
    printBannerAndWelcome();

    String startupNotifications = getStartupNotifications();
    if (StringUtils.hasText(startupNotifications)) {
      logger.info(startupNotifications);
    }

    setShellStatus(Status.STARTED);
    /*-
    // Monitor CTRL+C initiated shutdowns (ROO-1599)
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        shutdownHookFired = true;
        // We don't need to closeShell(), as the shutdown hook in o.s.r.bootstrap.Main calls stop() which calls JLineShellComponent.deactivate() and that calls closeShell()
      }
    }, getProductName() + " JLine Shutdown Hook")); */

    // Handle any "execute-then-quit" operation

    String rooArgs = System.getProperty("roo.args");
    if (rooArgs != null && !"".equals(rooArgs)) {
      setShellStatus(Status.USER_INPUT);
      boolean success = executeCommand(rooArgs);
      if (exitShellRequest == null) {
        // The command itself did not specify an exit shell code, so we'll fall back to something sensible here
        executeCommand("quit"); // ROO-839
        exitShellRequest = success ? ExitShellRequest.NORMAL_EXIT : ExitShellRequest.FATAL_EXIT;
      }
      setShellStatus(Status.SHUTTING_DOWN);
    }
    else {
      // Normal RPEL processing
      promptLoop();
    }
    Util.debug("Exiting the shell");
  }

  @Override
  public String getShellName() {
    return "Gfsh Launcher - " + name;
  }

  public ShellEvent tab() throws IOException {
    return eis.tab();
  }

  public ShellEvent addChars(String seq) throws IOException {

    return eis.addChars(seq);
  }

  public ShellEvent addCtrlZ() throws IOException {

    return eis.addCtrlZ();
  }

  public ShellEvent addCtrlD() throws IOException {

    return eis.addCtrlD();
  }

  public ShellEvent newline() throws IOException {
    eis.newline();
    return end();
  }

  public synchronized ShellEvent end() {
    return eis.end();
  }

  public void eof() {
    eis.eof();
  }

  public void terminate() {
    closeShell();
  }

  @Override
  public void stop() {
    super.stop();
    // set vm as a gfsh vm
    CliUtil.isGfshVM = false;

    //restore gfshout & gfsherr
    TestableGfsh.restoreGfshOutErr();
  }

  @Override
  public void notifyDisconnect(String endPoints) {
    super.notifyDisconnect(endPoints);
    addError(CliStrings.format(CliStrings.GFSH__MSG__NO_LONGER_CONNECTED_TO_0, new Object[] {endPoints}), null);
  }

  public String getOutputText() {
    return output.toString();
  }

  public String getPlainOutputText() {  // saj
    StringBuilder strBldr = new StringBuilder();

    String tmpStr = output.toString();

    Util.log("getOutputText shell string is:\n" + tmpStr);   // saj
    int x = 0;
    int havePromptOnFront = 0;

    x = tmpStr.indexOf("\\\n");
    if(x!=-1) {    // remove possible continuation
      tmpStr = tmpStr.substring(x+2);
    }

    x = tmpStr.indexOf(";\n");
    if(x!=-1) {    // remove command echo
      tmpStr = tmpStr.substring(x+2);
    }

    x = tmpStr.indexOf("CommandResult [");
    if(x!=-1) {
      x = tmpStr.indexOf(0x0a, x);
      if(x!=-1) {  // remove freaky CommandResult
        tmpStr = tmpStr.substring(x+1);
      }
    }

    x = tmpStr.lastIndexOf(">");  // may be prompt on end
    if(x!=-1 && x>=tmpStr.length()-3) { // found prompt near end of output
      if(tmpStr.lastIndexOf(0x0a)!=-1) {
        tmpStr=tmpStr.substring(0,tmpStr.lastIndexOf(0x0a));
      } else {
        tmpStr="";
      }
    }

    String mask = "\\r";
    tmpStr = tmpStr.replaceAll(mask,"").trim();

    strBldr.append(tmpStr);

    tmpStr = getPresentationString(this)[0];

    x = tmpStr.indexOf("CommandResult [");
    if(x!=-1) {
      x=x-1;
      for (; x >= 0; x--) {
        char ch = tmpStr.charAt(x);
        if (!Character.isWhitespace(ch)) break;
      }
      tmpStr = tmpStr.substring(0,x+1);
    }
    tmpStr = tmpStr.trim();
    strBldr.append(tmpStr);

    Util.log("getOutputText result string is:\n" + strBldr.toString());   // saj
    if(strBldr.toString().indexOf("Cluster-1")!=-1 || strBldr.toString().startsWith(">")) {
      Util.log("getOutputText missed something:\n" + toHexString(output.toByteArray()));   // saj
    }
    return strBldr.toString();
  }

  public static String toHexString(byte[] array) {
    char[] symbols="0123456789ABCDEF".toCharArray();
    char[] hexValue = new char[array.length * 2];

    for(int i=0;i<array.length;i++)
    {
    //convert the byte to an int
    int current = array[i] & 0xff;
    //determine the Hex symbol for the last 4 bits
    hexValue[i*2+1] = symbols[current & 0x0f];
    //determine the Hex symbol for the first 4 bits
    hexValue[i*2] = symbols[current >> 4];
    }
    return new String(hexValue);
  }

  public synchronized void clearEvents() {
    resultLatch = new CountDownLatch(1);
    commandExecError = null;
    eis.clearEvents();
    completorOutput.clear();
    commandOutput.clear();
    Util.debug("buffer before clear <" + reader.getCursorBuffer().toString() + ">");
    reader.getCursorBuffer().clearBuffer();
    Util.debug("buffer after clear <" + reader.getCursorBuffer().toString() + ">");
    output.reset();
    try {
      output.flush();
    } catch (IOException e){
      Util.error(e);
    }
  }

  public Map<String, Object> getCommandOutput() {
    return Collections.unmodifiableMap(commandOutput);
  }

  public Map<String, List> getCompletorOutput() {
    return Collections.unmodifiableMap(completorOutput);
  }

  public void addOutput(String command, Object object) {
	 commandOutput.put(command, object);
	 //resultLatch.countDown();
  }

  public void waitForOutput() {
	try {
		boolean completed = resultLatch.await(timeout, TimeUnit.SECONDS);
		if(!completed){
			commandExecError = "Command Timeout Error ";
			HydraUtil.threadDump();
			Util.error(commandExecError);
		}
	} catch (InterruptedException e) {
		commandExecError = "Command InterruptedException Error ";
		Util.error(commandExecError);
	}
  }


  @Override
  protected void handleExecutionResult(Object result) {
    super.handleExecutionResult(result);
    resultLatch.countDown();
  }

  @Override
  protected Parser getParser() {
    return parserHook;
  }

  public boolean hasError(){
    return (commandExecError!=null);
  }

  public String getError(){
    return commandExecError;
  }

  public static List<Object> autoComplete(TestableGfsh shell , String command) {
    try {
      Log.getLogWriter().info("Executing auto-complete command " + command + " with command Mgr " + CommandManager.getInstance());
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    try {
      shell.addChars(command).addChars("\t").newline();
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    shell.waitForOutput();
    if (shell.hasError()) {
      throw new TestException("Command " + command + " failed with error " + shell.getError());
    } else {
      List<Object> completorOutput = shell.getCompletorOutput().get(command);
      String outputText = shell.getOutputText();
      Object commandOutput = shell.getCommandOutput();
      Log.getLogWriter().info("Command completorOutput for " + command + ": " + completorOutput);
      Log.getLogWriter().info("Command outputText for " + command + ": " + outputText);
      shell.clearEvents();
      return completorOutput;
    }
  }

  public static Object[] execAndLogCommand(TestableGfsh shell , String command, PrintWriter commandOutputFile, boolean useFineLevelForLogging) {
    try {
      Log.getLogWriter().info("Executing command " + command + " with command Mgr " + CommandManager.getInstance());
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    try {
      shell.addChars(command).addChars(";").newline();
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    shell.waitForOutput();
    if (shell.hasError()) {
      throw new TestException("Command " + command + " failed with error " + shell.getError());
    } else {
      Map<String,List> completorOutput = shell.getCompletorOutput();
      String outputText = shell.getOutputText();
      Map<String,Object> commandOutput = shell.getCommandOutput();


      String[] tmpArr = getPresentationString(shell);
      String presentationStr = tmpArr[0];
      String errStr = tmpArr[1];
      if(!useFineLevelForLogging){
        Log.getLogWriter().info("Command completorOutput for " + command + ": " + completorOutput);
        Log.getLogWriter().info("Command outputText for " + command + ": " + outputText);
        Log.getLogWriter().info("Command output for " + command + ": " + commandOutput);
        Log.getLogWriter().info("Presentation string for " + command + ":\n" + presentationStr);
      }else{
        Log.getLogWriter().fine("Command completorOutput for " + command + ": " + completorOutput);
        Log.getLogWriter().fine("Command outputText for " + command + ": " + outputText);
        Log.getLogWriter().fine("Command output for " + command + ": " + commandOutput);
        Log.getLogWriter().fine("Presentation string for " + command + ":\n" + presentationStr);
      }

      /* CommandOutputValidator is doing this also issue is if command contains ERROR
       * below code will fail it.
      if (errStr.length() > 0) {
        Log.getLogWriter().info(errStr); // todo lynn; throw this instead
      }


      if ((presentationStr.indexOf("ERROR") >= 0) ||
          (presentationStr.indexOf("Exception") >= 0)) {
        throw new TestException("Unexpected command output:\n" + presentationStr);
      }
      //todo lynn - enable this when bug 45337 is fixed
      if ((outputText.indexOf("ERROR") >= 0) ||
          (outputText.indexOf("Exception") >= 0)) {
        throw new TestException("Unexpected output text:\n" + outputText);
      }*/

      logCommandOutput(shell,commandOutputFile,command, presentationStr, errStr);
      commandOutput = (Map<String, Object>) HydraUtil.copyMap(commandOutput);
      completorOutput = (Map<String, List>) HydraUtil.copyMap(completorOutput);
      shell.clearEvents();
      return new Object[]{commandOutput,completorOutput, presentationStr, errStr};
    }
  }

  public static Object[] execCommand(TestableGfsh shell , String command, PrintWriter commandOutputFile) {
    try {
      Log.getLogWriter().info("Executing command " + command + " with command Mgr " + CommandManager.getInstance());
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    try {
      shell.addChars(command).addChars(";").newline();
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    shell.waitForOutput();
    if (shell.hasError()) {
      throw new TestException("Command " + command + " failed with error " + shell.getError());
    } else {
      Map<String,List> completorOutput = shell.getCompletorOutput();
      String outputText = shell.getOutputText();
      Map<String,Object> commandOutput = shell.getCommandOutput();
      String[] tmpArr = getPresentationString(shell);
      String presentationStr = tmpArr[0];
      String errStr = tmpArr[1];
      /*
      if (errStr.length() > 0) {
        Log.getLogWriter().info(errStr); // todo lynn; throw this instead
      }
      if ((presentationStr.indexOf("ERROR") >= 0) ||
          (presentationStr.indexOf("Exception") >= 0)) {
        throw new TestException("Unexpected command output:\n" + presentationStr);
      }
      //todo lynn - enable this when bug 45337 is fixed
      if ((outputText.indexOf("ERROR") >= 0) ||
          (outputText.indexOf("Exception") >= 0)) {
        throw new TestException("Unexpected output text:\n" + outputText);
      }*/
      commandOutput = (Map<String, Object>) HydraUtil.copyMap(commandOutput);
      completorOutput = (Map<String, List>) HydraUtil.copyMap(completorOutput);
      shell.clearEvents();
      return new Object[]{commandOutput,completorOutput, presentationStr, errStr};
    }
  }



  public static String[] getCommandOutputStrings(TestableGfsh shell) {
    if(hasResultObject(shell)) {
      return getPresentationString(shell);
    } else {
      String[] x  = {shell.getOutputText(), ""};
      return x;
    }
  }

  private static boolean hasResultObject(TestableGfsh shell) {

    for (Object value: shell.getCommandOutput().values()) {
      if (value instanceof CommandResult) {
        CommandResult cr = (CommandResult) value;
        try {
          Log.getLogWriter().info("TestableGfsh saw ResultObject as follows: " + cr.getContent().toString());
          Log.getLogWriter().info("                                    size: " + cr.getContent().size());
          Log.getLogWriter().info("                                toString: " + cr.getContent().toString());
          Log.getLogWriter().info("                                   class: " + cr.getContent().getClass().getName());
          if(cr.getContent().size()==0) {
            return false;
          } else {
            StringBuffer checkForOutput = new StringBuffer();
            while (cr.hasNextLine()) {
              String line = cr.nextLine();
              checkForOutput.append(line);
            }
            String tmpStr = checkForOutput.toString();
            if(tmpStr.trim().length() == 0) {
              return false;
            } else {
              return true;
            }
          }
        } catch(Exception ex) {}
        return true;
      }
    }

    return false;
  }


  private static String[] getPresentationString(TestableGfsh shell) {
    Map<String, Object> result = shell.getCommandOutput();
    StringBuffer presentationStr = new StringBuffer();
    StringBuffer errStr = new StringBuffer();
    for (Object key: result.keySet()) {
      Object value = result.get(key);
//      Log.getLogWriter().info("key class is " + key.getClass().getName() + ", key is " + key + " value is class " +
//            value.getClass().getName() + ", value is " + value + "\n");
      if (value instanceof CommandResult) {
        CommandResult cr = (CommandResult)value;
        cr.resetToFirstLine();
        while (cr.hasNextLine()) {
          String line = cr.nextLine();
          presentationStr.append(line).append("\n");
          //errStr.append(checkForRightPadding(line));
        }
      }
    }
    return new String[] {presentationStr.toString(), errStr.toString()};
  }

  private static String checkForRightPadding(String aStr) {
    Log.getLogWriter().info("Checking for white space for line: \"" + aStr + "\"");
    String[] tokens = aStr.split("\n");
    StringBuffer errStr = new StringBuffer();
    for (String line: tokens) {
      //for (String )
      int whiteSpaceCount = 0;
      for (int i = line.length()-1; i >= 0; i--) {
        char ch = line.charAt(i);
        if (Character.isWhitespace(ch)) {
          whiteSpaceCount++;
          if (i == 0) { // found a blank line
            errStr.append("\"" + line + "\" contains " + whiteSpaceCount + " white space characters on the right\n");
          }
        } else { // found a non-white space character
          if (whiteSpaceCount > 0) { // we previously found a whitespace character
            errStr.append("\"" + line + "\" contains " + whiteSpaceCount + " white space characters on the right\n");
          }
          break;
        }
      }
    }
    return errStr.toString();
  }

  private static void logCommandOutput(TestableGfsh shell, PrintWriter commandOutputFile, String command, String output, String errStr) {
    StringBuffer logStr = new StringBuffer();
    logStr.append("================================================================================\n");
    logStr.append("" + (new Date()) + " vm_" + RemoteTestModule.getMyVmid() +
        ", thr_" + RemoteTestModule.getCurrentThread().getThreadId() +
        ", pid " + RemoteTestModule.getMyPid());
    logStr.append("\nCommand: \"" + command + "\"\n");
    if(shell.isConnectedAndReady())
      logStr.append("Connected  to: \"" + shell.getOperationInvoker().toString() + "\"\n");
    else logStr.append("Connected  : \"No\"\n");
    logStr.append("Context Path: \"" + shell.getEnvProperty(Gfsh.ENV_APP_CONTEXT_PATH) + "\"\n");
    logStr.append("Fetch Size: \"" + shell.getEnvProperty(Gfsh.ENV_APP_FETCH_SIZE) + "\"\n");
    logStr.append("Query Result Display Mode: \"" + shell.getEnvProperty(Gfsh.ENV_APP_QUERY_RESULTS_DISPLAY_MODE) + "\"\n");
    logStr.append("Query Collection Limit: \"" + shell.getEnvProperty(Gfsh.ENV_APP_COLLECTION_LIMIT) + "\"\n");
    logStr.append("Last Exist Status: \"" + shell.getEnvProperty(Gfsh.ENV_APP_LAST_EXIT_STATUS) + "\"\n");
    logStr.append("Output (starts at beginnning of next line), output length is " + output.length() + ":\n" + output + "<--end of output\n");
    if (errStr.length() > 0) {
      logStr.append(errStr + "\n");
    }
    logStr.append("\n");
    synchronized (CLITest.class) {
      commandOutputFile.print(logStr.toString());
      commandOutputFile.flush();
    }
  }



  /**
   * GfshConfig for tests.
   */
  static class TestableGfshConfig extends GfshConfig {
    {
      // set vm as a gfsh vm
      CliUtil.isGfshVM = true;
    }

    private File   parentDir;
    private String fileNamePrefix;
    private String name;
    private String generatedHistoryFileName = null;

    public TestableGfshConfig(String name) {
      this.name = name;

      if (isDUnitTest(this.name)) {
        fileNamePrefix = this.name;
      } else {
        try{
          fileNamePrefix = RemoteTestModule.getMyClientName();
        } catch(Exception e) {
          fileNamePrefix = "non-hydra-client";
        }
      }

      parentDir = new File("gfsh_files");
      parentDir.mkdirs();
    }

    private static boolean isDUnitTest(String name) {
      boolean isDUnitTest = false;
      if (name != null) {
        String[] split = name.split("_");
        if (split.length != 0 && split[0].endsWith("DUnitTest")) {
          isDUnitTest = true;
        }
      }
      return isDUnitTest;
    }

    @Override
    public String getLogFilePath() {
      return new File(parentDir, getFileNamePrefix() + "-gfsh.log").getAbsolutePath();
    }

    private String getFileNamePrefix() {
      String timeStamp = new java.sql.Time(System.currentTimeMillis()).toString();
      timeStamp = timeStamp.replace(':', '_');
      return fileNamePrefix + "-" + timeStamp;
    }

    @Override
    public String getHistoryFileName() {
      if(generatedHistoryFileName == null) {
        String fileName = new File(parentDir, (getFileNamePrefix() +"-gfsh.history")).getAbsolutePath();
        generatedHistoryFileName = fileName;
        return fileName;
      } else {
        return generatedHistoryFileName;
      }
    }

    @Override
    public boolean isTestConfig() {
      return true;
    }

    @Override
    public Level getLogLevel() {
      // Keep log level fine for tests
      return Level.FINE;
    }
  }
}
