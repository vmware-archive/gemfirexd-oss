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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import management.util.HydraUtil;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

public class CommandOutputValidator {

  public static class CommandOutputValidatorResult {
    public List<String> errorStrings = new ArrayList<String>();
    public boolean result;
    public String command;
    public String getExceptionMessage() {
      StringBuilder sb = new StringBuilder();
      sb.append("Command output validation failed for command <" + command +">").append(GfshParser.LINE_SEPARATOR);
      sb.append("ErrorStrings : ").append(GfshParser.LINE_SEPARATOR);
      int i=0;
      for(String s : errorStrings)
        sb.append("\t").append(i++).append("#. ").append(s).append(GfshParser.LINE_SEPARATOR);
      return sb.toString();
    }
  }

  private static final String EXCEPTION[] = { "Exception", "EXCEPTION", "exception" };
  private static final String ERROR[] = { "error", "Error", "ERROR" };
  private static final String NULL[] = { "null", "Null", "NULL" };

  private CommandResult result;
  private TestableGfsh shell;
  private String command;
  private String presentationStr;
  //private List<String> errorStringPatterns = new ArrayList<String>();
  int expectedRows = -1, expectedColumns = -1;
  //private List<String> stringPatterns = new ArrayList<String>();
  StringValidator stringValidator = null;
  
  public static int EXPECT_ERRORSTRING_INOUTPUT = 2;
  public static int EXPECT_EXCEPTIONSTRING_INOUTPUT = 4;
  public static int EXPECT_NULLTRING_INOUTPUT = 8;
  public static int DEFAULT = 1;
  
  public CommandOutputValidator(TestableGfsh shell, Object[] output){
    this(shell,output,DEFAULT);
  }
  
  public CommandOutputValidator(TestableGfsh shell, Object[] output, int errorStrings) {
    this.shell = shell;
    Map<String, List> completorOutput = (Map<String, List>) output[1];
    Map<String, Object> commandOutput = (Map<String, Object>) output[0];
    presentationStr = (String) output[2];

    HydraUtil.logInfo("Command Output : " + commandOutput);
    
    for (Object key : commandOutput.keySet()) {
      Object value = commandOutput.get(key);
      if (value instanceof CommandResult) {
        this.result = (CommandResult) value;
        this.command = (String) key;
        HydraUtil.logInfo("Adding validator for command : " + command);
      }
    }
    
    stringValidator = new StringValidator(errorStrings);

    /*for (String s : EXCEPTION)
      this.errorStringPatterns.add(s);
    for (String s : ERROR)
      this.errorStringPatterns.add(s);
    for (String s : NULL)
      this.errorStringPatterns.add(s);*/
  }

  public CommandOutputValidator addUnExpectedErrorString(String str) {
    //errorStringPatterns.add(str);
    stringValidator.dontExpectString(str);
    return this;
  }

  public CommandOutputValidator addExpectedString(String str) {
    stringValidator.expectString(str);
    return this;
  }

  public CommandOutputValidatorResult validate() {
    CommandOutputValidatorResult validationResult = new CommandOutputValidatorResult();
    validationResult.command = this.command;
    validationResult.result = true;
    List<String> stringerrors = stringValidator.validate(presentationStr);
    
    if(stringerrors.size()>0){
      validationResult.result = false;
      validationResult.errorStrings.addAll(stringerrors);
    }
    
//    checkStrings(errorStringPatterns, validationResult, false);
//    checkStrings(stringPatterns, validationResult, true);
    if(!result.getStatus().equals(Result.Status.OK)){
      validationResult.result = false;
      validationResult.errorStrings.add("Result status is not OK : " + result.getStatus());
    }    
    return validationResult;
  }

  private void checkStrings(List<String> stringPatterns, CommandOutputValidatorResult validationResult,
      boolean patternExpected) {
    int violations = 0;
    for(String s : stringPatterns){
      if(patternExpected){
        if(!presentationStr.contains(s)){
          validationResult.errorStrings.add(s);
          violations++;
        }
      }else{
        if(presentationStr.contains(s)){
          validationResult.errorStrings.add(s);
          violations++;
        }
      }
    }
    if(violations>0)
      validationResult.result = false;
  }
  
public static class StringPattern{
    
    public StringPattern(String str,boolean expected) {
      this.expected = expected;
      String finalPattern = null;
      if(str.contains("{0}")){
        finalPattern = str.replace("{0}", "\\w+");
        isRegex = true;
      }
      
      if(str.contains("{1}")){
        finalPattern = finalPattern.replace("{1}", "\\w+");
        isRegex = true;
      }
      
      if(str.contains("{2}")){
        finalPattern = finalPattern.replace("{2}", "\\w+");
        isRegex = true;
      }
      if(finalPattern==null){
        finalPattern = str;
        isRegex = false;        
      }
      patternString = finalPattern;
      originalString = str;
    }
    
    public boolean expected;
    public String originalString;
    public String patternString;
    public boolean isRegex;
    
    static boolean match(String template,String s){
      Pattern memberPattern = Pattern.compile(template);
      Matcher match = memberPattern.matcher(s);
      return match.find();
    }
    
    static boolean contains(String template,String s){
      String a[] = s.split(template);
      return a.length==0 || a.length==2;
    }
    
  }
  
  public static class StringValidator{
    
    private static final String EXCEPTION[] = { "Exception", "EXCEPTION", "exception" };
    private static final String ERROR[] = { "error", "Error", "ERROR" };
    private static final String NULL[] = { "null", "Null", "NULL" };
    
    private List<StringPattern> patternList = new ArrayList<StringPattern>();
    public String matchAgainst = null;
    
    
    public static boolean configured(int option, int specification){
      int result = specification & (option&0xffffffff);
      return (result==specification);
    }
    
    public StringValidator(int strings){
      boolean allowExceptionStrings = false;
      boolean allowErrorStrings = false;
      boolean allowNullStrings = false;
      if(configured(strings, DEFAULT)){
        allowExceptionStrings = false;
        allowErrorStrings = false;
        allowNullStrings = false;
      }else{
        if (configured(strings, EXPECT_ERRORSTRING_INOUTPUT)) {
          //System.out.println("Configured to allow error strings ");
          allowErrorStrings = true;
        }
        if (configured(strings, EXPECT_EXCEPTIONSTRING_INOUTPUT)) {
          //System.out.println("Configured to allow exception strings ");
          allowExceptionStrings = true;
        }
        if (configured(strings, EXPECT_NULLTRING_INOUTPUT)) {
          //System.out.println("Configured to allow null strings ");
          allowNullStrings = true;
        }
      }
      if(!allowExceptionStrings){
       // System.out.println("Adding exceptions to not allowed strings ");
        for(String s : EXCEPTION)
          dontExpectString(s);
      }
      if(!allowErrorStrings){
        //System.out.println("Adding ERROR to not allowed strings ");
        for(String s : ERROR)
          dontExpectString(s);
      }
      if(!allowNullStrings){
        //System.out.println("Adding NULL to not allowed strings ");
        for(String s : NULL)
          dontExpectString(s);
      }
    }
    
    public StringValidator expectString(String s){
      patternList.add(new StringPattern(s,true));
      return this;
    }
    
    public StringValidator dontExpectString(String s){
      patternList.add(new StringPattern(s,false));
      return this;
    }
    
    public List<String> validate(String output){
      List<String> list = new ArrayList<String>();
      for(StringPattern p : patternList){
        if(p.expected && !p.contains(p.patternString, output)){
          System.out.println("For expected string <" + p.patternString + "> : adding for " + output);
          list.add(p.originalString);
        }
        
        if(!p.expected && p.contains(p.patternString, output)){
          //System.out.println("For not expected string <" + p.patternString + "> : adding for " + output);
          list.add(p.originalString);
        }
      }
      return list;
    }    
  }  
  
public static void main(String[] args) {
    
    StringValidator validator = new StringValidator(CommandOutputValidator.DEFAULT);
    
    
    String patterns[] ={ 
        "Command executed successfully",
        "Command {0} executed successfully",
        "Command {0} executed successfully in {1} ms.",
        "Cannot create directory - {0}"
    };
    
    String messages[] ={ 
        "Command executed successfully",
        "Command netstat executed successfully",
        "Command netstat executed successfully in 3214324 ms.",
        "\n\n\n  s;kg;lfdsg Cannot create directory - mydirectory \n\n skgf \n",
        "\n\t\tn Exception \n\tError \n  Null \n",
    };
    
    //match exact string
    validator.expectString(patterns[0]);
    System.out.println("Match for(" + patterns[0] +") : true) : " + validator.validate(messages[0]));
    System.out.println();
    
    //expected right kind of string
    validator = new StringValidator(CommandOutputValidator.DEFAULT);validator.expectString(patterns[1]);
    System.out.println("Match for(" + patterns[1] +") : true) : " + validator.validate(messages[1]));
    System.out.println();
    
    //expected right kind of string with two params
    validator = new StringValidator(CommandOutputValidator.DEFAULT);validator.expectString(patterns[2]);
    System.out.println("Match for(" + patterns[2] +") : true) : " + validator.validate(messages[2]));
    System.out.println();
    
    //not-expected string with one params
    validator = new StringValidator(CommandOutputValidator.DEFAULT);validator.dontExpectString(patterns[3]);
    System.out.println("Match for(" + patterns[3] +") : true) : " + validator.validate(messages[3])); 
    System.out.println();
    
    //generic check for error and exception strings
    validator = new StringValidator(CommandOutputValidator.DEFAULT);
    System.out.println("Match for(default) : true) : " + validator.validate(messages[4])); 
    System.out.println();
    
    //generic check for error and exception strings : allow error
    validator = new StringValidator(CommandOutputValidator.EXPECT_ERRORSTRING_INOUTPUT);
    System.out.println("Match for(allow error) : true) : " + validator.validate(messages[4])); 
    System.out.println();
    
    //generic check for error and exception strings : allow exception
    validator = new StringValidator(CommandOutputValidator.EXPECT_EXCEPTIONSTRING_INOUTPUT);
    System.out.println("Match for(allow exception) : true) : " + validator.validate(messages[4])); 
    System.out.println();
    
    //generic check for error and exception strings : allow null
    validator = new StringValidator(CommandOutputValidator.EXPECT_NULLTRING_INOUTPUT);
    System.out.println("Match for(allow null) : true) : " + validator.validate(messages[4])); 
    System.out.println();
    
    //generic check for error and exception strings : allow null, exception
    validator = new StringValidator(CommandOutputValidator.EXPECT_NULLTRING_INOUTPUT
        | CommandOutputValidator.EXPECT_EXCEPTIONSTRING_INOUTPUT);
    System.out.println("Match for(allow null, exception) : true) : " + validator.validate(messages[4])); 
    System.out.println();
    
    //generic check for error and exception strings : allow all
    validator = new StringValidator(CommandOutputValidator.EXPECT_NULLTRING_INOUTPUT 
        | CommandOutputValidator.EXPECT_EXCEPTIONSTRING_INOUTPUT 
        | CommandOutputValidator.EXPECT_ERRORSTRING_INOUTPUT);
    System.out.println("Match for(allow null, exception,error) : true) : " + validator.validate(messages[4])); 

  }

}
