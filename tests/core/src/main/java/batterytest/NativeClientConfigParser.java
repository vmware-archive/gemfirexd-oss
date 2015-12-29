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

package batterytest;

import com.gemstone.gemfire.LogWriter;
import hydra.*;
import java.io.*;
import java.util.*;

/** 
 * Processes native client config files.
 */

public class NativeClientConfigParser {

  //////////////////////  Constructors  //////////////////////

  private NativeClientConfigParser() {
  }

  /**
   * Reads the full text of the given file.  Evaluates configuration properties.
   */
  private String parse(String fn, Vector p)
  throws FileNotFoundException {

    log().info("PASS 1a: getText: " + fn);
    String pass1 = getText(fn);
    //log().info("PASS 1: getText: " + fn + "\n" + pass1);

    log().info("PASS 2: expandSystemProperties: " + fn);
    String pass2 = expandSystemProperties(pass1, p);
    //log().info("PASS 2: expandSystemProperties: " + fn + "\n" + pass2);

    return pass2;
  }

//------------------------------------------------------------------------------
// TOKENIZER: PASS 1 (TEXT)
//------------------------------------------------------------------------------

  /**
   * Reads the file into a string.
   */
  private String getText( String fn ) throws FileNotFoundException {
    try {
      return FileUtil.getText( fn );
    } catch( FileNotFoundException e ) {
      throw e;
    } catch( IOException e ) {
      String s = "Error reading hydra configuration file " + fn;
      throw new HydraRuntimeException(s);
    }
  }

//------------------------------------------------------------------------------
// TOKENIZER: PASS 2 (SYSTEM PROPERTIES)
//------------------------------------------------------------------------------

  /**
   * Expands system properties in the given text.
   */
  private String expandSystemProperties(String text, Vector vp) {
    String tmp = text;
    if (vp != null) {
      for (Iterator i = vp.iterator(); i.hasNext();) {
        BatteryTestConfig.Property p = (BatteryTestConfig.Property)i.next();
        tmp = tmp.replace("${" + p.getKey() + "}", (String)p.getVal());
      }
      if (tmp.indexOf("${") != -1) {
        String s = "Missing properties for configuration variables:\n" + tmp; 
        throw new HydraConfigException(s);
      }
    }
    return tmp;
  }

//------------------------------------------------------------------------------
// STATIC METHODS
//------------------------------------------------------------------------------

  /**
   * Returns the log writer.
   */
  private static LogWriter log() {
    return Log.getLogWriter();
  }

  /**
   * Parses the native client config file using the properties.
   */
  protected static String parseFile(String fn, Vector p)
  throws FileNotFoundException {
    log().info("Parsing configuration file: " + fn);
    NativeClientConfigParser parser = new NativeClientConfigParser();
    String testxml = parser.parse(fn, p);
    log().info("Parsed configuration file: " + fn);
    return testxml;
  }

  /**
   * Parses the specified native client config file and writes it to .parsed.
   */
  private static void parseConfigFile(String fn) {
    int index = fn.indexOf( ".xml" );
    if ( index == -1 ) { // native client test
      String s = "Not an XML file: " + fn;
      throw new IllegalArgumentException(s);
    }

    // read properties, if any
    Vector pv = new Vector();
    String propFileName = fn.substring(0, index) + ".prop";
    try {
      FileInputStream fis = new FileInputStream(propFileName);
      Properties p = new Properties();
      p.load(fis);
      for (Enumeration names = p.propertyNames(); names.hasMoreElements();) {
        String name = (String)names.nextElement();
        pv.add(new BatteryTestConfig.Property(name, p.getProperty(name)));
      }
    } catch( FileNotFoundException e ) {
      // no property file provided
    } catch (IOException e) {
      String s = "Unable to load test properties from " + propFileName;
      throw new HydraConfigException(s);
    }

    // process file using properties
    log().info( "Parsing native client test XML file: " + fn + "..." );
    String result = null;
    try {
      result = parseFile(fn, pv);
    } catch (FileNotFoundException e) {
      String s = "Native client xml file not found: " + fn;
      throw new HydraConfigException(s, e);
    }
    System.out.println(result);
  }

  /**
   * Parses either all hydra configuration files or the single file
   * given as an argument.
   */
  public static void main( String args[] ) {
    Log.createLogWriter( "parser", "fine" );
    if (args.length == 1) {
      String fn = args[0];
      parseConfigFile(fn);
    } else {
      String s = "Usage: batterytest.NativeClientConfigParser <xmlFile>";
      throw new HydraRuntimeException(s);
    }
  }
}
