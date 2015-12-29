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
package haOverflow;

import java.io.File;
import java.io.PrintStream;

import com.gemstone.gemfire.internal.ProcessOutputReader;

/**
 * This class is a command-line application that functions as a Durable Client
 * Manager in the system and allows the user to run, verify and experiment with
 * the HA Overflow features
 * 
 * @author GemStone Systems, Inc.
 * @since 5.7
 */
public class HADurableClientMgr {

  /**
   * Prints information on how this program should be used.
   */
  static void showHelp() {
    PrintStream out = System.out;

    out.println();
    out
        .println("A distributed system is created with properties loaded from your ");
    out
        .println("  gemfire.properties file.  You *should* specify alternative property");
    out
        .println("  files using -DgemfirePropertyFile=client_gemfire.properties");
    out
        .println("The declarative XML to use for building the cache can by default be");
    out.println("  client.xml");
    out.println("  ");
    out.println("Usage: java -DgemfirePropertyFile=gemfire.properties");
    out.println("            HADurableClient <cache.xml> ");
    out.println();
  }

  /**
   * Parses the command line and runs the <code>HACacheServer</code> example.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      showHelp();
      System.exit(1);
    }
    String xmlFileName = args[0];

    File xmlFile = new File(xmlFileName);
    if (!xmlFile.exists()) {
      System.err
          .println("Supplied Cache config file <cache.xml> does not exist");
      System.exit(1);

    }

    String cmdArgs = " -classpath " + System.getProperty("java.class.path");
    String exDir = xmlFile.getParent();
    if (exDir == null)
      exDir = "";
    else
      exDir = exDir + File.separator;

    System.out.println("");
    // Fork off a new Durable Client & exit abnormally
    Process client0 = Runtime.getRuntime().exec(
        "java " + cmdArgs + " -DgemfirePropertyFile=" + exDir
            + "durable_client_gemfire.properties"
            + "  haOverflow.HADurableClient " + args[0] + " false");
    System.out
        .println("Started a Durable Client, Gemfire logs go to durableClient.log ");
    ProcessOutputReader reader = new ProcessOutputReader(client0);
    String output = reader.getOutput();
    System.out.println(output);
    System.out
        .println("Disconnected the Durable Client, keepalive for 5 mins ");
    System.out.println("");

    Process client1 = Runtime.getRuntime().exec(
        "java " + cmdArgs + " -DgemfirePropertyFile=" + exDir
            + "client_gemfire.properties"
            + "  haOverflow.HAFeederClient " + args[0]);
    System.out
        .println("Started a Feeder Client to add data, Gemfire logs go to feederClient.log ");
    reader = new ProcessOutputReader(client1);
    output = reader.getOutput();
    System.out.println(output);
    System.out.println("Disconnected Feeder Client ");
    System.out.println("");

    File backupDir = new File(System.getProperty("user.dir") + File.separator
        + "backupDirectory");
    System.out.println("=============== Contents of server overflow Dir ");
    listFiles(backupDir, 0);
    System.out.println("");

    // Fork off client2
    Process client2 = Runtime.getRuntime().exec(
        "java " + cmdArgs + " -DgemfirePropertyFile=" + exDir
            + "durable_client_gemfire.properties"
            + "  haOverflow.HADurableClient " + args[0] + " true");
    System.out
        .println("Re-Started Durable Client that should have events queue, Gemfire logs go to durableClient.log.");
    System.out
        .println("The previous durableClient.log is rolled to durableClient-01-00.log");
    reader = new ProcessOutputReader(client2);
    output = reader.getOutput();
    System.out.println(output);

    System.exit(0);
  }

  static void listFiles(File dir, int level) {
    if (dir.isDirectory()) {
      for (int i = 0; i < level; i++) {
        System.out.print(" ");
      }
      System.out.println(dir.getPath());

      File[] listing = dir.listFiles();
      for (int i = 0; i < listing.length; i++) {
        listFiles(listing[i], level + 1);
      }
    }
    else {
      for (int i = 0; i < level; i++) {
        System.out.print(" ");
      }
      System.out.println(dir.getName());
    }
  }
}
