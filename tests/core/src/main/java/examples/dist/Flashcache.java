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

package examples.dist;

import com.gemstone.gemfire.SystemFailure;

import hydra.*;
import util.*;
import java.io.*;
import flashcache.Quote;
import java.util.*;

public class Flashcache {

private static Quote quoteInstance = null;

/** 
 * Hydra task to initialize.  Creates and initializes an instance of
 * the {@link Quote} example class.
 */
public synchronized static void HydraTask_initTask() {
   if (quoteInstance == null) {
      quoteInstance = new Quote();
      try {
         quoteInstance.initializeCache(CacheUtil.createCache(), new FlashListener());
      } catch (Exception e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
}
   
/** 
 * Hydra task to look up a quote using the examples.Quote API.  After
 * the quote has been fetched from NASDAQ, the quote is {@linkplain
 * #validateQuotes validated} to make sure that the output of the
 * example is reasonable. When then quickly refetch the quote to make
 * sure it hasn't changed.  Finally, we want for the TTL to expire,
 * refetch the quote, and revalidate it.
 */
public static void HydraTask_quoteTask() {
   long numInvalidates = EventCountersBB.getBB().getSharedCounters().read(
                         EventCountersBB.numAfterInvalidateEvents_isExp);

   // Get a quote and validate it
   String symbol = TestConfig.tab().stringAt(FlashcachePrms.symbol);
   String quote = null;
   try {
      quote = quoteInstance.getQuote(symbol);
   } 
   catch (VirtualMachineError e) {
     SystemFailure.initiateFailure(e);
     throw e;
   }
   catch (Throwable e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().info("Quote for " + symbol + ": " + quote);
   validateQuote(symbol, quote);
   
   FlashcacheBB.getBB().getSharedCounters().increment(FlashcacheBB.NUM_GET_QUOTE);

   // get it again before the TTL expires it
   String hitQuote = null;
   try {
      hitQuote = quoteInstance.getQuote(symbol);
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   if (!quote.equals(hitQuote)) {
      throw new TestException("Expected first getQuote " + quote + " to be equal to second getQuote " + hitQuote);
   }

   // wait for the TTL to expire it
   long expectedNumInvalidates = numInvalidates + 1;
   TestHelper.waitForCounter(EventCountersBB.getBB(), "EventCountersBB.numAfterInvalidateEvents_isExp", 
       EventCountersBB.numAfterInvalidateEvents_isExp, expectedNumInvalidates, true, 60000, 2000);

   // fetch it again; now it's a cache miss
   String missQuote = null;
   try {
      missQuote = quoteInstance.getQuote(symbol);
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   validateQuote(symbol, missQuote);

   // wait for the TTL to expire it again
   expectedNumInvalidates++;
   TestHelper.waitForCounter(EventCountersBB.getBB(), "EventCountersBB.numAfterInvalidateEvents_isExp", 
       EventCountersBB.numAfterInvalidateEvents_isExp, expectedNumInvalidates, true, 60000, 2000);
}

/** 
 * Hydra task to look up a quote using the examples.Quote main.  
 */
public static void HydraTask_mainQuoteTask() throws IOException {
   // write the input to the Quote.main class to a file
   final int NUM_QUOTES = 5;
   String inputFileName = "mainInput.txt";
   String inputStr = "";
   ArrayList inputStrList = new ArrayList();
   for (int i = 1; i <= NUM_QUOTES; i++) {
      String symbol = TestConfig.tab().stringAt(FlashcachePrms.symbol);
      inputStr += symbol;
      inputStr += "\n";
      inputStrList.add(symbol);
   }
   inputStr += "\n";
   File aFile = new File(inputFileName);
   FileWriter fileWriter = new FileWriter(aFile);
   fileWriter.write(inputStr);
   fileWriter.close();
   Log.getLogWriter().info("Input to main is " + inputStr);

   // execute the Quote.main class 
   String classpath = System.getProperty("gemfire.home") + File.separator + "examples" +
          File.separator + "dist" + File.separator + "classes" + System.getProperty("path.separator") +
          System.getProperty("java.class.path");
   List cmd = new ArrayList();
   if (System.getProperty("os.name").indexOf("Windows") == -1) {
     cmd.add("bash");
     cmd.add("--norc");
     cmd.add("-c");
   } else {
     cmd.add("cmd");
     cmd.add("/c");
   }
   StringBuffer buf = new StringBuffer();
   buf.append(System.getProperty("java.home"))
      .append(File.separator).append("bin")
      .append(File.separator).append("java");
   buf.append(" -Xms256m -Xmx256m");
   buf.append(" -classpath ").append(classpath);
   buf.append(" flashcache.Quote");
   buf.append(" < mainInput.txt > mainOutput.txt");
   cmd.add(buf.toString());

   String[] cmdStrings = (String[]) cmd.toArray( new String[0] );
   Log.getLogWriter().info("cmd is " + buf.toString());
   int maxWaitSec = TestConfig.tab().intAt(Prms.maxResultWaitSec) - 10;
   String output = hydra.ProcessMgr.fgexec(cmdStrings, maxWaitSec);
   Log.getLogWriter().info("output is " + output);
   if (output.contains("xception") || output.contains("timed out")) {
      throw new TestException("While trying to get quote, got " + output);
   }

   // read the output from running Quote.main
   String outputFileName = "mainOutput.txt";
   FileReader reader = new FileReader(outputFileName);
   BufferedReader bReader = new BufferedReader(reader);
   String line = bReader.readLine();
   if (line == null)
      throw new TestException("No output from running " + cmd);
   String total = "";
   ArrayList outputStrList = new ArrayList();
   while (line != null) {
      total += line;
      outputStrList.add(line);
      line = bReader.readLine();
   }
   Log.getLogWriter().info("Total output *" + total + "*");
   validateQuotes(inputStrList, outputStrList);
}

/** Validate quote.
 *
 *  @param symbol A symbol used to get a quote.
 *  @param quote A list of strings, each one is an output line
 *         for getting a quote (there may be up to 3 output lines per symbol).
 */
private static void validateQuote(String symbol, String quote) {
   ArrayList symbolList = new ArrayList();
   symbolList.add(symbol);
   ArrayList quoteList = new ArrayList();
   quoteList.add(quote);
   validateQuotes(symbolList, quoteList);
}

/** Validate quotes.
 *
 *  @param inputStrList A List of symbols used to get quotes.
 *  @param outputStrList A list of strings, each one is an output line
 *         for getting a quote (there may be up to 3 output lines per symbol).
 *  For regular expressions
 *     (.++) means any char one or more times
 *     (\\.) means the literal char '.'
 *     ([0-9]+) means any digit, one or more times
 *     (-?) means the char '-', 0 or 1 times
 *     ([0-9]+) means any digit one or more times
 *     ([0-9]{1,3}) means any digit at least once but no more than 3 times
 *     ((,([0-9]{3}))*?) means a ',' followed by any digit 3 times, and this whole thing
 *                       can be repeated zero or more times
 */
private static void validateQuotes(List inputStrList, List outputStrList) {
   int outputStrListSize = outputStrList.size();
   final String outputLine1 = "(QuoteLoader netSearching for ";
   final String outputLine2 = "(QuoteLoader querying nasdaq for ";
   final String regExForOutputLine3 = 
                "last sale=([0-9]+)((\\.)([0-9]+))?" +
                "  net change=(((-|\\+)([0-9]+)((\\.)([0-9]+))?)|unch)" +
                "  volume=([0-9]{1,3})((,([0-9]{3}))*)";
   final String enter1 = "Enter symbol: ";
   final String enter2 = "Enter next symbol: ";
   StringBuffer inputSB = new StringBuffer();
   inputSB.append("Input:");
   for (int i = 0; i < inputStrList.size(); i++)
      inputSB.append("\n   " + inputStrList.get(i));
   StringBuffer outputSB = new StringBuffer();
   outputSB.append("Output:");
   for (int i = 0; i < outputStrList.size(); i++)
      outputSB.append("\n   " + outputStrList.get(i));

   int outputListIndex = 0;
//   boolean firstOutputLine = true;
   for (int i = 0; i < inputStrList.size(); i++) {
      String symbol = (String)inputStrList.get(i);
      String regEx = symbol + ": " + regExForOutputLine3;

      // get first output line for this symbol
      if (outputListIndex >= outputStrListSize) 
         throw new TestException("No output for symbol " + symbol +
                   " see files mainInput.txt and mainOutput.txt");
      String outputLine = (String)outputStrList.get(outputListIndex);   
      outputListIndex++;
      if (outputLine == null)
         throw new TestException("Quote returned null for stock symbol " + symbol + 
               "; probably timed out waiting for nasdaq to respond, see bg*.log files to verify");
      if (outputLine.startsWith(enter1))
         outputLine = outputLine.substring(enter1.length(), outputLine.length());
      else if (outputLine.startsWith(enter2))
         outputLine = outputLine.substring(enter2.length(), outputLine.length());

      // check 1st output line; line 1 is optional
      if (outputLine.matches(regEx)) {
         // if we previously looked up this symbol, we won't see the searching
         // output lines for line1 and line2, all we get is line3. If we get here,
         // then we only got line3 and everything is fine.
         continue;
      } 
      String expectedStr = outputLine1 + symbol + ")";
      if (!outputLine.equals(expectedStr))
         throw new TestException("Unexpected 1st output line \"" + outputLine +
                   "\" for stock symbol " + symbol + ", expected \"" + expectedStr + "\"" +
                   "\n" + inputSB.toString() + "\n" + outputSB.toString());

      // check 2nd output line; line2 is optional
      if (outputListIndex >= outputStrListSize) 
         throw new TestException("No 2nd output line for symbol " + symbol +
                   "\n" + inputSB.toString() + "\n" + outputSB.toString());
      outputLine = (String)outputStrList.get(outputListIndex++);   
      if (outputLine.matches(regEx)) {
         // if we previously looked up this symbol, we won't see the searching
         // output line for line2, all we get is line3. If we get here,
         // then we got line3 and everything is fine.
         continue;
      } 
      expectedStr = outputLine2 + symbol + ")";
      if (!outputLine.equals(expectedStr))
         throw new TestException("Unexpected 2nd output line \"" + outputLine +
                   "\" for stock symbol " + symbol + ", expected \"" + expectedStr + "\"" +
                   "\n" + inputSB.toString() + "\n" + outputSB.toString());

      // output line 3
      if (outputListIndex >= outputStrListSize) 
         throw new TestException("No 3rd output line for symbol " + symbol +
                   " see files mainInput.txt and mainOutput.txt");
      outputLine = (String)outputStrList.get(outputListIndex++);   
      if (!outputLine.matches(regEx)) 
         throw new TestException("Unexpected 3rd output line \"" + outputLine + 
                   "\" for stock symbol " + symbol + 
                   "; does not match the regular expression " + regEx +
                   "\n" + inputSB.toString() + "\n" + outputSB.toString());
   }
}

/** Hydra end task */
public static void HydraTask_endTask() {
   FlashcacheBB.getBB().printSharedCounters();
   EventCountersBB.getBB().printSharedCounters();
}

}
