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

package hydra;

import com.gemstone.gemfire.LogWriter;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/** 
 * Manages the VM running GFMonMgr and WindowTester.
 */

public class GFMonMgr {

  protected static final String GFMON_DIR = "gfmon";
  private static int PID = -1;

  /**
   * Starts the GFMon/WindowTester VM and waits for it to live.
   *
   * @throws HydraTimeoutException if the VM does not start within {@link
   *                               GFMonPrms#maxStartupWaitSec} seconds.
   */
  protected static void start() {
    if (GFMonPrms.getTestClassName() != null)
    {
      // set preferences
      log().info("Setting preferences for GFMon/WindowTester VM..." );
      try {
        setCurrentPreferences();
      } catch (BackingStoreException e) {
        throw new HydraRuntimeException("While setting preferences", e);
      }
      log().info("Set preferences for GFMon/WindowTester VM..." );

      // start vm
      log().info("Starting GFMon/WindowTester VM..." );
      PID = Java.javaGFMon();

      // wait for vm to start
      int maxWaitSec = GFMonPrms.getMaxStartupWaitSec();
      log().info("Waiting " + maxWaitSec + " seconds for process with pid="
                + PID + " to start...");
      if (ProcessMgr.waitForLife(HostHelper.getLocalHost(), PID, maxWaitSec))
      {
        log().info("Started GFMon/WindowTester VM with pid="  + PID);
      }
      else
      {
        String s = "Failed to start GFMon/WindowTester VM with pid=" + PID
                 + " within " + maxWaitSec + " seconds.";
        throw new HydraTimeoutException(s);
      }
    }
  }

  /**
   * Waits for the GFMon/WindowTester VM stop.  Reports the test outcome.
   *
   * @throws HydraTimeoutException if the VM does not stop within {@link
   *                               GFMonPrms#maxShutdownWaitSec} seconds.
   */
  protected static void waitForStop() {
    if (PID != -1)
    {
      log().info("Waiting for GFMon/WindowTester VM pid=" + PID
                + " to stop..." );
      int maxWaitSec = GFMonPrms.getMaxShutdownWaitSec();
      log().info("Waiting " + maxWaitSec + " seconds for process with pid="
                + PID + " to stop...");
      if (ProcessMgr.waitForDeath(HostHelper.getLocalHost(), PID, maxWaitSec))
      {
        log().info("GFMon/WindowTester VM with pid="  + PID + " has stopped");
        HostDescription hd = TestConfig.getInstance().getMasterDescription()
                                       .getVmDescription().getHostDescription();
        Nuker.getInstance().removePID(hd, PID);
        String resultFileName = System.getProperty("user.dir") + File.separator
                              + "wintest/" + GFMonPrms.getTestClassName()
                              + "-result.xml";
        parseXml(resultFileName, PID);
        PID = -1;
      }
      else
      {
        String s = "GFMon/WindowTester VM with pid=" + PID
                 + " failed to stop within " + maxWaitSec + " seconds.";
        throw new HydraTimeoutException(s);
      }
    }
  }

  /**
   * Set the current user preferences for this test run.
   */
  private static void setCurrentPreferences()
  throws BackingStoreException
  {
    String currDir = System.getProperty("user.dir");

    // look up current user preferences, which needs forward slashes
    String modDir = currDir.replace('\\', '/');
    Preferences currentUser =
      Preferences.userRoot().node("GemFire Monitor 2.0").node(modDir);

    // override the log directory, which needs backslashes
    currentUser.put("prefs_dir", currDir + File.separator + GFMON_DIR);

    // make it write them out right away
    currentUser.flush();
    currentUser.sync();
    log().info("Set current preferences: " + preferencesToString(currentUser));
  }

  /**
   * Returns the preferences with key-value pairs as a string.
   */
  private static String preferencesToString(Preferences prefs)
  throws BackingStoreException
  {
    StringBuffer buf = new StringBuffer();
    buf.append(prefs);
    String[] keys = prefs.keys();
    for (int i = 0; i < keys.length; i++) {
      buf.append("\n" + keys[i] + "=" + prefs.get(keys[i], null));
    }
    return buf.toString();
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }

  /**
   * Parses the WindowTester XML result file and writes error files as needed.
   */
  public static void parseXml(String fn, int pid) {
    File f = new File(fn);
    SAXParser parser = null;
    try {
      parser = SAXParserFactory.newInstance().newSAXParser();
    } catch (javax.xml.parsers.ParserConfigurationException e) {
      throw new HydraRuntimeException("While creating SAX parser", e);
    } catch (SAXException e) {
      throw new HydraRuntimeException("While creating SAX parser", e);
    }
    XMLHandler handler = new XMLHandler(pid);
    try {
      parser.parse(f, handler);
    } catch (HydraRuntimeException e) {
      throw new HydraRuntimeException("While parsing " + f, e);
    } catch (SAXException e) {
      throw new HydraRuntimeException("While parsing " + f, e);
    } catch (java.io.IOException e) {
      throw new HydraRuntimeException("While parsing " + f, e);
    }
  }

  static class XMLHandler extends DefaultHandler {

    private List stack = null; // for accumulating stack traces
    int pid = -1;              // pid of windowtester/gfmon vm

    public XMLHandler(int pid) {
      this.pid = pid;
    }
    public InputSource resolveEntity(String publicId, String systemId)
    throws SAXException {
      return this.resolveEntity(publicId, systemId);
    }
    public void setDocumentLocator(Locator locator) {
    }
    public void startDocument() throws SAXException {
    }
    public void endDocument() throws SAXException {
    }
    public void startPrefixMapping(String prefix, String uri)
    throws SAXException {
    }
    public void endPrefixMapping(String prefix) throws SAXException {
    }
    public void startError(Attributes attributes) {
      if (stack == null) {
        stack = new ArrayList();
      }
      for (int i = 0; i < attributes.getLength(); i++) {
        String qName = attributes.getQName(i);
        if (qName.equals("type")) {
          String s = attributes.getValue(i);
          stack.add(s + ": ");
        }
      }
      for (int i = 0; i < attributes.getLength(); i++) {
        String qName = attributes.getQName(i);
        if (qName.equals("message")) {
          String s = attributes.getValue(i);
          stack.add(s + "\n\n");
        }
      }
    }
    public void startFailure(Attributes attributes) {
      if (stack == null) {
        stack = new ArrayList();
      }
      for (int i = 0; i < attributes.getLength(); i++) {
        String qName = attributes.getQName(i);
        if (qName.equals("type")) {
          String s = attributes.getValue(i);
          stack.add(s + ": ");
        }
      }
      for (int i = 0; i < attributes.getLength(); i++) {
        String qName = attributes.getQName(i);
        if (qName.equals("message")) {
          String s = attributes.getValue(i);
          stack.add(s + "\n\n");
        }
      }
    }
    public void startTestSuite(Attributes attributes) {
      String errors = attributes.getValue("errors");
      String failures = attributes.getValue("failures");
      String name = attributes.getValue("name");
      String report = "WINDOWTESTER/GFMON REPORT for"
                    + " " + name + ":"
                    + " errors=" + errors
                    + " failures=" + failures;
      Log.getLogWriter().info(report);
    }
    public void startElement(String uri, String localName, String qName,
                             Attributes attributes) throws SAXException {
      if (qName.equals("testsuite")) {
        startTestSuite(attributes);
      } else if (qName.equals("error")) {
        startError(attributes);
      } else if (qName.equals("failure")) {
        startFailure(attributes);
      }
    }
    public void endElement(String uri, String localName, String qName)
    throws SAXException {
      if (stack != null) {
        processStack(qName);
      }
    }
    private void processStack(String qName) {
      StringBuffer buf = new StringBuffer();
      if (stack.size() == 0) {
        buf.append("no information available");
      } else {
        for (Iterator i = stack.iterator(); i.hasNext();) {
          buf.append((String)i.next());
        }
      }
      if (qName.equals("error")) {
        ResultLogger.reportErr(this.pid, buf.toString());
      } else if (qName.equals("failure")) {
        ResultLogger.reportHang(this.pid, buf.toString());
      } else {
        ResultLogger.reportHang(this.pid, qName + ": " + buf.toString());
      }
      stack = null;
    }
    public void characters(char[] ch, int start, int length)
    throws SAXException {
      if (stack != null) {
        StringBuffer buf = new StringBuffer();
        buf.append(ch, start, length);
        stack.add(buf.toString());
      }
    }
    public void ignorableWhitespace(char[] ch, int start, int length)
    throws SAXException {
    }
    public void processingInstruction(String target, String data)
    throws SAXException {
    }
    public void skippedEntity(String name) throws SAXException {
    }
    public void warning(SAXParseException e) throws SAXException {
      Log.getLogWriter().warning("While parsing", e);
    }
    public void error(SAXParseException e) throws SAXException {
      throw e;
    }
    public void fatalError(SAXParseException e) throws SAXException {
      throw e;
    }
  }
}
