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

/**
 * Class to create an XML report suitable for cruisecontrol for a test run.
 */
package batterytest;

import hydra.FileUtil;
import hydra.HostHelper;
import hydra.HydraRuntimeException;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class XMLReport {

  private static final String DIVIDER = "--------------------";
  private static final char SLASH = '/';
  private static final char DOT = '.';

  /**
   * Generate an XML report with information required by cruisecontrol.
   */
  public static void createReport(String testName, String testProps,
                                  String localConf, String testDir,
                                  String btFile)
  throws IOException, NumberFormatException {
    // create the xml reports directory, if needed
    String xmlDir = "xml-reports";
    if (!FileUtil.exists(xmlDir)) {
      FileUtil.mkdir(xmlDir);
    }

    String testNameTmp = testName.replace(SLASH, DOT).substring(0, testName.lastIndexOf("."));
    String fn = "xml-reports/TEST-" + testNameTmp + ".xml";
    boolean isNewReport = !FileUtil.exists(fn);

    String err = getFirstError(testDir);
    try {
      DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = dbf.newDocumentBuilder();
      Document doc;
      if (isNewReport) {
        //Create an empty document with no tests.
        doc = builder.newDocument();

        Element suite = doc.createElement("testsuite");
        suite.setAttribute("failures", "0");
        suite.setAttribute("errors", "0");
        suite.setAttribute("hostname", HostHelper.getLocalHost());
        suite.setAttribute("name", FileUtil.filenameFor(btFile));
        suite.setAttribute("tests", "0");
        suite.appendChild(doc.createElement("properties"));
        doc.appendChild(suite);
      } else {
        //Parse an existing document
        doc = builder.parse(new File(fn));
      }

      NodeList suites = doc.getElementsByTagName("testsuite");
      Element suite = (Element) suites.item(0);
      
      //Increment the test count
      incrementAttribute(suite, "tests");

      //Add a testcase
      Element testcase = doc.createElement("testcase");
      suite.appendChild(testcase);
      testcase.setAttribute("name", testName + " " + testProps);

      if(err != null) {
        incrementAttribute(suite, "errors");
        Element error = doc.createElement("error");
        testcase.appendChild(error);
        error.setAttribute("type", "");
        error.setAttribute("message", "");
        String message = new StringBuilder("RESULT DIR ")
        .append(FileUtil.filenameFor(testDir))
        .append("\n").append(err).append("    ").toString();
        error.setTextContent(message);
      }
    
    
      TransformerFactory tFactory =
          TransformerFactory.newInstance();
      Transformer transformer;
      transformer = tFactory.newTransformer();

      DOMSource source = new DOMSource(doc);
      BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(fn));
      try {
        StreamResult result = new StreamResult(stream);
        transformer.transform(source, result);
      } finally {
        stream.close();
      }
    } catch (TransformerConfigurationException e) {
      throw new HydraRuntimeException( "Unable to write to file: " + fn, e );
    } catch (TransformerException e) {
      throw new HydraRuntimeException( "Unable to write to file: " + fn, e );
    } catch (ParserConfigurationException e) {
      throw new HydraRuntimeException( "Unable to write to file: " + fn, e );
    } catch (SAXException e) {
      throw new HydraRuntimeException( "Unable to write to file: " + fn, e );
    }
  }

  /**
   * Increments the named attribute on the xml element by 1.
   */
  private static void incrementAttribute(Element suite, String name) {
    Attr attribute = suite.getAttributeNode(name);
    String text = attribute.getValue();
    int current = Integer.parseInt(text);
    suite.setAttribute(name, Integer.toString(current + 1));
  }

  /**
   * Returns a string containing the first error reported in the errors.txt
   * file, if any, found in the given test directory. Else returns null.
   */
  private static String getFirstError(String testDir) throws IOException {
    String fn = testDir + File.separator + "errors.txt";
    if (FileUtil.exists(fn)) {
      StringBuffer buf = new StringBuffer();
      BufferedReader br = new BufferedReader(new FileReader(fn));
      int count = 0;
      String line;
      while ((line = br.readLine()) != null && !line.startsWith(DIVIDER)) {
        if (count == 20) {
          buf.append("&lt;snip&gt;\n");
          break;
        }
        line = line.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&apos;");
        if (line.length() > 200) {
          buf.append(line.substring(0, 200)).append("&lt;snip&gt;\n");
        } else {
          buf.append(line).append("\n");
        }
        ++count;
      }
      br.close();
      return buf.toString();
    } else {
      return null;
    }
  }
}
