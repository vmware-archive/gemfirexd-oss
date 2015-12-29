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
package s2qa;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/*
 * Parses a junit output summary xml file.  The easiest way to generate
 * this file is by running junit with ant, with printsummary="on", and then
 * running the junitreport task.
 */
public class JUnitXMLParser {
  Document dom;
  File xmlFile = null;
  public JUnitResultData parseXmlFile(File xmlFile){
    this.xmlFile = xmlFile;
    //get the factory
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    JUnitResultData returnResultsForFile = null;
    try {

      //Using factory get an instance of document builder
      DocumentBuilder db = dbf.newDocumentBuilder();

      //parse using builder to get DOM representation of the XML file
      dom = db.parse(xmlFile);
      returnResultsForFile = parseDocument();
      returnResultsForFile.setJunitResultfile(xmlFile);

    }catch(ParserConfigurationException pce) {
      pce.printStackTrace();
    }catch(SAXException se) {
      se.printStackTrace();
    }catch(IOException ioe) {
      ioe.printStackTrace();
    }
    return returnResultsForFile;
  }
  private JUnitResultData parseDocument(){
    //get the root element
    Element docEle = dom.getDocumentElement();
    ArrayList<JUnitResult> resultList = new ArrayList<JUnitResult>();
    //get a nodelist of testcases
    NodeList testcaseNodes = docEle.getElementsByTagName("testcase");
    if(testcaseNodes != null && testcaseNodes.getLength() > 0) {
      for(int i = 0 ; i < testcaseNodes.getLength();i++) {

        //get the employee element
        Element el = (Element)testcaseNodes.item(i);

        JUnitResult testResult = getJUnitResult(el);
        resultList.add(testResult);
      }
    }
    JUnitResultData resultsForFile = new JUnitResultData();
    resultsForFile.setResults(resultList);
    return resultsForFile;
  }
  private JUnitResult getJUnitResult(Element junitElement) {
    JUnitResult res = new JUnitResult();
    res.setTestclassName(junitElement.getAttribute("classname"));
    res.setTestcaseName(junitElement.getAttribute("name"));
    res.deriveScopedTestcaseName();
    res.setFromFile(xmlFile);
    if (junitElement.hasChildNodes()) {
      // JUnit distinguishes errors (random execution issues) from
      // failures (identified by test, e.g. failed assertions),
      // but we really don't care about the difference...
      String failureTypes[] = {"error","failure"};
      for (int ft=0; ft<failureTypes.length; ft++) {
        NodeList errfails = junitElement.getElementsByTagName(failureTypes[ft]);
        for (int i=0; i<errfails.getLength(); i++) {
          Element errfail = (Element) errfails.item(i);
          res.setFailed(true);
          res.setErrString(errfail.getAttribute("type")+": "+errfail.getAttribute("message"));
          res.setOriginalIndexNumber(i);
          res.setTraceback(errfail.getTextContent());
          if (ft == 1) {
            // probably don't need to track this at all... (fails vs. errors)
            res.setInFailureContext(true);
          }
        }
      }
    }
    return res;
  }
}
