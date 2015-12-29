/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.tools.planexporter.CreateXMLFile

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.tools.planexporter;

import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.impl.services.uuid.BasicUUID;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil.XMLForms;

/**
 * This class is to create the final xml file, that will be used by the
 * Graphical Query Explainer. This is called from
 * com.pivotal.gemfirexd.internal.tools.PlanExporter.
 */
public final class CreateXML extends AbstractCreatePlan {

  public final static String defaultXML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

  // GemStone changes BEGIN
  // originally final static String comment =
  // "<!-- Apache Derby Query Explainer (DERBY-4587)-->\n";
  final static String comment = "<!-- GemFireXD Query Plan -->\n";

  // GemStone changes END
  final static String rootTagStart = "<root>\n";
  final static String rootTagEnd = "</root>\n";

  final static String parentTagStart = "<plan>\n";

  final static String parentTagEnd = "</plan>\n";

  final static String childTagStart = "<details>\n";

  final static String childTagEnd = "</details>\n";

  public CreateXML(AccessDistributedSystem access, boolean isRemote,
      XMLForms xmlForm, String embedXslFileName) {
    super(access, isRemote, xmlForm, embedXslFileName);
  }

  private void writeToStream(
      Writer out,
      String stmt,
      String time,
      String begin_end_exe_time, 
      boolean isLocalPlanExtracted) throws IOException {

    if (!isLocalPlanExtracted) {
      if (this.xmlForm == XMLForms.none) {
        writeXMLHeader(out, false);
      }
      out.write(parentTagStart);
    }
    else {
      out.write("<local>\n");
    }
    out.write(ds.indent(0));
    out.write(ds.member());

    out.write(ds.indent(0));
    out.write(stmt);

    out.write(ds.indent(0));
    out.write(time);

    out.write(ds.indent(0));
    out.write(begin_end_exe_time);

    out.write(ds.indent(0));
    out.write(ds.stmt_id());

    out.write(ds.indent(0));
    out.write(childTagStart);

    out.write(ds.getXmlString());

    out.write(ds.indent(0));
    out.write(childTagEnd);

    if(isLocalPlanExtracted) {
      out.write("</local>\n");
    }
  }
  
  public void writeXMLHeader(Writer out, boolean withRoot) throws IOException {
    out.write(defaultXML);
    if (embedXslFileName != null) {
      final String embedXSL = "<?xml-stylesheet type=\"text/xsl\" href=\""
          + embedXslFileName + "\"?>\n";
      out.write(embedXSL);
    }
    out.write(comment);
    
    if (withRoot) {
      out.write(rootTagStart);
    }
  }

  // test method
  public static boolean testXML(StringBuilder xmlFragment) {
    StringBuilder xmlForm = new StringBuilder();
    xmlForm.append(defaultXML);
    final String embedXSL = "<?xml-stylesheet type=\"text/xsl\" href=\"" + "com/pivotal/gemfirexd/tools/planexporter/resources/vanilla_text.xsl"
        + "\"?>\n";

    xmlForm.append(embedXSL);

    xmlForm.append(comment);
    xmlForm.append(parentTagStart);
    
    xmlForm.append(childTagStart);
    xmlForm.append(xmlFragment);
    xmlForm.append(childTagEnd);
    
    xmlForm.append(parentTagEnd);
    
    char[] planCharArray = new char[xmlForm.length()];
    
    xmlForm.getChars(0, xmlForm.length(), planCharArray, 0);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "transforming plan : " + new String(planCharArray));
        System.out.println(new String(planCharArray));
      }
    }
    
    Element e = transformToXML(planCharArray);
    
    if (e == null) {
      return false;
    }
    
    return true;
  }

  public List<char[]> getPlan() throws SQLException {

    final boolean createSingleXML = xmlForm == XMLForms.asXML;
    final CharArrayWriter singleXML = (createSingleXML ? new CharArrayWriter() : null);

    char[] planCharArray = null;
    try {
      
      final ExecutionPlanMessage msg = sendMessage();
      final List<char[]> listOfPlans = msg.getResults();
      if(!createSingleXML) {
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            for (int i = 0; i < listOfPlans.size(); i++) {
              planCharArray = listOfPlans.get(i);
              SanityManager.DEBUG_PRINT(
                  GfxdConstants.TRACE_PLAN_GENERATION,
                  "received plan " + new String(planCharArray) + " from "
                      + msg.getSender());
            }
          }
        }
        return listOfPlans;
      }
      
      for (int i = 0; i < listOfPlans.size(); i++) {

        planCharArray = listOfPlans.get(i);

        if (GemFireXDUtils.TracePlanAssertion) {
          Element e = transformToXML(planCharArray);
          if (e == null) {
            SanityManager.DEBUG_PRINT(
                "warning:" + GfxdConstants.TRACE_PLAN_GENERATION,
                " ALERT!!!!, unsucessful XML transformation for '"
                    + ds.getQueryID() + "' userQuery='" + ds.getUserQueryStr()
                    + "'");
          }
        }

        if (i == 0) {
          writeXMLHeader(singleXML, true);
        }
        singleXML.write(planCharArray);
      } // end of loop
      
      if (createSingleXML) {
        singleXML.write(rootTagEnd);
        if (GemFireXDUtils.TracePlanGeneration) {
          Element e = transformToXML(singleXML.toCharArray());
          if (e == null) {
            SanityManager.DEBUG_PRINT(
                "warning:" + GfxdConstants.TRACE_PLAN_GENERATION,
                " ALERT!!!!, unsucessful XML transformation for '"
                    + ds.getQueryID() + "' userQuery='" + ds.getUserQueryStr()
                    + "' CharXML=" + singleXML.toString());
          }
        }
      }

    } catch (SQLException ex) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "CreateXML:getPlan processing error " + ex + " for "
                + (planCharArray != null ? String.valueOf(planCharArray)
                    : "NULL"), ex);
      }
      throw ex;
    } catch (Throwable t) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "CreateXML:getPlan processing error " + t + " for "
                + (planCharArray != null ? String.valueOf(planCharArray)
                    : "NULL"), t);
      }
      throw Util.javaException(t);
    }
    
    ArrayList<char[]> xmlChars = new ArrayList<char[]>();
    xmlChars.add(singleXML.toCharArray());
    
    return xmlChars;
  }
  
  public void processPlan(
      CharArrayWriter out, 
      boolean isLocalPlanExtracted) throws SQLException, IOException {

    final boolean current = ds.setRuntimeStatisticsMode(false);

    try {
      if (!ds.verifySchemaExistance()) {
        throw GemFireXDRuntimeException.newRuntimeException(
            "CreateXML: specified Schema does not exist",
            null);
      }

      if (!ds.initializeDataArray()) {
        if (ds.isRemote()) {
          return;
        }
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                "No statistics were captured in this distributed member for "
                    + ds.getQueryID());
          }
        }
        return;
      }

      ds.createXMLFragment();

      writeToStream(
          out,
          ds.statement(),
          ds.time(),
          ds.begin_end_exe_time(), 
          isLocalPlanExtracted);
      
      // to avoid recursion further.
      if (!isLocalPlanExtracted) {
        BasicUUID locallyExecutedId = new BasicUUID(ds.getQueryID());
        locallyExecutedId.setLocallyExecuted(1);
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                "Now determining locally Executed Plan if any for "
                    + ds.getQueryID() + " with local stmtUUID="
                    + locallyExecutedId.toString());
          }
        }

        new CreateXML(ds.getClone(locallyExecutedId.toString()), true, xmlForm, embedXslFileName)
            .processPlan(out, true);
      }
      else {
        return;
      }

      out.write(parentTagEnd);
      
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TracePlanGeneration) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
              "Returning  " + out.toString());
        }
      }
    } finally {
      ds.closeConnection();
      ds.setRuntimeStatisticsMode(current);
    }
    
  }

  public static Element transformToXML(
      char[] plan) {
    try {
      final InputSource is = new InputSource(
          new CharArrayReader(
              plan));

      final DocumentBuilderFactory factory = DocumentBuilderFactory
          .newInstance();
      DocumentBuilder builder;
      builder = factory.newDocumentBuilder();
      Document d = builder.parse(is);

      return d.getDocumentElement();
    } catch (ParserConfigurationException e) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT("error:"
            + GfxdConstants.TRACE_PLAN_GENERATION,
            "CreateXML#transformToXML XML Parsing exception "
                + String.valueOf(plan), e);
      }
    } catch (SAXException e) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT("error:"
            + GfxdConstants.TRACE_PLAN_GENERATION,
            "CreateXML#transformToXML XML Parsing exception "
                + String.valueOf(plan), e);
      }
    } catch (IOException e) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT("error:"
            + GfxdConstants.TRACE_PLAN_GENERATION,
            "CreateXML#transformToXML XML Parsing exception "
                + String.valueOf(plan), e);
      }
    }
    return null;
  }
  
  public String toString() {
    return "CreateXML:isRemote="+ds.isRemote();
  }

  // try {
  // final String stmtstr = ds.statement();
  // final String timestr = ds.time();
  // final TreeNode[] data = ds.getData();
  //
  // final DocumentBuilderFactory factory =
  // DocumentBuilderFactory.newInstance();
  // final DocumentBuilder builder = factory.newDocumentBuilder();
  // final Document doc = builder.newDocument();
  // Element plan = doc.createElement("plan");
  // doc.appendChild(plan);
  //    
  // //stmt info
  // {
  // Element stmt = doc.createElement("statement");
  // stmt.appendChild(doc.createTextNode(stmtstr));
  // plan.appendChild(stmt);
  //
  // Element stmt_time = doc.createElement("time");
  // stmt_time.appendChild(doc.createTextNode(timestr));
  // plan.appendChild(stmt_time);
  //      
  // Element stmt_id = doc.createElement("stmt_id");
  // stmt_id.appendChild(doc.createTextNode(ds.stmtID()));
  // plan.appendChild(stmt_id);
  // }
  //    
  // Element details = doc.createElement("details");
  //    
  // for(TreeNode n : data) {
  //      
  // }
  //    
  //    
  //
  // } catch (ParserConfigurationException e) {
  // e.printStackTrace(System.err);
  // }

}
