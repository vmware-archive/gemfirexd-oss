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

package com.pivotal.gemfirexd.tools.utils;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.w3c.dom.Element;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNodeList;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil.XMLForms;
import com.pivotal.gemfirexd.tools.planexporter.AccessDistributedSystem;
import com.pivotal.gemfirexd.tools.planexporter.CreateResultSet;
import com.pivotal.gemfirexd.tools.planexporter.CreateXML;

//import com.pivotal.gemfirexd.internal.tools.PlanExporter;

/**
 * Retrieves a statement plan in two ways.
 * <ol>
 * 
 * <li>Get an already captured plan from meta tables i.e. statements executed
 * over a connection with plan recording enabled via
 * <code>syscs_util.SET_XPLAIN_SCHEMA(<i>< schema name ></i>) </code>.
 * 
 * <li>Execute a query by creating a new connection & retrieve its execution
 * plan.
 * 
 * </ol>
 * 
 * @author soubhikc
 * 
 */
public final class ExecutionPlanUtils {

  private final AccessDistributedSystem ds;

  private final List<Element> planXML;

  private final List<String> planRS;

  /**
   * Must provide with Embedded or Net connection url of the form
   * "jdbc:gemfirexd:;...' or 'jdbc:gemfirexd://...'. Optionally a connection
   * property set can be passed like in {@link com.pivotal.gemfirexd.FabricService
   * FabricService} connection attributes.
   * 
   * @param connectionURL
   * @param connectionProperties
   * @param targetSchema
   *          , if null uses current schema
   * @param statement
   * @param queryParameters
   *          TODO
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public ExecutionPlanUtils(String connectionURL,
      Properties connectionProperties, String targetSchema, String statement,
      ArrayList<ArrayList<Object>> queryParameters)
      throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, SQLException {

    ds = new AccessDistributedSystem(connectionURL, connectionProperties,
        targetSchema, statement, queryParameters);

    planXML = getPlanAsXML();
    planRS = null;

    if (GemFireXDUtils.TracePlanGeneration) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
          "caching final query plan in XML form as " + planXML);
    }
  }

  /**
   * Determines information from active connection (in Embedded mode only),
   * acquires a nested connection & uses current schema as target.
   * 
   * @param conn
   *          user connection
   * @param queryParameters
   *          parameter values if statement contains parameters.
   * @param asXMLOrText
   *          whether to generate the output as xml or text.
   * @throws SQLException
   */
  public ExecutionPlanUtils(Connection conn, String statement,
      ArrayList<ArrayList<Object>> queryParameters, boolean asXMLOrText)
      throws SQLException {

    assert conn instanceof EmbedConnection;

    ds = new AccessDistributedSystem((EmbedConnection)conn, statement,
        queryParameters);

    if (asXMLOrText) {
      planXML = getPlanAsXML();
      planRS = null;
    }
    else {
      planXML = null;
      planRS = getPlanAsList();
    }

    if (GemFireXDUtils.TracePlanGeneration) {
      SanityManager.DEBUG_PRINT(
          GfxdConstants.TRACE_PLAN_GENERATION,
          "caching final query plan "
              + Integer.toHexString(System.identityHashCode(planXML))
              + " in XML form as " + planXML);
    }
  }

  public List<String> getPlanAsList() throws SQLException {
    if (planRS != null)
      return planRS;

    return (new CreateResultSet(ds, false)).getPlan();
  }

  public List<Element> getPlanAsXML() throws SQLException {
    if (planXML != null)
      return planXML;

    ArrayList<Element> xmlPlan = new ArrayList<Element>();
    List<char[]> planAsChars = (new CreateXML(ds, false, XMLForms.none, null)).getPlan();
    for (char[] p : planAsChars) {
      xmlPlan.add(CreateXML.transformToXML(p));
    }
    
    return xmlPlan;
  }

  public void getPlanAsHTML(String stylesheet, OutputStream output) {
    return;
  }

  public char[] getPlanAsText(String stylesheet) {
    if (stylesheet == null || stylesheet.length() <= 0) {
      stylesheet = "vanilla_text.xsl";
    }
    return Misc.serializeXMLAsCharArr(planXML, stylesheet);
  }

  public char[] getPlanAsText(Element e, String stylesheet) {
    if (stylesheet == null || stylesheet.length() <= 0) {
      stylesheet = "vanilla_text.xsl";
    }
    final List<Element> l = new ArrayList<Element>(1);
    l.add(e);
    return Misc.serializeXMLAsCharArr(l, stylesheet);
  }

  public void writePlanToXMLFile(String filename) {

  }

  public void writePlanToHTMLFile(String filename, String stylesheet) {

  }

}
