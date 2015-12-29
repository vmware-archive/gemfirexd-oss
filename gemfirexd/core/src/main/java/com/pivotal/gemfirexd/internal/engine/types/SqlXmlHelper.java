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

package com.pivotal.gemfirexd.internal.engine.types;

import java.util.List;

import javax.xml.parsers.DocumentBuilder;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.SqlXmlUtil;
import com.pivotal.gemfirexd.internal.iapi.types.XMLDataValue;

/**
 * This is a common interface for <code>{@link SqlXmlUtil}</code> helper classes
 * using either Sun JDK5's builtin Xerces/Xalan or the default Xalan classes.
 * 
 * Avoided changing {@link SqlXmlUtil} itself due to dependency on a fixed value
 * of <code>TypeFormat</code> that should not be different across different
 * implementations.
 * 
 * @see SqlXmlUtil
 * 
 * @author swale
 */
public interface SqlXmlHelper {

  /**
   * Take the received string, which is an XML query expression, compile it, and
   * store the compiled query locally. Note that for now, we only support XPath
   * because that's what Xalan supports.
   * 
   * @param queryExpr
   *          The XPath expression to compile
   */
  public void compileXQExpr(String queryExpr, String opName,
      DocumentBuilder dBuilder) throws StandardException;

  /**
   * Take an array list (sequence) of XML nodes and/or string values and
   * serialize that entire list according to SQL/XML serialization rules, which
   * ultimately point to XML serialization rules as defined by w3c. As part of
   * that serialization process we have to first "normalize" the sequence. We do
   * that by iterating through the list and performing the steps for
   * "sequence normalization" as defined here:
   * 
   * http://www.w3.org/TR/xslt-xquery-serialization/#serdm
   * 
   * This method primarily focuses on taking the steps for normalization; for
   * the rest of the serialization work, we just make calls on the DOMSerializer
   * class provided by Xalan.
   * 
   * @param items
   *          List of items to serialize
   * @param xmlVal
   *          XMLDataValue into which the serialized string returned by this
   *          method is ultimately going to be stored. This is used for keeping
   *          track of XML values that represent sequences having top-level
   *          (parentless) attribute nodes.
   * @return Single string holding the serialized version of the normalized
   *         sequence created from the items in the received list.
   */
  public String serializeToString(List<?> items, XMLDataValue xmlVal)
      throws java.io.IOException;

  /**
   * Evaluate this object's compiled XML query expression against the received
   * xmlContext. Then if returnResults is false, return an empty sequence
   * (ArrayList) if evaluation yields at least one item and return null if
   * evaluation yields zero items (the caller can then just check for null to
   * see if the query returned any items). If returnResults is true, then return
   * return a sequence (ArrayList) containing all items returned from evaluation
   * of the expression. This array list can contain any combination of atomic
   * values and XML nodes; it may also be empty.
   * 
   * Assumption here is that the query expression has already been compiled and
   * is stored in this.query.
   * 
   * @param xmlContext
   *          The XML value against which to evaluate the stored (compiled)
   *          query expression
   * @param returnResults
   *          Whether or not to return the actual results of the query
   * @param resultXType
   *          The qualified XML type of the result of evaluating the expression,
   *          if returnResults is true. If the result is a sequence of exactly
   *          one Document node then this will be XML(DOCUMENT(ANY)); else it
   *          will be XML(SEQUENCE). If returnResults is false, this value is
   *          ignored.
   * 
   * @return If returnResults is false then return an empty ArrayList if
   *         evaluation returned at least one item and return null otherwise. If
   *         returnResults is true then return an array list containing all of
   *         the result items and return the qualified XML type via the
   *         resultXType parameter.
   * @exception Exception
   *              thrown on error (and turned into a StandardException by the
   *              caller).
   */
  public List<?> evalXQExpression(XMLDataValue xmlContext,
      boolean returnResults, int[] resultXType, DocumentBuilder dBuilder,
      boolean recompileQuery, String queryExpr, String opName) throws Exception;

  /**
   * Returns true if the compiled query is null.
   */
  public boolean nullQuery();
}
