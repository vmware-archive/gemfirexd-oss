/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.types.SqlXmlUtil

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
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

package com.pivotal.gemfirexd.internal.iapi.types;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.Formatable;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

import java.util.List;
import java.util.ArrayList;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.StringReader;

// -- JDBC 3.0 JAXP API classes.

import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/* (original imports)
import org.apache.xpath.XPath;
import org.apache.xpath.XPathContext;
import org.apache.xpath.objects.XObject;
import org.apache.xpath.objects.XNodeSet;

import org.apache.xml.utils.PrefixResolverDefault;

import org.apache.xalan.serialize.DOMSerializer;
import org.apache.xalan.serialize.Serializer;
import org.apache.xalan.serialize.SerializerFactory;
import org.apache.xalan.templates.OutputProperties;
*/

/**
 * This class contains "utility" methods that work with XML-specific
 * objects that are only available if JAXP and/or Xalan are in
 * the classpath.
 *
 * NOTE: This class is only compiled with JDK 1.4 and higher since
 * the XML-related classes that it uses (JAXP and Xalan) are not
 * part of earlier JDKs.
 *
 * Having a separate class for this functionality is beneficial
 * for two reasons:
 *
 *    1. Allows us to allocate XML objects and compile an XML
 *       query expression a single time per statement, instead of
 *       having to do it for every row against which the query
 *       is evaluated.  An instance of this class is created at
 *       compile time and then passed (using "saved objects")
 *       to the appropriate operator implementation method in
 *       XML.java; see SqlXmlExecutor.java for more about the
 *       role this class plays in "saved object" processing.
 *
 *    2. By keeping all XML-specific references in this one class, 
 *       we have a single "point of entry" to the XML objects--namely,
 *       the constructor for this class.  Thus, if we always make
 *       sure to check for the required XML classes _before_ calling
 *       this class's constructor, we can detect early on whether
 *       some classes (ex. Xalan) are missing, and can throw a friendly
 *       error up front, instead of a ClassNotFoundException somewhere
 *       deeper in the execution codepath.  The initial check for the
 *       required XML classes can be found in XML.checkXMLRequirements().
 *
 *       Note that we don't want to put references to XML-specific
 *       objects directly into XML.java because that class (XML.java) is
 *       instantiated anytime a table with an XML column is referenced.
 *       That would mean that if a user tried to select a non-XML column
 *       (ex. integer) from a table that had at least one XML column in
 *       it, the user would have to have JAXP and Xalan classes in
 *       his/her classpath--which we don't want.  Instead, by keeping
 *       all XML-specific objects in this one class, and then only
 *       instantiating this class when an XML operator is used (either
 *       implicitly or explicitly), we make it so that the user is only
 *       required to have XML-specific classes in his/her classpath
 *       _if_ s/he is trying to access or operate on XML values.
 */
public final class SqlXmlUtil implements Formatable {

    // Used to parse a string into an XML value (DOM); checks
    // the well-formedness of the string while parsing.
    private DocumentBuilder dBuilder;

// GemStone changes BEGIN
    // most functionality now moved into SqlXmlHelper implementations
    // that will provide the implementation depending on available ones
    private final com.pivotal.gemfirexd.internal.engine.types
        .SqlXmlHelper xmlHelper;
    /* (original code)
    // Used to serialize an XML value according the standard
    // XML serialization rules.
    private Serializer serializer;

    // Classes used to compile and execute an XPath expression
    // against Xalan.
    private XPath query;
    private XPathContext xpContext;
    */
// GemStone changes END

    // Used to recompile the XPath expression when this formatable
    // object is reconstructed.  e.g.:  SPS 
    private String queryExpr;
    private String opName;
    private boolean recompileQuery;
    
    /**
     * Constructor: Initializes objects required for parsing
     * and serializing XML values.  Since most XML operations
     * that require XML-specific classes perform both parsing
     * and serialization at some point, we just initialize the
     * objects up front.
     */
    public SqlXmlUtil() throws StandardException
    {
        try {

            /* Note: Use of DocumentBuilderFactory means that we get
             * whatever XML parser is the "default" for the JVM in
             * use--and thus, we don't have to hard-code the parser
             * name, nor do we have to require that the user have a
             * specific parser in his/her classpath.
             *
             * This DocumentBuilder is currently used for parsing
             * (esp. XMLPARSE), and the SQL/XML spec says that XMLPARSE
             * should NOT perform validation (SQL/XML[2006], 6.15:
             * "Perform a non-validating parse of a string to produce
             * an XML value.").   So we disable validation here, and
             * we also make the parser namespace aware.
             *
             * At some point in the future we will probably want to add
             * support for the XMLVALIDATE function--but until then, user
             * is unable to validate the XML values s/he inserts.
             *
             * Note that, even with validation turned off, XMLPARSE
             * _will_ still check the well-formedness of the values,
             * and it _will_ still process DTDs to get default values,
             * etc--but that's it; no validation errors will be thrown.
             */

            DocumentBuilderFactory dBF = null;
            try {

                dBF = DocumentBuilderFactory.newInstance();

            } catch (Throwable e) {

                /* We assume that if we get an error creating the
                 * DocumentBuilderFactory, it's because there's no
                 * JAXP implementation.  This can happen in the
                 * (admittedly unlikely) case where the classpath
                 * contains the JAXP _interfaces_ (ex. via xml-apis.jar)
                 * and the Xalan classes but does not actually
                 * contain a JAXP _implementation_.  In that case the
                 * check in XML.checkXMLRequirements() will pass
                 * and this class (SqlXmlUtil) will be instantiated
                 * successfully--which is how we get to this constructor.
                 * But then attempts to create a DocumentBuilderFactory
                 * will fail, bringing us here.  Note that we can't
                 * check for a valid JAXP implementation in the
                 * XML.checkXMLRequirements() method because we
                 * always want to allow the XML.java class to be
                 * instantiated, even if the required XML classes
                 * are not present--and that means that it (the
                 * XML class) cannot reference DocumentBuilder nor
                 * any of the JAXP classes directly.
                 */
                 throw StandardException.newException(
                     SQLState.LANG_MISSING_XML_CLASSES, "JAXP");

            }

            dBF.setValidating(false);
            dBF.setNamespaceAware(true);

            // Load document builder that can be used for parsing XML.
            dBuilder = dBF.newDocumentBuilder();
            dBuilder.setErrorHandler(new XMLErrorHandler());

// GemStone changes BEGIN
            this.xmlHelper = com.pivotal.gemfirexd.internal.engine.types
                .SqlXmlHelperFactory.newInstance();
            /* (original code)
            // Load serializer for serializing XML into string according
            // XML serialization rules.
            loadSerializer();
            */
// GemStone changes END

        } catch (StandardException se) {

            // Just rethrow it.
            throw se;

        } catch (Throwable t) {

            /* Must be something caused by JAXP or Xalan; wrap it in a
             * StandardException and rethrow it. Note: we catch "Throwable"
             * here to catch as many external errors as possible in order
             * to minimize the chance of an uncaught JAXP/Xalan error (such
             * as a NullPointerException) causing Derby to fail in a more
             * serious way.  In particular, an uncaught Java exception
             * like NPE can result in Derby throwing "ERROR 40XT0: An
             * internal error was identified by RawStore module" for all
             * statements on the connection after the failure--which we
             * clearly don't want.  If we catch the error and wrap it,
             * though, the statement will fail but Derby will continue to
             * run as normal.
             */ 
            throw StandardException.newException(
                SQLState.LANG_UNEXPECTED_XML_EXCEPTION, t, t.getMessage());

        }
    }

    /**
     * Take the received string, which is an XML query expression,
     * compile it, and store the compiled query locally.  Note
     * that for now, we only support XPath because that's what
     * Xalan supports.
     *
     * @param queryExpr The XPath expression to compile
     */
    public void compileXQExpr(String queryExpr, String opName)
        throws StandardException
    {
        this.xmlHelper.compileXQExpr(queryExpr, opName, this.dBuilder);
        this.queryExpr = queryExpr;
        this.opName = opName;
        this.recompileQuery = false;
    }

    /**
     * Take a string representing an XML value and serialize it
     * according SQL/XML serialization rules.  Right now, we perform
     * this serialization by first parsing the string into a JAXP
     * Document object, and then applying the serialization semantics
     * to that Document.  That seems a bit inefficient, but neither
     * Xalan nor JAXP provides a more direct way to do this.
     *
     * @param xmlAsText String version of XML on which to perform
     *   serialization.
     * @return A properly serialized version of xmlAsText.
     */
    public String serializeToString(String xmlAsText)
        throws Exception
    {
        ArrayList<Object> aList = new ArrayList<Object>();

        /* The call to dBuilder.parse() is a call to an external
         * (w.r.t. to Derby) JAXP parser.  If the received XML
         * text references an external DTD, then the JAXP parser
         * will try to read that external DTD.  Thus we wrap the
         * call to parse inside a privileged action to make sure
         * that the JAXP parser has the required permissions for
         * reading the DTD file.
         */
        try {

            final InputSource is = new InputSource(new StringReader(xmlAsText));
            aList.add(java.security.AccessController.doPrivileged(
                new java.security.PrivilegedExceptionAction<Object>()
                {
                    public Object run() throws IOException, SAXException
                    {
                        return dBuilder.parse(is);
                    }
                }));

        } catch (java.security.PrivilegedActionException pae) {

            /* Unwrap the privileged exception so that the user can
             * see what the underlying error is. For example, it could
             * be an i/o error from parsing the XML value, which can
             * happen if the XML value references an external DTD file
             * but the JAXP parser hits an i/o error when trying to read
             * the DTD.  In that case we want to throw the i/o error
             * itself so that it does not appear as a security exception
             * to the user.
             */
            throw pae.getException();

        }

        /* The second argument in the following call is for
         * catching cases where we have a top-level (parentless)
         * attribute node--but since we just created the list
         * with a single Document node, we already we know we
         * don't have a top-level attribute node in the list,
         * so we don't have to worry.  Hence the "null" here.
         */
        return serializeToString(aList, null);
    }

    /**
     * Take an array list (sequence) of XML nodes and/or string values
     * and serialize that entire list according to SQL/XML serialization
     * rules, which ultimately point to XML serialization rules as
     * defined by w3c.  As part of that serialization process we have
     * to first "normalize" the sequence.  We do that by iterating through
     * the list and performing the steps for "sequence normalization" as
     * defined here:
     *
     * http://www.w3.org/TR/xslt-xquery-serialization/#serdm
     *
     * This method primarily focuses on taking the steps for normalization;
     * for the rest of the serialization work, we just make calls on the
     * DOMSerializer class provided by Xalan.
     *
     * @param items List of items to serialize
     * @param xmlVal XMLDataValue into which the serialized string
     *  returned by this method is ultimately going to be stored.
     *  This is used for keeping track of XML values that represent
     *  sequences having top-level (parentless) attribute nodes.
     * @return Single string holding the serialized version of the
     *  normalized sequence created from the items in the received
     *  list.
     */
    public String serializeToString(final java.util.List<?> items,
        final XMLDataValue xmlVal) throws java.io.IOException {
      return this.xmlHelper.serializeToString(items, xmlVal);
    }

    /**
     * Evaluate this object's compiled XML query expression against
     * the received xmlContext.  Then if returnResults is false,
     * return an empty sequence (ArrayList) if evaluation yields
     * at least one item and return null if evaluation yields zero
     * items (the caller can then just check for null to see if the
     * query returned any items).  If returnResults is true, then return
     * return a sequence (ArrayList) containing all items returned
     * from evaluation of the expression.  This array list can contain
     * any combination of atomic values and XML nodes; it may also
     * be empty.
     *
     * Assumption here is that the query expression has already been
     * compiled and is stored in this.query.
     *
     * @param xmlContext The XML value against which to evaluate
     *  the stored (compiled) query expression
     * @param returnResults Whether or not to return the actual
     *  results of the query
     * @param resultXType The qualified XML type of the result
     *  of evaluating the expression, if returnResults is true.
     *  If the result is a sequence of exactly one Document node
     *  then this will be XML(DOCUMENT(ANY)); else it will be
     *  XML(SEQUENCE).  If returnResults is false, this value
     *  is ignored.
     * @return If returnResults is false then return an empty
     *  ArrayList if evaluation returned at least one item and return
     *  null otherwise.  If returnResults is true then return an
     *  array list containing all of the result items and return
     *  the qualified XML type via the resultXType parameter.
     * @exception Exception thrown on error (and turned into a
     *  StandardException by the caller).
     */
  public List<?> evalXQExpression(XMLDataValue xmlContext,
      boolean returnResults, int[] resultXType) throws Exception {
    return this.xmlHelper.evalXQExpression(xmlContext, returnResults,
        resultXType, this.dBuilder, this.recompileQuery, this.queryExpr,
        this.opName);
  }

    /* ****
     * Formatable interface implementation
     * */

    /** 
     * @see java.io.Externalizable#writeExternal 
     * 
     * @exception IOException on error
     */
    public void writeExternal(ObjectOutput out) 
        throws IOException
    {
        // query may be null
        if (this.xmlHelper.nullQuery())
        {
            out.writeBoolean(false);
        }
        else
        {
            out.writeBoolean(true);
            out.writeObject(queryExpr);
            out.writeObject(opName);
        }
    }

    /** 
     * @see java.io.Externalizable#readExternal 
     *
     * @exception IOException on error
     * @exception ClassNotFoundException on error
     */
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        if (in.readBoolean())
        {
            queryExpr = (String)in.readObject();
            opName = (String)in.readObject();
            recompileQuery = true;
	    }
    }

    /**
     * Get the formatID which corresponds to this class.
     *
     * @return	the formatID of this class
     */
    public int getTypeFormatId()
    { 
        return StoredFormatIds.SQL_XML_UTIL_V01_ID;
    }

    /*
     ** The XMLErrorHandler class is just a generic implementation
     ** of the ErrorHandler interface.  It allows us to catch
     ** and process XML parsing errors in a graceful manner.
     */
    private class XMLErrorHandler implements ErrorHandler
    {
        public void error (SAXParseException exception)
            throws SAXException
        {
            throw new SAXException (exception);
        }

        public void fatalError (SAXParseException exception)
            throws SAXException
        {
            throw new SAXException (exception);
        }

        public void warning (SAXParseException exception)
            throws SAXException
        {
            throw new SAXException (exception);
        }
    }
}
