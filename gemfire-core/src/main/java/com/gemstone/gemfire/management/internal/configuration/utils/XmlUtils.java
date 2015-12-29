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
package com.gemstone.gemfire.management.internal.configuration.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Node;
import nu.xom.Nodes;
import nu.xom.ParsingException;
import nu.xom.Serializer;
import nu.xom.ValidityException;

/***
 * Provides utilities for reading, writing, formatting, modifying xml.
 * @author bansods
 *
 */
public class XmlUtils {
  private static final Builder builder1 = new Builder(true);
  private static final Builder builder2 = new Builder(false);

  public static Document createDocumentFromFile(String xmlFilePath) {
    Document doc = null;
    try {
      doc = builder1.build(new File(xmlFilePath));
    } catch (ValidityException e) {
      e.printStackTrace();
    } catch (ParsingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return doc;
  }

  public static Document createDocumentFromXml(String xmlContent) {
    Document doc = null;
    try {
      doc = builder2.build(new StringReader(xmlContent));
    } catch (ValidityException e) {
      e.printStackTrace();
    } catch (ParsingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return doc;
  }
  
  public static Node createNode(String xmlDefintion)  {
    Node node = null;
    try {
      node = builder2.build(new StringReader(xmlDefintion)).getRootElement().copy();
    } catch (ValidityException e) {
      e.printStackTrace();
    } catch (ParsingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return node;
  }

  public static String readXmlAsStringFromFile(String xmlFilePath){
    Document doc = null;
    doc = createDocumentFromFile(xmlFilePath);
    if (doc != null) {
      return doc.toXML();
    }
    return null;
  }
  
  public static String prettyXml(Document doc) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Serializer serializer = new Serializer(out);
    serializer.setIndent(2);
    serializer.write(doc);
    return out.toString("UTF-8");
  }
  
  private static String createQueryStringByName(String entityType, String entityName) {
    StringBuilder sb = new StringBuilder();
    sb.append("//");
    sb.append(entityType);
    sb.append("[@name='");
    sb.append(entityName);
    sb.append("']");
    return sb.toString();
  }
  
  public static void deleteNode(Document doc , String entityType, String entityName) {
    Node node = getNode(doc, entityType, entityName);
    if (node != null) {
      node.detach();
    }
  }
  
  public static Node getNode(Document doc, String entityType, String entityName) {
    String query = createQueryStringByName(entityType, entityName);
    Nodes nodes = doc.query(query);
    if (nodes != null && nodes.size() > 0) {
      return nodes.get(0);
    } 
    return null;
  }
  
  public static String getXmlDefinition(Document doc, String entityType, String entityName) {
    Node node = getNode(doc, entityType, entityName);
    if (node != null) {
      return node.toXML();
    } else {
      return null;
    }
  }
  
  public static void addNewNode(Document doc, String xmlDefinition) {
    doc.getRootElement().appendChild(createNode(xmlDefinition));
  }
}
