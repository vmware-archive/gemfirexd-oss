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
 * 
 */
package diskRecovery;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import util.TestException;
import util.TestHelper;

/**
 * @author lynn
 *
 */
public class XmlManipulator {


  /** Given an xml document, write it to the given file path.
   * 
   * @param doc The xml document to write to a file.
   * @param xmlfilePath The file to write the xml document to. 
   * 
   */
  public static void writeXml(Document doc, String xmlFilePath) {
    DOMSource domSource = new DOMSource(doc);
    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer;
    try {
      transformer = tf.newTransformer();
      transformer.setOutputProperty(OutputKeys.METHOD, "xml");
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      DocumentType docType = doc.getDoctype();
      transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, docType.getSystemId());
      transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, docType.getPublicId());

      java.io.StringWriter sw = new java.io.StringWriter();
      StreamResult sr = new StreamResult(sw);
      transformer.transform(domSource, sr);
      String xml = sw.toString();
      PrintWriter aFile = new PrintWriter(new FileOutputStream(new File(xmlFilePath)));
      aFile.print(xml);
      aFile.flush();
      aFile.close();
    } catch (TransformerConfigurationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (TransformerException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (FileNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Given an xml file path, put the region tags in sort order. With persistent
   *  region creation, regions will wait for other jvms deemed to have the most
   *  recent disk files. This means that regions must be created in the same
   *  order in each jvm. The xml generator does not sort regions before it
   *  produces the xml file in pre-6.6 versions. So, when running conversion
   *  tests with pre-6.6 versions of gemfire, we need the region order to be
   *  the same for each jvm. Note: the xml generator was changed in 6.6 to
   *  sort regions before creating the xml file. 
   *  
   * @param xmlFilePath The path to the xml file.
   * @param sortedXmlFilePath The path to the sorted xml file to be created.
   */
  public static void sortRegions(String xmlFilePath, String sortedXmlFilePath) {
    class RegionNodeComparator implements Comparator {
      public int compare(Object o1, Object o2) {
        String regionName1 = ((Node)o1).getAttributes().getNamedItem("name").getNodeValue();
        String regionName2 = ((Node)o2).getAttributes().getNamedItem("name").getNodeValue();
        return regionName1.compareTo(regionName2);
      }
      public boolean equals(Object anObj) {
        String regionName1 = ((Node)this).getAttributes().getNamedItem("name").getNodeValue();
        String regionName2 = ((Node)anObj).getAttributes().getNamedItem("name").getNodeValue();
        return regionName1.equals(regionName2);
      }
    }

    TreeSet<Node> regionNodeSet = new TreeSet(new RegionNodeComparator());
    File file = new File(xmlFilePath);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db;
    try {
      db = dbf.newDocumentBuilder();
      CacheXmlParser handler = new CacheXmlParser(); // make this find the dtd locally rather than our website
      db.setEntityResolver(handler);
      Document doc = db.parse(file);
      doc.getDocumentElement().normalize();
      NodeList baseNodeList = doc.getElementsByTagName("cache");
      if (baseNodeList.getLength() != 1) {
        throw new TestException("Error, baseNodeList size is " + baseNodeList.getLength());
      }
      Node cacheNode = baseNodeList.item(0);
      NodeList cacheNodeList = cacheNode.getChildNodes();

      // divide all nodes off of the cache node into 3 categories: nodes that come
      // before the region nodes (node order preserved), nodes that come after 
      // the region nodes (node order preserved), and the region nodes 
      // themselves (in sorted order)
      List<Node> priorNodes = new ArrayList();
      List<Node> afterNodes = new ArrayList();
      boolean prior = true;
      for (int i = 0; i < cacheNodeList.getLength(); i++) { // iterate the Nodes of the cache
        Node aNode = cacheNodeList.item(i);
        String nodeName = aNode.getNodeName();
        if (nodeName.equals("region")) {
          regionNodeSet.add(aNode);
          prior = false;
        } else {
          if (prior) {
            priorNodes.add(aNode);
          } else {
            afterNodes.add(aNode);
          }
        }
      }

      // now that the nodes are saved, remove them from the cacheNode
      for (int i = 0; i < cacheNodeList.getLength(); i++) { 
        cacheNode.removeChild(cacheNodeList.item(i));
      }
      
      // add them back in in the desired order
      for (Node priorNode: priorNodes) {
        cacheNode.appendChild(priorNode);
      }
      for (Node regionNode: regionNodeSet) {
        //System.out.println("adding node " + regionNode);
        cacheNode.appendChild(regionNode);
      }
      for (Node afterNode: afterNodes) {
        cacheNode.appendChild(afterNode);
      }

      writeXml(doc, sortedXmlFilePath);
    } catch (ParserConfigurationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (SAXException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

}
