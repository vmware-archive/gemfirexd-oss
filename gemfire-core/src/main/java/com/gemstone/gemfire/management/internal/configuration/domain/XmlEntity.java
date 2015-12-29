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
package com.gemstone.gemfire.management.internal.configuration.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.Nodes;
import nu.xom.ParsingException;
import nu.xom.ValidityException;

import org.springframework.util.Assert;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;

/****
 * Domain class for defining a GemFire entity in XML.
 * 
 * @author bansods
 * @author David Hoots
 * 
 */
public class XmlEntity implements DataSerializable {
  private static final long serialVersionUID = 1L;

  private String type;
  private Map<String, String> attributes;
  private String xmlDefinition;

  public XmlEntity() {
  }
  
  /**
   * Construct a new XmlEntity while creating XML from the cache using the
   * element which has a type and attribute matching those given.
   * 
   * @param type
   *          Type of the XML element to search for. Should be one of the
   *          constants from the {@link CacheXml} class. For example,
   *          CacheXml.REGION.
   * @param key
   *          Key of the attribute to match, for example, "name" or "id".
   * @param value
   *          Value of the attribute to match.
   */
  public XmlEntity(final String type, final String key, final String value) {
    Assert.hasLength(type, "Type cannot be empty");
    Assert.hasLength(key, "Key cannot be empty");
    Assert.hasLength(value, "Value cannot be empty");
    
    this.type = type;
    
    this.attributes = new HashMap<String, String>();
    this.attributes.put(key, value);
    
    this.xmlDefinition = loadXmlDefinition(type, this.attributes);
  }

  /**
   * Construct a new XmlEntity while creating XML from the cache using the
   * element which has attributes matching those given.
   * 
   * @param type
   *          Type of the XML element to search for. Should be one of the
   *          constants from the {@link CacheXml} class. For example,
   *          CacheXml.REGION.
   * @param attributes
   *          Attributes of the element that should match, for example "name" or
   *          "id" and the value they should equal.
   */
  public XmlEntity(final String type, Map<String, String> attributes) {
    Assert.hasLength(type, "Type cannot be empty");
    Assert.notNull(attributes, "Attributes cannot be null");
    
    this.type = type;
    this.attributes = attributes;
    this.xmlDefinition = loadXmlDefinition(type, attributes);
  }
  
  /**
   * Use the CacheXmlGenerator to create XML from the entity associated with
   * the current cache.
   * 
   * @param element Name of the XML element to search for.
   * @param attributes
   *          Attributes of the element that should match, for example "name" or
   *          "id" and the value they should equal.
   * 
   * @return XML string representation of the entity.
   */
  private final String loadXmlDefinition(final String element, final Map<String, String> attributes) {
    final Cache cache = CacheFactory.getAnyInstance();
    Document document = null;

    try {
      final StringWriter stringWriter = new StringWriter();
      final PrintWriter printWriter = new PrintWriter(stringWriter);
      CacheXmlGenerator.generate(cache, printWriter, false, false, false);
      printWriter.close();

      // Remove the DOCTYPE line when getting the string to prevent
      // errors when trying to locate the DTD.
      String xml = stringWriter.toString().replaceFirst("<!DOCTYPE.*>", "");
      document = new Builder(false).build(xml, null);

    } catch (ValidityException vex) {
      cache.getLogger().error("Could not parse XML when creating XMLEntity", vex);

    } catch (ParsingException pex) {
      cache.getLogger().error("Could not parse XML when creating XMLEntity", pex);
      
    } catch (IOException ioex) {
      cache.getLogger().error("Could not parse XML when creating XMLEntity", ioex);
    }
    
    Nodes nodes = document.query(createQueryString(element, attributes));
    if (nodes.size() != 0) {
      return nodes.get(0).toXML();
    }
    
    cache.getLogger().warning("No XML definition could be found with name=" + element + " and attributes=" + attributes);
    return null;
  }

  /**
   * Create an XmlPath query string from the given element name and attributes.
   * 
   * @param element Name of the XML element to search for.
   * @param attributes
   *          Attributes of the element that should match, for example "name" or
   *          "id" and the value they should equal.  This list may be empty.
   * 
   * @return An XmlPath query string.
   */
  private String createQueryString(final String element, final Map<String, String> attributes) {
    StringBuilder queryStringBuilder = new StringBuilder();
    Iterator<Entry<String, String>> attributeIter = attributes.entrySet().iterator();
    queryStringBuilder.append("//").append(element);

    if (attributes.size() > 0) {
      queryStringBuilder.append("[");
      Entry<String, String> attrEntry = attributeIter.next();
      queryStringBuilder.append("@").append(attrEntry.getKey()).append("=\"").append(attrEntry.getValue()).append("\"");

      while (attributeIter.hasNext()) {
        attrEntry = attributeIter.next();
        queryStringBuilder.append(" and @").append(attrEntry.getKey()).append("=\"").append(attrEntry.getValue()).append("\"");
      }

      queryStringBuilder.append("]");
    }

    return queryStringBuilder.toString();
  }
  
  public String getType() {
    return this.type;
  }

  public Map<String, String> getAttributes() {
    return this.attributes;
  }

  /**
   * Return the value of a single attribute.
   * 
   * @param key
   *          Key of the attribute whose while will be returned.
   * 
   * @return The value of the attribute.
   */
  public String getAttribute(String key) {
    return this.attributes.get(key);
  }

  /**
   * A convenience method to get a name or id attributes from the list of
   * attributes if one of them has been set.  Name takes precedence.
   * 
   * @return The name or id attribute or null if neither is found.
   */
  public String getNameOrId() {
    if (this.attributes.containsKey("name")) {
      return this.attributes.get("name");
    }
    
    return this.attributes.get("id");
  }

  public String getXmlDefinition() {
    return this.xmlDefinition;
  }

  @Override
  public String toString() {
    return "XmlEntity [type=" + this.type + ", attributes=" + this.attributes + ", xmlDefinition=" + this.xmlDefinition + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.attributes == null) ? 0 : this.attributes.hashCode());
    result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    XmlEntity other = (XmlEntity) obj;
    if (this.attributes == null) {
      if (other.attributes != null)
        return false;
    } else if (!this.attributes.equals(other.attributes))
      return false;
    if (this.type == null) {
      if (other.type != null)
        return false;
    } else if (!this.type.equals(other.type))
      return false;
    return true;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.type, out);
    DataSerializer.writeObject(this.attributes, out);
    DataSerializer.writeString(this.xmlDefinition, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.type = DataSerializer.readString(in);
    this.attributes = DataSerializer.readObject(in);
    this.xmlDefinition = DataSerializer.readString(in);
  }
}
