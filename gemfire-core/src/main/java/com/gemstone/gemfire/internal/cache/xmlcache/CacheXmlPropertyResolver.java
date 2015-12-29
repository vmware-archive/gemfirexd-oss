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

package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.HashSet;
import java.util.Properties;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Property resolver for resolving ${} like strings with system or Gemfire
 * properties in Cache.xml
 * 
 * @author Shobhit Agarwal
 * @since 6.6
 */
public class CacheXmlPropertyResolver implements PropertyResolver{

  LogWriterI18n logWriter;

  /**
   * Would not throw any exception {@link IllegalArgumentException} if property
   * could not be resolved
   */
  private boolean ignoreUnresolvedProperties = false;

  /** Would not throw any exception if property could not be resolved */
  private int propertyOverridden = SYSTEM_PROPERTIES_OVERRIDE;

  private CacheXmlPropertyResolverHelper helper = null;

  /**
   * A properties object to hold all user specified properties passed through
   * .properties file or passed as {@link Properties} objects. It will be set
   * by <code>CacheXmlParser</code> if gemfire.properties has to be included during
   * replacement.
   */
  private Properties props = null;

  public CacheXmlPropertyResolver(boolean ignoreUnresolvedProperties,
      int propertyOverridden, Properties props) {
    super();
    this.ignoreUnresolvedProperties = ignoreUnresolvedProperties;
    this.propertyOverridden = propertyOverridden;
    this.props = props;
  }

  public int getPropertyOverridden() {
    return propertyOverridden;
  }

  /**
   * Sets <code>propertyOverridden</code> with one of the constants specified in
   * this {@link CacheXmlPropertyResolver} class.
   * 
   * @param propertyOverridden
   */
  public void setPropertyOverridden(int propertyOverridden) {
    this.propertyOverridden = propertyOverridden;
  }

  public void setIgnoreUnresolvedProperties(boolean ignoreUnresolvedProperties) {
    this.ignoreUnresolvedProperties = ignoreUnresolvedProperties;
  }

  public boolean isIgnoreUnresolvedProperties() {
    return ignoreUnresolvedProperties;
  }

  /**
   * Resolves the given property string either from system properties or given
   * properties. and returns the replacement of the property found in available
   * properties. If no string replacement is found then {@link IllegalArgumentException}
   * would be thrown based on <code>ignoreUnresolvedProperties</code> flag being set by
   * {@link CacheXmlParser}.
   * 
   * @param replaceString
   * @return resolvedString
   */
  public String resolveReplaceString(String replaceString) {
    String replacement = null;

    //Get System property first.
    replacement = System.getProperty(replaceString);

    //Override system property if replacement is null or we want to override system property.
    if((replacement == null || getPropertyOverridden() == SYSTEM_PROPERTIES_OVERRIDE) && props != null){
      String userDefined = props.getProperty(replaceString);
      if(userDefined != null){
        replacement = userDefined;
      }
    }
    return replacement;
  }

  /**
   * @param stringWithPrefixAndSuffix
   * @param prefix
   * @param suffix
   * @return string
   */
  public String processUnresolvableString(String stringWithPrefixAndSuffix, String prefix, String suffix){
    String resolvedString = null;
    try{
      if (helper == null){
        helper = new CacheXmlPropertyResolverHelper(prefix, suffix);
      }
      /** A <code>resolvedString</code> can be same as <code>stringWithPrefixAndSuffix</code> if 
       * <code>ignoreUnresolvedProperties</code> is set true and we just return it as is.
       */
      resolvedString = helper.parseResolvablePropString(stringWithPrefixAndSuffix, this, new HashSet<String>());
    } catch (IllegalArgumentException e) {
      if(ignoreUnresolvedProperties) {
        //Do Nothing
      } else {
        if(getLogWriter() != null) {
          logWriter.error(LocalizedStrings.CacheXmlPropertyResolver_UNSEROLVAVLE_STRING_FORMAT_ERROR__0, stringWithPrefixAndSuffix);
        } else {
          throw e;
        }
      }      
    }
    return resolvedString;
  }

  public LogWriterI18n getLogWriter() {
    return logWriter;
  }

  public void setLogWriter(LogWriterI18n logWriter) {
    this.logWriter = logWriter;
  }

  public void setProps(Properties props) {
    this.props = props;
  }

  public String processUnresolvableString(String stringWithPrefixAndSuffix) {
    return processUnresolvableString(stringWithPrefixAndSuffix, null, null);
  }
}
