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

package com.pivotal.gemfirexd.internal.impl.services.jmx;

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.admin.jmx.internal.AgentConfigImpl;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;

public class GfxdAgentConfigImpl extends AgentConfigImpl {

  public GfxdAgentConfigImpl() {
    super();
  }

  public GfxdAgentConfigImpl(String[] args) {
    super(args);
  }

  public GfxdAgentConfigImpl(Properties props) {
    super(gfxdFilterOutAgentProperties(props),
        true);
  }

  public static Properties gfxdFilterOutAgentProperties(Properties props) {
    
    props = appendOptionalPropertyFileProperties(props);
    
    final Properties filteredProps = new Properties();

    for (final Object key : props.keySet()) {
      if (gfxdGetPropertyDescription(key.toString()) == null) {
        final String value = props.getProperty(key.toString());
        if (value != null) {
          filteredProps.setProperty(key.toString(), value);
        }
      }
    }

    appendLogFileProperty(filteredProps);

    return filteredProps;
  }

  public GfxdAgentConfigImpl(File propFile) {
    super(propFile);
  }
  
  
  public static String gfxdGetPropertyDescription(String prop) {
    
    if(prop.startsWith(Property.USER_PROPERTY_PREFIX) || prop.startsWith(Property.SQLF_USER_PROPERTY_PREFIX)) {
      return "This is the user property used by Derby and LDAP schemes";
    }
    else if(prop.equals(Attribute.USERNAME_ATTR) || prop.equals(Attribute.USERNAME_ALT_ATTR)) {
      return "attribute to set 'user' or 'UserName' connection attribute.";
    }
    else if(prop.equals(Attribute.PASSWORD_ATTR)) {
      return "The attribute that is used to set the user password.";
    }
    
    return AgentConfigImpl._getPropertyDescription(prop);
  }
  
}
