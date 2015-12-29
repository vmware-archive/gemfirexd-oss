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
package container.app.lang.support;

import java.util.Properties;

import container.app.util.StringUtils;

public class PropertiesConfigurationVisitor extends ConfigurationVisitor<Properties> {

  private static final long serialVersionUID = 918273645L;

  private final Properties props = new Properties();

  @Override
  public Properties getConfiguration() {
    return props;
  }

  @Override
  public void setConfiguration(final Properties configuration) {
    throw new UnsupportedOperationException(StringUtils.OPERATION_NOT_SUPPORTED);
  }

  public Object put(final String property, final String value) {
    return props.put(property, value);
  }
  
  public void putAll(final Properties configuration) {
    this.props.putAll(configuration);
  }

  public Object remove(final String property) {
    return props.remove(property);
  }

  public void removeAll() {
    props.clear();
  }

}
