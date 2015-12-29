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
package com.gemstone.gemfire.internal.datasource;

/**
 * This class represents the config property for vendor specific data. This has
 * a name, value and type.
 * 
 * @author rreja
 */
public class ConfigProperty  {

  private String _name; // name of property
  private String _value; // value
  private String _type; // Class of the property. It could be one of

  // java.lang.String or Wrapper classes of Basic data types like int, double,
  // boolean etc.
  public ConfigProperty() {
  }

  public ConfigProperty(String name, String value, String type) {
    _name = name;
    _type = type;
    _value = value;
  }

  public void setName(String name) {
    this._name = name;
  }

  public String getName() {
    return _name;
  }

  public void setType(String type) {
    this._type = type;
  }

  public String getType() {
    return _type;
  }

  public void setValue(String value) {
    this._value = value;
  }

  public String getValue() {
    return _value;
  }
}
