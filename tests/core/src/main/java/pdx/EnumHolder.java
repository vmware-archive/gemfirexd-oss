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
package pdx;

/**
 * @author lynn
 * Class to hold information about an enum when the enum is not available on the classPath
 *
 */
public class EnumHolder implements java.io.Serializable {
  
   private String className = null;
  private String enumValue = null;
  
  public EnumHolder(String className, String enumValue) {
    this.className = className;
    this.enumValue = enumValue;
  }
  
  public String getClassName() {
    return className;
  }
  public void setClassName(String className) {
    this.className = className;
  }
  public String getEnumValue() {
    return enumValue;
  }
  public void setEnumValue(String enumValue) {
    this.enumValue = enumValue;
  }
  @Override
  public String toString() {
    return "EnumHolder [className=" + className + ", enumValue=" + enumValue
        + "]";
  }


}
