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
package container.app.phonedirectory.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

import container.app.domain.AbstractDomainObject;
import container.app.util.ObjectUtils;

public class PhoneNumber extends AbstractDomainObject implements DataSerializable {
  
  private static final long serialVersionUID = 3141591L;

  private String areaCode;
  private String prefix;
  private String suffix;
  private String extension;

  public PhoneNumber() {
  }

  public PhoneNumber(final String areaCode, final String prefix, final String suffix) {
    this.areaCode = areaCode;
    this.prefix = prefix;
    this.suffix = suffix;
  }

  public String getAreaCode() {
    return areaCode;
  }

  public void setAreaCode(final String areaCode) {
    this.areaCode = areaCode;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(final String prefix) {
    this.prefix = prefix;
  }

  public String getSuffix() {
    return suffix;
  }

  public void setSuffix(final String suffix) {
    this.suffix = suffix;
  }

  public String getExtension() {
    return extension;
  }

  public void setExtension(final String extension) {
    this.extension = extension;
  }

  public void toData(final DataOutput out) throws IOException {
    DataSerializer.writeString(getAreaCode(), out);
    DataSerializer.writeString(getPrefix(), out);
    DataSerializer.writeString(getSuffix(), out);
    DataSerializer.writeString(getExtension(), out);
  }

  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    this.areaCode = DataSerializer.readString(in);
    this.prefix = DataSerializer.readString(in);
    this.suffix = DataSerializer.readString(in);
    this.extension = DataSerializer.readString(in);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    
    if (!(obj instanceof PhoneNumber)) {
      return false;
    }
    
    final PhoneNumber that = (PhoneNumber) obj;
    
    return ObjectUtils.equalsOrNull(getExtension(), that.getExtension())
      && ObjectUtils.equalsOrNull(getSuffix(), that.getSuffix())
      && ObjectUtils.equalsOrNull(getPrefix(), that.getPrefix())
      && ObjectUtils.equalsOrNull(getAreaCode(), that.getAreaCode());
  }
  
  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getExtension());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getSuffix());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getPrefix());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getAreaCode());
    return hashValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(getClass().getName());
    buffer.append("{areaCode = ").append(getAreaCode());
    buffer.append(", prefix = ").append(getPrefix());
    buffer.append(", suffix = ").append(getSuffix());
    buffer.append(", extension = ").append(getExtension());
    buffer.append("}");
    return buffer.toString();
  }

}
