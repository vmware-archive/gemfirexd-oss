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

public class Address extends AbstractDomainObject implements DataSerializable {
  
  private static final long serialVersionUID = 1235711L;
  
  private String street1;
  private String street2;
  private String city;
  private String state;
  private String zipCode;

  public Address() {
  }

  public Address(final String street1, final String city, final String state, final String zipCode) {
    this.street1 = street1;
    this.city = city;
    this.state = state;
    this.zipCode = zipCode;
  }

  public String getStreet1() {
    return street1;
  }

  public void setStreet1(final String street1) {
    this.street1 = street1;
  }

  public String getStreet2() {
    return street2;
  }

  public void setStreet2(final String street2) {
    this.street2 = street2;
  }

  public String getCity() {
    return city;
  }

  public void setCity(final String city) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState(final String state) {
    this.state = state;
  }

  public String getZipCode() {
    return zipCode;
  }

  public void setZipCode(final String zipCode) {
    this.zipCode = zipCode;
  }

  public void toData(final DataOutput out) throws IOException {
    DataSerializer.writeString(getStreet1(), out);
    DataSerializer.writeString(getStreet2(), out);
    DataSerializer.writeString(getCity(), out);
    DataSerializer.writeString(getState(), out);
    DataSerializer.writeString(getZipCode(), out);
  }

  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    this.street1 = DataSerializer.readString(in);
    this.street2 = DataSerializer.readString(in);
    this.city = DataSerializer.readString(in);
    this.state = DataSerializer.readString(in);
    this.zipCode = DataSerializer.readString(in);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    
    if (!(obj instanceof Address)) {
      return false;
    }
    
    final Address that = (Address) obj;
    
    return ObjectUtils.equalsOrNull(getStreet1(), getStreet1())
      && ObjectUtils.equalsOrNull(getStreet2(), getStreet2())
      && ObjectUtils.equalsOrNull(getCity(), that.getCity())
      && ObjectUtils.equalsOrNull(getState(), getState())
      && ObjectUtils.equalsOrNull(getZipCode(), that.getZipCode());
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getStreet1());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getStreet2());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getCity());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getState());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getZipCode());
    return hashValue;
  }
  
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(getClass().getName());
    buffer.append("{street1 = ").append(getStreet1());
    buffer.append(", street2 = ").append(getStreet2());
    buffer.append(", city = ").append(getCity());
    buffer.append(", state = ").append(getState());
    buffer.append(", zipCode = ").append(getZipCode());
    buffer.append("}");
    return buffer.toString();
  }

}
