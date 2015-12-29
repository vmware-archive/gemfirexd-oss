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
import java.util.Calendar;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

import container.app.domain.AbstractDomainObject;
import container.app.util.Assert;
import container.app.util.DateTimeFormatUtils;
import container.app.util.DateTimeUtils;
import container.app.util.ObjectUtils;

public class Person extends AbstractDomainObject implements DataSerializable {

  private static final long serialVersionUID = 112358L;

  private Calendar birthDate;

  private String firstName;
  private String lastName;

  public Person() {
  }

  public Person(final String firstName, final String lastName) {
    this.firstName = firstName;
    this.lastName = lastName;
  }

  public Calendar getBirthDate() {
    return DateTimeUtils.copy(birthDate);
  }

  public void setBirthDate(final Calendar birthDate) {
    this.birthDate = DateTimeUtils.copy(birthDate);
  }

  public String getFirstName() {
    return firstName;
  }

  public final void setFirstName(String firstName) {
    Assert.hasValue(firstName, "The Person's first name must be specified!");
    this.firstName = firstName;
  }
  
  public String getFullName() {
    final StringBuilder buffer = new StringBuilder(getFirstName());
    buffer.append(" ");
    buffer.append(getLastName());
    return buffer.toString();
  }

  public String getLastName() {
    return lastName;
  }

  public final void setLastName(final String lastName) {
    Assert.hasValue(lastName, "The Person's last name must be specified!");
    this.lastName = lastName;
  }

  public void toData(final DataOutput out) throws IOException {
    DataSerializer.writeLong(DateTimeUtils.toMilliseconds(getBirthDate()), out);
    DataSerializer.writeString(getFirstName(), out);
    DataSerializer.writeString(getLastName(), out);
  }

  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    final Long milliseconds = DataSerializer.readLong(in);
    
    if (milliseconds != 0) {
      this.birthDate = DateTimeUtils.fromMilliseconds(milliseconds);
    }
    
    this.firstName = DataSerializer.readString(in);
    this.lastName = DataSerializer.readString(in);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    
    if (!(obj instanceof Person)) {
      return false;
    }
    
    final Person that = (Person) obj;
    
    return ObjectUtils.equalsOrNull(getBirthDate(), that.getBirthDate())
      && ObjectUtils.equalsOrNull(getFirstName(), that.getFirstName())
      && ObjectUtils.equalsOrNull(getLastName(), that.getLastName());
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getBirthDate());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getFirstName());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getLastName());
    return hashValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(getClass().getName());
    buffer.append("{birthDate = ").append(DateTimeFormatUtils.format(getBirthDate()));
    buffer.append(", firstName = ").append(getFirstName());
    buffer.append(", lastName = ").append(getLastName());
    buffer.append("}");
    return buffer.toString();
  }

}
