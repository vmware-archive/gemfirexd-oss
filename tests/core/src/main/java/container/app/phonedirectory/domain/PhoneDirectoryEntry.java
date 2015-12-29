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
import container.app.lang.Visitor;
import container.app.util.Assert;

public class PhoneDirectoryEntry extends AbstractDomainObject implements DataSerializable {
  
  private static final long serialVersionUID = 1010101L;

  private Address address;
  private Person person;
  private PhoneNumber phoneNumber;

  public PhoneDirectoryEntry() {
  }
  
  public PhoneDirectoryEntry(final Person person, final Address address) {
    this(person, address, null);
  }
  
  public PhoneDirectoryEntry(final Person person, final PhoneNumber phoneNumber) {
    this(person, null, phoneNumber);
  }
  
  public PhoneDirectoryEntry(final Person person, final Address address, final PhoneNumber phoneNumber) {
    setPerson(person);
    this.address = address;
    this.phoneNumber = phoneNumber;
  }

  public Address getAddress() {
    return address;
  }

  public void setAddress(final Address address) {
    this.address = address;
  }

  public Person getPerson() {
    return person;
  }
  
  public final void setPerson(final Person person) {
    Assert.notNull(person, "The Person for the Phone Directory Entry cannot be null!");
    this.person = person;
  }

  public PhoneNumber getPhoneNumber() {
    return phoneNumber;
  }

  public void setPhoneNumber(final PhoneNumber phoneNumber) {
    this.phoneNumber = phoneNumber;
  }

  public void toData(final DataOutput out) throws IOException {
    DataSerializer.writeObject(getPerson(), out);
    DataSerializer.writeObject(getAddress(), out);
    DataSerializer.writeObject(getPhoneNumber(), out);
  }

  public void fromData(final DataInput in) throws IOException, ClassNotFoundException {
    this.person = DataSerializer.<Person>readObject(in);
    this.address = DataSerializer.<Address>readObject(in);
    this.phoneNumber = DataSerializer.<PhoneNumber>readObject(in);
  }

  @Override
  public void accept(final Visitor visitor) {
    super.accept(visitor);

    getPerson().accept(visitor);

    if (getAddress() != null) {
      getAddress().accept(visitor);
    }

    if (getPhoneNumber() != null) {
      getPhoneNumber().accept(visitor);
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    
    if (!(obj instanceof PhoneDirectoryEntry)) {
      return false;
    }
    
    final PhoneDirectoryEntry that = (PhoneDirectoryEntry) obj;
    
    return getPerson().equals(that.getPerson());
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + getPerson().hashCode();
    return hashValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(getClass().getName());
    buffer.append("{person = ").append(getPerson());
    buffer.append(", address = ").append(getAddress());
    buffer.append(", phoneNumber = ").append(getPhoneNumber());
    buffer.append("}");
    return buffer.toString();
  }

}
