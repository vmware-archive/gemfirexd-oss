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
package com.gemstone.gemfire.tutorial.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A profile for a user of the gemstone social networking application. The
 * profile contains the list of friends of this person.
 * 
 * Profiles are used as the value in the people region. They will be distributed
 * to other VMs, so they must be serializable.
 * 
 * @author GemStone Systems, Inc.
 */
public class Profile implements Serializable {
  private static final long serialVersionUID = -3569243165627161127L;
  
  private final Set<String> friends = new HashSet<String>();
  
  public boolean addFriend(String name) {
    return this.friends.add(name);
  }
  
  public boolean removeFriend(String name) {
    return this.friends.remove(name);
  }
  
  public Set<String> getFriends() {
    return Collections.unmodifiableSet(friends);
  }

  @Override
  public String toString() {
    return "Profile [friends=" + friends + "]";
  }
  
  
}
