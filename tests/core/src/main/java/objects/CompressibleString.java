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
package objects;

import java.io.Serializable;

public final class CompressibleString implements ConfigurableObject, UpdatableObject, Serializable {
  private static final long serialVersionUID = -3681062601264160662L;

  private static final String LATIN_FRAGMENT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam volutpat lacinia mauris ut fringilla. Maecenas nec sodales neque. In et diam a enim facilisis blandit. Maecenas cursus luctus arcu eget congue. Nunc mi ligula, porttitor molestie aliquet ac, cursus eget turpis. Mauris id porta lacus. Suspendisse adipiscing turpis vitae neque euismod at tristique neque luctus. In pulvinar malesuada rutrum.";
  
  private static final int MAX_NUMBER_OF_FRAGMENTS = 20;
  
  private String latin = null;
  
  private int index = 0;
  
  private int numberOfFragments = 0;
  
  private volatile int numberOfUpdates = 0;
  
  @Override
  public void init(int index) {
    this.index = index;
    buildLatin();
  }

  @Override
  public int getIndex() {
    return this.index;
  }

  @Override
  public void validate(int index) {
    // Do nothing...
  }

  private int generateNumberOfFragments() {
    return (int) ((Math.random() * MAX_NUMBER_OF_FRAGMENTS) + 1);
  }
  
  private void buildLatin() {
    StringBuilder builder = new StringBuilder();
    this.numberOfFragments = generateNumberOfFragments();
    
    for(int i = 0; i < this.numberOfFragments; ++i) {
      builder.append(LATIN_FRAGMENT);
    }
    
    this.latin = builder.toString();
  }
  
  public String getLatin() {
    return this.latin;
  }
  
  public int getNumberOfFragments() {
    return this.numberOfFragments;
  }

  @Override
  public void update() {
    ++this.numberOfUpdates;
  }
}
