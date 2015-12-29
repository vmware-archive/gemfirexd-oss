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
package container.app.util;

public abstract class ComposableFilter<T> implements Filter<T> {
  
  private final Filter<T> leftFilter;
  private final Filter<T> rightFilter;
  
  protected ComposableFilter(final Filter<T> leftFilter, final Filter<T> rightFilter) {
    this.leftFilter = leftFilter;
    this.rightFilter = rightFilter;
  }

  public static <T> Filter<T> composeAnd(final Filter<T> leftFilter, final Filter<T> rightFilter) {
    return compose(leftFilter, rightFilter, new AndFilter<T>(leftFilter, rightFilter));
  }
  
  public static <T> Filter<T> composeOr(final Filter<T> leftFilter, final Filter<T> rightFilter) {
    return compose(leftFilter, rightFilter, new OrFilter<T>(leftFilter, rightFilter));
  }
  
  protected static <T> Filter<T> compose(final Filter<T> leftFilter, 
                                         final Filter<T> rightFilter, 
                                         final ComposableFilter<T> composedFilter) 
  {
    return (leftFilter == null ? rightFilter : (rightFilter == null ? leftFilter : composedFilter));
  }

  protected Filter<T> getLeftFilter() {
    Assert.state(this.leftFilter != null, "The left Filter object was not properly set!");
    return this.leftFilter;
  }
  
  protected Filter<T> getRightFilter() {
    Assert.state(this.rightFilter != null, "The right Filter object was not properly set!");
    return this.rightFilter;
  }

  protected static final class AndFilter<T> extends ComposableFilter<T> {

    public AndFilter(final Filter<T> leftFilter, final Filter<T> rightFilter) {
      super(leftFilter, rightFilter);
    }

    public boolean accept(final T obj) {
      return (getLeftFilter().accept(obj) && getRightFilter().accept(obj));
    }
  }

  protected static final class OrFilter<T> extends ComposableFilter<T> {

    public OrFilter(final Filter<T> leftFilter, final Filter<T> rightFilter) {
      super(leftFilter, rightFilter);
    }

    public boolean accept(final T obj) {
      return (getLeftFilter().accept(obj) || getRightFilter().accept(obj));
    }
  }

}
