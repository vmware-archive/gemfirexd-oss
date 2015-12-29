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
package com.gemstone.gemfire.pdx;

/**
 * Thrown if "check-portability" is enabled and an attempt is made to
 * pdx serialize a class that is not portable to non-java platforms.
 * 
 * @author darrel
 * @since 6.6.2
 */
public class NonPortableClassException extends PdxSerializationException {

  private static final long serialVersionUID = -743743189068362837L;

  public NonPortableClassException(String message) {
    super(message);
  }

}
