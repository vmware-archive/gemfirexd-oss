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

/**
 *  Interface for configurable objects that encode an <code>int</code>
 *  key ("index").  An object type can be configured using its
 *  corresponding parameter class, if one exists.
 */

public interface ConfigurableObject {

  /**
   *  Returns a new instance of the object encoded with the index.
   *
   *  @throws ObjectCreationException
   *          An error occured when creating the object.  See the error
   *          message for more details.
   */
  public void init( int index );

  /**
   *  Returns the index encoded in the object.
   *
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   */
  public int getIndex();

  /**
   *  Validates whether the index is encoded in the object, if this
   *  applies, and performs other validation checks as needed.
   *
   *  @throws ObjectAccessException
   *          An error occured when accessing the object.  See the error
   *          message for more details.
   *  @throws ObjectValidationException
   *          The object failed validation.  See the error message for more
   *          details.
   */
  public void validate( int index );
}
