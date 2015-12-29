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
package container.app.dao;

import container.app.util.StringUtils;

/**
 * This enumerated type defines enumerated values for each of the basic persistence/CRUD operations 
 * (CREATE, RETRIEVE, UPDATE and DELETE).
 * @author jblum
 */
public enum DataAccessType {
  CREATE,
  RETRIEVE,
  UPDATE,
  DELETE;
  
  public static DataAccessType getByName(final String name) {
    for (final DataAccessType op : values()) {
      if (op.name().equalsIgnoreCase(StringUtils.trim(name))) {
        return op;
      }
    }

    return null;
  }

  @Override
  public String toString() {
    return name().toLowerCase();
  }

}
