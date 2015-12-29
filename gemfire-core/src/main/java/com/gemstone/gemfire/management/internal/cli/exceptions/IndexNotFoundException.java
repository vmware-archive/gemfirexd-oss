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
package com.gemstone.gemfire.management.internal.cli.exceptions;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

public class IndexNotFoundException extends Exception {

  private static final long serialVersionUID = 1L;
  final String indexName;
  final String message;
  
  public IndexNotFoundException(final String indexName) {
    this.indexName = indexName;
    this.message = CliStrings.format("Index \" {0} \" not found", indexName);
  }
  
  public String getMessage() {
    return this.message;
  }
}
