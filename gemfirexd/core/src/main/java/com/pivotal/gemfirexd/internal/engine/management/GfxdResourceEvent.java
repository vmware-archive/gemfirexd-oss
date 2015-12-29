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
package com.pivotal.gemfirexd.internal.engine.management;

/**
 *
 * @author Abhishek Chaudhari
 * @since gfxd 1.0
 */
public interface GfxdResourceEvent {
  int DERBY_MBEAN__REGISTER   = 0;
  int DERBY_MBEAN__UNREGISTER = 1;
  int FABRIC_DB__BOOT         = 2;
  int FABRIC_DB__STOP         = 3;
  int EMBEDCONNECTION__INIT   = 4;
  int TABLE__CREATE           = 5;
  int TABLE__DROP             = 6;
  int STATEMENT__CREATE       = 7;
  int STATEMENT__INVALIDATE   = 8; // CachedStatement invalidated from memory
  int TABLE__ALTER            = 9;
}
