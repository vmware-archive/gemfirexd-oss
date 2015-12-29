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
package com.gemstone.gemfire.internal.memcached.commands;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.memcached.Command;
import com.gemstone.gemfire.internal.memcached.RequestReader;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * Does not send a response on a cache miss.
 * 
 * @author Swapnil Bawaskar
 */
public class GetQCommand extends GetCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
