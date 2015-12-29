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
package com.gemstone.gemfire.admin.jmx;

//import com.gemstone.gemfire.admin.AdminDistributedSystem;
import java.util.Properties;

import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.jmx.internal.AgentConfigImpl;
import com.gemstone.gemfire.admin.jmx.internal.AgentImpl;

/**
 * A factory class that creates JMX administration entities.
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class AgentFactory {

  /**
   * Defines a "default" GemFire JMX administration agent
   * configuration.
   */
  public static AgentConfig defineAgent() {
    return new AgentConfigImpl();
  }

  /**
   * Defines a GemFire JMX administration agent
   * configuration with the given Properties.
   */
  public static AgentConfig defineAgent(Properties props) {
    return new AgentConfigImpl(props);
  }
  
  /**
   * Creates an unstarted GemFire JMX administration agent with the
   * given configuration.
   *
   * @see Agent#start
   */
  public static Agent getAgent(AgentConfig config) 
    throws AdminException {
    return new AgentImpl(config);
  }

}
