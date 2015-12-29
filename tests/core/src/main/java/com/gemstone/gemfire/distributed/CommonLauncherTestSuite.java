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

package com.gemstone.gemfire.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
 * The CommonLauncherTestSuite is a base class for encapsulating reusable functionality across the various, specific
 * launcher test suites.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.AbstractLauncherJUnitTest
 * @see com.gemstone.gemfire.distributed.LocatorLauncherJUnitTest
 * @see com.gemstone.gemfire.distributed.ServerLauncherJUnitTest
 * @since 7.0
 */
@SuppressWarnings("unused")
public abstract class CommonLauncherTestSuite {

  protected static File writeGemFirePropertiesToFile(final Properties gemfireProperties,
                                                     final String filename,
                                                     final String comment)
  {
    final File gemfirePropertiesFile = new File(System.getProperty("user.dir"), filename);

    try {
      gemfireProperties.store(new FileWriter(gemfirePropertiesFile, false), comment);
      return gemfirePropertiesFile;
    }
    catch (IOException e) {
      return null;
    }
  }

}
