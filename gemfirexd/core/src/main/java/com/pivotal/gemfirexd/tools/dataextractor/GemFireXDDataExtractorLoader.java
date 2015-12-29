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
package com.pivotal.gemfirexd.tools.dataextractor;

import com.pivotal.gemfirexd.internal.tools.dataextractor.loader.GemFireXDDataExtractorLoaderImpl;

/**
 * The GemFireXDDataExtractorLoader is a tool that allows the rehydration of the
 * GemFireXD System based on output created by the
 * {@link GemFireXDDataExtractor} Tool. The tool takes in the Recommended.txt
 * file. It requires that the csvs be accessible by all nodes
 * 
 * @since 1.0.1
 */
public class GemFireXDDataExtractorLoader {

  /**
   * 
   * @param args
   *          the command line arguments and options to pass to the tool. The
   *          'recommended' argument is required.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    GemFireXDDataExtractorLoaderImpl.main(args);
  }
}
