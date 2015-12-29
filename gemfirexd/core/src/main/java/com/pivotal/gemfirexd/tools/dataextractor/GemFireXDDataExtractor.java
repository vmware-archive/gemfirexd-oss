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

import com.pivotal.gemfirexd.internal.tools.dataextractor.extractor.GemFireXDDataExtractorImpl;

/**
 * The data extractor tool is a best effort tool to extract data from the
 * persisted oplogs of the GemFireXD System. It allows extraction of the table
 * data into csv files. The csv files will be per table, where a partitioned
 * table will be extracted as a csv file per bucket.
 * 
 * Along with the extracted csv files, the data extractor will also export a
 * Summary.txt file that provides a detailed list of all files extracted as well
 * as locations of the files and any concerning errors or exceptions. This will
 * lead to numerous csv files across all the servers, so the tool will also
 * generate a Recommended.txt that lists all the files (.sql and .csv files)
 * that are recommended to be used when recreating the system. It will make a 
 * best guess in determining which server had the latest information for each
 * table. Again this is a best guess and no guarantees are made for data
 * consistency. It is also recommended that a user analyzes the Recommended.txt
 * and Summary.txt and determine which files ultimately they would like to keep.
 * 
 * 
 * @since 1.0.1
 */
public class GemFireXDDataExtractor {

  /**
   * Starts the extraction process
   * 
   * @param args
   *          contains the command line options to feed to the extraction tool
   *          The 'property-file' argument is required.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    GemFireXDDataExtractorImpl.main(args);
  }
}
