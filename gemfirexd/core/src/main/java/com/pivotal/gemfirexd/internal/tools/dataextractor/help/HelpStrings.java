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
package com.pivotal.gemfirexd.internal.tools.dataextractor.help;

public class HelpStrings {

 public static String helpText = " " + 
" Command line tool for extracting the schema and data from the persistent files of server(s) of a single GemfireXD distributed system." +

 "Arguments : " + 

 "\n* property-file : Path of the property file containing the information about the server(s) and their working directory and disk store directories." +
 "\n  Usage: propertyfile=extraction.properties" +

 "\n  Options :" + 

 "\n* use-single-ddl : Option used to provide the .sql file containing the schema definition. The data extraction on all the server(s) would be based on this single schema." +
 "\n  Usage: --use-single-ddl=/backup/schema/schema.sql" +

 "\n* string-delimiter : Option to change the string delimiter.  The default is the double quotation \"" +
 "\n  Usage: --string-delimiter='/'" +
 "\n  Default: \" \""  +
 "\n" +
  
  
 "\n* help : Displays the help for the data extraction tool. " +
 "\n  Usage: --help " +
 "\n" +
 
 "\n* output-dir : Path of the output directory , where the extractor schema and data for all the servers will be store. " +
 "\n  Usage: --output-dir=/export/extractData/" +
 "\n  Default: Working directory of extractor tool." + 
 "\n" + 

 "*\n save-in-server-working-dir : Flag when set to true, saves the extracted data and schema for a server in its working directory. Default value is false" +
 "\n  Usage: --save-in-server-working-dir=true" +
 "\n" + 
 
 "\n* log-file : Specify the log file for the extractor" +
 "\n  Usage: --log-file=data_extractor.log" +
 "\n  Default : /outputdir/extractor.log" +
 "\n" +
 
 "\n* num-threads : Number of threads used for parallel data extraction of server(s)" +
 "\n  Usage : --num-threads=1" +
 "\n  Default : 1 " +
 "\n" + 
 "\n* user-name : Option to set the user-name for extracting the tables , its helpful when the tables were created by an user, the schema name for that table is the user name." +
 "\n   Usage: --user-name=admin";
}
