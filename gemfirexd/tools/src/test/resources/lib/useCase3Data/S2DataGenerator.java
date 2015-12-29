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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class S2DataGenerator extends DataGenerator {

  public S2DataGenerator(String[] args) {
    super(args);
  }

  @Override
  protected char getFieldSeparator() {
    return '|';
  }

  @Override
  protected String getFormatterTimestamp() {
    return "yyyyMMddHH:mm:ss";
  }

  @Override
  protected String getFormatterDate() {
    return "yyyyMMdd";
  }

  @Override
  protected String getBaseTime() {
    return "2012010101:01:00";
  }

  public static void main(String[] args) {
    new S2DataGenerator(args);
  }
}
