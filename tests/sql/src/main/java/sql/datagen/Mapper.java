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
package sql.datagen;

import hydra.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.gemfire.LogWriter;

/**
 * Mapper to parse the mapper file
 * 
 * @author Rahul Diyewar
 */

public class Mapper {
  static public LogWriter log = Log.getLogWriter();
  private static Mapper mapper;
  private AtomicBoolean parsed = new AtomicBoolean(false);

  public final static String nullToken = "[null]";
  public final static String skipToken = "[skip]";

  // column name map with key=columnName and value="column mapped Object"
  private HashMap<String, MappedColumnInfo> columnNameMapping = new HashMap<String, MappedColumnInfo>();

  private Mapper() {
  }

  public static Mapper getMapper() {
    if (mapper == null) {
      synchronized (Mapper.class) {
        if (mapper == null) {
          mapper = new Mapper();
        }
      }
    }
    return mapper;
  }

  public void resetMapper() {
    columnNameMapping = new HashMap<String, MappedColumnInfo>();
  }

  public synchronized void parseMapperFile(String mapper, Connection conn)
      throws Exception {
    if (parsed.get()) {
      return;
    }

    parsed.set(true);
    if (mapper == null) {
      log.error(" mapper file is null, returning.");
      return;
    }

    log.info("Start parsing mapper: " + mapper);
    BufferedReader in = null;
    try {
      // read and parse the mapper file
      FileReader freader = new FileReader(mapper);
      in = new BufferedReader(freader);
      String line;
      boolean skipMode = false;

      while ((line = in.readLine()) != null) {
        // end of multi-line comments
        if (line.startsWith("*/")) {
          skipMode = false;
          continue;
        }
        // start of new block
        else if (line.startsWith("##")) {
          continue;
        }
        // single line comments
        else if (skipMode || line.startsWith("#")) {
          continue;
        }
        // start of multi-line comments
        else if (line.startsWith("/*")) {
          skipMode = true;
          continue;
        }

        // <column-name> = <column-format>
        line = line.trim();
        if (line.length() > 0) {
          String[] mapping = line.split("=");
          String col = mapping[0].trim().toUpperCase();
          String mappedColumnS = mapping[1].trim();
          // mapped column Object
          MappedColumnInfo mappedColumn = parseLine(col, mappedColumnS, conn);
          if (mappedColumn == null) {
            // column name mapping observed, for example
            // APP.CHART.EQP_ID = APP.HISTORY.EQP_ID
            mappedColumnS = mappedColumnS.toUpperCase();
            mappedColumn = columnNameMapping.get(mappedColumnS);
            log.info("Registering mapping for column " + col + " to "
                + mappedColumn);

            if (mappedColumn == null) {
              throw new Exception("No parent mapping found for " + col
                  + " for parent " + mappedColumnS);
            }
          }
          if (columnNameMapping.put(col, mappedColumn) != null) {
            throw new IllegalStateException("Multiple mappings for column "
                + col);
          }
        }
      }
      in.close();
    } catch (Exception e) {
      throw e;
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          throw e;
        }
      }
    }

    log.info("Stop parsing mapper:" + mapper + "\n");
  }

  public HashMap<String, MappedColumnInfo> getColumnNameMapping() {
    return columnNameMapping;
  }

  private MappedColumnInfo parseLine(String col, String line, Connection conn)
      throws Exception {
    if (line.startsWith("::")) {
      line = line.substring("::".length());

      if (SpecialTokens.valuelist.lineStartsWith(line)) {
        return SpecialTokens.valuelist.parseToken(col, line, conn);
      } else if (SpecialTokens.foreignkey.lineStartsWith(line)) {
        return SpecialTokens.foreignkey.parseToken(col, line, conn);
      } else if (SpecialTokens.random.lineStartsWith(line)) {
        return SpecialTokens.random.parseToken(col, line, conn);
      } else if (SpecialTokens.format.lineStartsWith(line)) {
        return SpecialTokens.format.parseToken(col, line, conn);
      } else if (SpecialTokens.UUID.lineStartsWith(line)) {
        return SpecialTokens.UUID.parseToken(col, line, conn);
      } else if (SpecialTokens.LCTS.lineStartsWith(line)) {
        return SpecialTokens.LCTS.parseToken(col, line, conn);
      } else if (SpecialTokens.LMTS.lineStartsWith(line)) {
        return SpecialTokens.LMTS.parseToken(col, line, conn);
      } else if (SpecialTokens.UUID.lineStartsWith(line)) {
        return SpecialTokens.UUID.parseToken(col, line, conn);
      } else if (SpecialTokens.UDT.lineStartsWith(line)) {
        return SpecialTokens.UDT.parseToken(col, line, conn);
      } else {
        throw new IllegalArgumentException("Invalid col " + col + " for line: "
            + line);
      }
    } else if (SpecialTokens.nullvalue.lineStartsWith(line)) {
      return SpecialTokens.nullvalue.parseToken(col, line, conn);
    } else if (SpecialTokens.skipvalue.lineStartsWith(line)) {
      return SpecialTokens.skipvalue.parseToken(col, line, conn);
    } else {
      return null;
    }
  }

}
