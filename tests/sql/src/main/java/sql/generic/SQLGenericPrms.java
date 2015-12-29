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
package sql.generic;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.TestConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import sql.generic.ddl.create.DDLStmtFactory;

public class SQLGenericPrms extends BasePrms {
  static {
    setValues(SQLGenericPrms.class);
  }

  /**
   * (String) Mapper file name.
   */
  public static Long mapperFile;

  public static Long genericDmlOperations;
  public static Long genericIndexOperations;
  public static Long alterTableDDLs;
  public static Long alterTableDDLChoices;
  public static Long dropIndex;
  public static Long dmlOperations;

  public static List<String> getAnyDMLGroup() {
    Vector dmlOperations = SQLGenericPrms.tab().vecAt(genericDmlOperations,
        null);
    List<String> dmlOperationGroup = null;

    if (dmlOperations != null) {
      if (dmlOperations.get(0) instanceof String)
        dmlOperationGroup = new ArrayList<String>(dmlOperations);
      else
        dmlOperationGroup = (List<String>)dmlOperations.get(SQLOldTest.random
            .nextInt(dmlOperations.size()));
    }
    return dmlOperationGroup;
  }  

  // select alter DDL randomly
  public static String getAnyAlterDDL() {
    String alterDDL = "";
    int randDDL;
    Vector alterStmts = TestConfig.tab().vecAt(SQLGenericPrms.alterTableDDLs,
        new HydraVector());
    Vector alterChoices = TestConfig.tab().vecAt(
        SQLGenericPrms.alterTableDDLChoices, null);
    if (alterChoices != null) { // user wants to run selected alter DDLs
      randDDL = Integer.valueOf((String) alterChoices
          .elementAt(SQLOldTest.random.nextInt(alterChoices.size())));
    } else { // select any alterDDL from the list
      randDDL = SQLOldTest.random.nextInt(alterStmts.size());
    }
    alterDDL = (String) alterStmts.elementAt(randDDL);
    return alterDDL.trim().toUpperCase();
    // TODO need to replace extra spaces.
  }

  public static Long isHA;

  public static boolean isHA() {
    return TestConfig.tab().booleanAt(isHA, false);
  }

  public static Long hasServerGroups;
  public static boolean hasServerGroups() {
    return TestConfig.tab().booleanAt(hasServerGroups, false);
  }

  public static Long hasWan;
  public static boolean hasWan() {
    return TestConfig.tab().booleanAt(hasWan, false);
  }

  public static Long hasView;
  public static boolean hasView() {
    return TestConfig.tab().booleanAt(hasView, false);
  }
  
  public static Long hasRedundancy;
  public static boolean hasRedundancy() {
    return TestConfig.tab().booleanAt(hasRedundancy, false);
  }

  public static Long redundancy;
  public static int redundancy() {
    return TestConfig.tab().intAt(redundancy, 0);
  }

  /**
   * Define member type. Possible values are { LOCATOR, DATASTORE, ACCESSOR,
   * CLIENT, DBsyncDatastore}
   */
  public static Long memberType;

  public static String getMemberType() {
    return TestConfig.tasktab().stringAt(memberType, null);
  }

  public static Long procedureNames;

  public static ArrayList<String> getProcedureNames() {
    ArrayList<String> procedures = new ArrayList<String>();

    Vector procList = tasktab().vecAt(
        SQLGenericPrms.procedureNames,
        TestConfig.tab()
            .vecAt(SQLGenericPrms.procedureNames, new HydraVector()));
    for (int i = 0; i < procList.size(); i++) {
      procedures.add((String) procList.elementAt(i)); // get what tables are in
                                                      // the tests
    }
    return procedures;
  }

  public static String getAnyProcedure() {
    ArrayList<String> procedures = getProcedureNames();
    int size = procedures.size();
    return procedures.get(SQLOldTest.random.nextInt(size));
  }

  public static Long functionNames;

  public static ArrayList<String> getFunctionNames() {
    ArrayList<String> functions = new ArrayList<String>();

    Vector functionList = tasktab()
        .vecAt(
            SQLGenericPrms.functionNames,
            TestConfig.tab().vecAt(SQLGenericPrms.functionNames,
                new HydraVector()));
    for (int i = 0; i < functionList.size(); i++) {
      functions.add((String) functionList.elementAt(i)); // get what tables are
                                                         // in the tests
    }
    return functions;
  }

  public static String getAnyFunction() {
    ArrayList<String> functions = getFunctionNames();
    int size = functions.size();
    return functions.get(SQLOldTest.random.nextInt(size));
  }

  public static Long ddlOperations;

  public static int[] getDDLs() {
    Vector ddls = TestConfig.tab().vecAt(SQLGenericPrms.ddlOperations,
        new HydraVector());
    int[] ddlArr = new int[ddls.size()];
    String[] strArr = new String[ddls.size()];
    for (int i = 0; i < ddls.size(); i++) {
      strArr[i] = (String) ddls.elementAt(i); // get what ddl ops are in the
                                              // tests
    }

    for (int i = 0; i < strArr.length; i++) {
      ddlArr[i] = DDLStmtFactory.getInt(strArr[i]); // convert to int array
    }
    return ddlArr;
  }

  public static List<String> getAnyIndexGroup() {
    Vector indexOperations = SQLGenericPrms.tab().vecAt(genericIndexOperations,
        null);
    List<String> indexOperationGroup = null;

    if (indexOperations != null) {
      if (indexOperations.get(0) instanceof String)
        indexOperationGroup = new ArrayList<String>(indexOperations);
      else
        indexOperationGroup = (List<String>)indexOperations
            .get(SQLOldTest.random.nextInt(indexOperations.size()));
    }
    return indexOperationGroup;
  }

  public static List<String> getDMLOperations() {
    return TestConfig.tasktab().vecAt(dmlOperations,
        SQLGenericPrms.tab().vecAt(dmlOperations, null));
  }
}
