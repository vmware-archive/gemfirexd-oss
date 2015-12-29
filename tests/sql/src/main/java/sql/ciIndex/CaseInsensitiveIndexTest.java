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
package sql.ciIndex;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;

import hydra.Log;
import sql.SQLHelper;
import sql.SQLThinClientTest;
import sql.sqlutil.ResultSetHelper;
import util.TestException;
import static sql.sqlutil.ResultSetHelper.*;


public class CaseInsensitiveIndexTest extends SQLThinClientTest {
    public static String importTableSqlTemplate = "call syscs_util.import_table_ex('T', 'TABLE_DATA', '%DataFilePath%', ',',NULL,NULL,0,0,6,0,NULL,NULL)";
    public static CaseInsensitiveIndexTest ciIndexTest = new CaseInsensitiveIndexTest();

    public static void HydraTask_createGfxdObjectsByClients() {
        ciIndexTest.createGfxdObejcts(true);
    }

    public static void HydraTask_createGfxdObjectsByPeers() {
        ciIndexTest.createGfxdObejcts(false);
    }

    public static void HydraTask_populateDataByClients() {
        ciIndexTest.populateData(true);
    }

    public static void HydraTask_populateDataByPeers() {
        ciIndexTest.populateData(false);
    }

    public static void HydraTask_queryCaseInsentiveIndexByClients() {
        ciIndexTest.queryCaseInsentiveIndex(true);
    }

    public static void HydraTask_queryCaseInsentiveIndexByPeers() {
        ciIndexTest.queryCaseInsentiveIndex(false);
    }

    @SuppressWarnings("unchecked")
    protected void queryCaseInsentiveIndex(boolean gfxdclient) {
        Vector<String> queryVec = CaseInsensitiveIndexPrms.getQueryStatements();
        Vector<String> ciQueryVec = CaseInsensitiveIndexPrms.getCiQueryStatements();
        Vector<String> comparators = CaseInsensitiveIndexPrms.getResultComparators();
        Vector<String> hints = CaseInsensitiveIndexPrms.getQueryHints();
        int index = random.nextInt(queryVec.size());
        String queryStr = queryVec.get(index);
        String ciQueryStr = ciQueryVec.get(index);
        String hintStr = index < hints.size() ? hints.get(index) : null;
        if (hintStr != null && !hintStr.trim().isEmpty()) {
            queryStr = addHint(queryStr, hintStr);
            ciQueryStr = addHint(ciQueryStr, hintStr);
        }

        Connection gConn = getGFEConnection();
        queryResultSets(gConn, queryStr, ciQueryStr, comparators.get(index));
        closeGFEConnection(gConn);
    }

    protected void queryResultSets(Connection gConn, String queryStr, String ciQueryStr, String comparator) {
        try {
            Log.getLogWriter().info("Query: " + queryStr);
            Log.getLogWriter().info("CI Query: " + ciQueryStr);
            ResultSet rs = gConn.createStatement().executeQuery(queryStr);
            ResultSet ciRs = gConn.createStatement().executeQuery(ciQueryStr);
            compareResultSets(rs, ciRs, comparator);
            commit(gConn);
        } catch (SQLException se) {
            SQLHelper.handleSQLException(se);
        }
    }

    protected String addHint(String queryStr, String hintStr) {
        final String hintPrefix = " -- GEMFIREXD-PROPERTIES ";
        final String hintPostfix = " \n ";
        int whereIdx = queryStr.toLowerCase().indexOf("where");
        if (whereIdx != -1) {
            String selectPart = queryStr.substring(0, whereIdx);
            String wherePart = queryStr.substring(whereIdx);
            return selectPart + hintPrefix + hintStr + hintPostfix + wherePart;
        }

        return queryStr + hintPrefix + hintStr + hintPostfix;
    }

    protected boolean compareResultSets(ResultSet rs, ResultSet ciRs, String comparator) {
        StructTypeImpl sti = getStructType(rs);
        List<Struct> csList = asList(rs, sti, false);
        if (csList == null) {
            return false;
        }
        Log.getLogWriter().info("ResultSet contain " + csList.size() + " rows");

        StructTypeImpl gfxdsti = getStructType(ciRs);
        List<Struct> ciList = asList(ciRs, gfxdsti, false);
        if (ciList == null) {
          return false;
        }
        Log.getLogWriter().info("Case insensitive query resultset contain " + ciList.size() + " rows");

        if (comparator.trim().equalsIgnoreCase("equal")){
            ResultSetHelper.compareResultSets(csList, ciList, "query", "ciQuery");
            return true;
        } else if (comparator.trim().equalsIgnoreCase("greater")) {
            return  csList.size() > ciList.size() ? true : false;
        } else if (comparator.trim().equalsIgnoreCase("less")) {
            return csList.size() < ciList.size() ? true : false;
        } else if (comparator.trim().equalsIgnoreCase("zero_ciquery")){
            if (csList.size() > 0 && ciList.size() == 0) {
                return true;
            } else if (ciList.size() != 0) {
                throw new TestException("CI Query should return zero resultSet");
            } else if (csList.size() <= 0) {
                throw new TestException("Query failed to return resultSet");
            } else {
              throw new TestException("Query failed to return resultSet " +
              		"and CI Query should return zero resultSet");
            }
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    protected void populateData(boolean gfxdclient) {
        Vector<String> dataFilePathVec = CaseInsensitiveIndexPrms
                .getCiIndexDataFiles();
        String fixedDatafilePath = System.getProperty( "JTESTS" ) + "/sql/ciIndex/TABLE_DATA_fixed.dat";
                fixedDatafilePath = fixedDatafilePath.replaceAll("\\\\","\\\\\\\\");
        dataFilePathVec.add(fixedDatafilePath);
        Connection conn = getGFEConnection();
        for (int i = 0; i < dataFilePathVec.size(); i++) {
            String dataFile = dataFilePathVec.get(i);
            Log.getLogWriter().info(
                    "Populating gfxd tables using data in: " + dataFile);
            String importTableSql = importTableSqlTemplate.replaceAll(
                    "%DataFilePath%", dataFile);
            try {
                SQLHelper.executeSQL(conn, importTableSql, true);
            } catch (SQLException sqle) {
                Log.getLogWriter().error(
                        "Failed in populating gfxd tables using " + dataFile);
            }
        }

        Log.getLogWriter().info("done populating gfxd tables.");
        commit(conn);
        closeGFEConnection(conn);
    }

    protected void createGfxdObejcts(boolean gfxdclient) {
        Connection conn = getGFEConnection();
        String ciIndexDDLPath = CaseInsensitiveIndexPrms
                .getCiIndexDDLFilePath();
        Log.getLogWriter().info(
                "creating gfxd objects in gfxd using sql script: "
                        + ciIndexDDLPath);
        SQLHelper.runSQLScript(conn, ciIndexDDLPath, true);
        Log.getLogWriter().info("done creating database objects in gfxd.");
        commit(conn);
        closeGFEConnection(conn);
    }
}
