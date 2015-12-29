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
 
#include "GemFireXDHelper.h"
using namespace std;

//*-------------------------------------------------------------------------
#define TESTNAME "SQLBindCol"
#define TABLE "BINDCOL"

#define MAX_NAME_LEN 50

//*-------------------------------------------------------------------------

BEGIN_TEST(testSQLBindCol)
{
  /*------------------------------------------------------------------------- */
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];

      CHAR szChar[MAX_NAME_LEN + 1], szFixed[MAX_NAME_LEN + 1];
      double sFloat;

      SQLLEN cbChar, cbFloat, cbFixed;

      //initialize the sql handles
      INIT_SQLHANDLES

      /* --- Create Table --------------------------------------------- */
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create, " (TYP_CHAR CHAR(30), TYP_FLOAT FLOAT(15),");
      strcat(create, " TYP_DEC DECIMAL(15,2) )");
      LOGF("Create Stmt = '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Insert Table --------------------------------------------- */
      /* --- 1. ---*/
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert, " VALUES ('Dies ein Datatype-Test.', 123456789,");
      strcat(insert, " 98765321.99)");
      LOGF("Insert Stmt = '%s'", insert);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* --- Select Table --------------------------------------------- */
      /* --- 1. --- */
      strcpy(select, "SELECT ");
      strcat(select, "TYP_CHAR, TYP_FLOAT, TYP_DEC ");
      strcat(select, " FROM ");
      strcat(select, tabname);
      LOGF("Select Stmt= '%s'", select);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLBindCol(hstmt, 1, SQL_C_CHAR, szChar, MAX_NAME_LEN,
          &cbChar);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLBindCol(hstmt, 2, SQL_C_DOUBLE, &sFloat, 0, &cbFloat);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLBindCol(hstmt, 3, SQL_C_CHAR, szFixed, MAX_NAME_LEN,
          &cbFixed);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      while (1) {
        retcode = SQLFetch(hstmt);
        if (retcode == SQL_NO_DATA_FOUND) break;
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
            "SQLFetch");
      }
      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      /* --- Drop Table ----------------------------------------------- */
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      /* ---------------------------------------------------------------------har- */

      /* - Disconnect ---------------------------------------------------- */
      //free sql handles
      FREE_SQLHANDLES

    }
    END_TEST(testSQLBindCol)

#define ROW_ARRAY_SIZE 10000

BEGIN_TEST(testSQLBindCol_ColumnBinding)
{

      SQLUINTEGER OrderIDArray[ROW_ARRAY_SIZE], NumRowsFetched;
      SQLCHAR SalesPersonArray[ROW_ARRAY_SIZE][20],
          StatusArray[ROW_ARRAY_SIZE][10];
      SQLLEN OrderIDIndArray[ROW_ARRAY_SIZE],
          SalesPersonLenOrIndArray[ROW_ARRAY_SIZE],
          StatusLenOrIndArray[ROW_ARRAY_SIZE];
      SQLUSMALLINT RowStatusArray[ROW_ARRAY_SIZE];

      //initialize sql hanldles
      INIT_SQLHANDLES

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"drop table if exists Orders",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"create table Orders(OrderID int, SalesPerson VARCHAR(20), Status VARCHAR(10))",
              SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(1, 'Abhishek', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(2, 'Ajay', 'CANCEL')", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(3, 'Amol', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(4, 'Amey', 'CANCEL')", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(5, 'Amogh', 'CANCEL')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(6, 'Avinash', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(7, 'Deepak', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(8, 'Sachin', 'CANCEL')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(9, 'Virat', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(10, 'Ajinkya', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(11, 'Ishan', 'CANCEL')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      for (int i = 0; i < ROW_ARRAY_SIZE; i++) {
        retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(12, 'Atul', 'CANCEL')",
          SQL_NTS);
        DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      }

      // Set the SQL_ATTR_ROW_BIND_TYPE statement attribute to use
      // column-wise binding. Declare the rowset size with the
      // SQL_ATTR_ROW_ARRAY_SIZE statement attribute. Set the
      // SQL_ATTR_ROW_STATUS_PTR statement attribute to point to the
      // row status array. Set the SQL_ATTR_ROWS_FETCHED_PTR statement
      // attribute to point to cRowsFetched.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE,
          SQL_BIND_BY_COLUMN, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
          (SQLPOINTER)ROW_ARRAY_SIZE, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR, RowStatusArray,
          0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROWS_FETCHED_PTR,
          &NumRowsFetched, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Bind arrays to the OrderID, SalesPerson, and Status columns.
      retcode = SQLBindCol(hstmt, 1, SQL_C_ULONG, OrderIDArray,
          sizeof(OrderIDArray[0]), OrderIDIndArray);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");
      retcode = SQLBindCol(hstmt, 2, SQL_C_CHAR, SalesPersonArray,
          sizeof(SalesPersonArray[0]), SalesPersonLenOrIndArray);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");
      retcode = SQLBindCol(hstmt, 3, SQL_C_CHAR, StatusArray,
          sizeof(StatusArray[0]), StatusLenOrIndArray);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      // Execute a statement to retrieve rows from the Orders table.
      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"SELECT OrderID, SalesPerson, Status FROM Orders order by OrderID",
              SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      retcode = SQLFetch(hstmt);

      LOGF("Rows Fetched count is %d", NumRowsFetched);

      for (int i = 0; i < ROW_ARRAY_SIZE; i++) {
        LOGF("OrderID = %d SalesPerson= %s Status= %s", OrderIDArray[i],
            (char* )SalesPersonArray[i], (char* )StatusArray[i]);
        LOGF(
            "Order ID indicator = %ld SalesPerson Indicator = %dl Status Indicator = %ld",
            OrderIDIndArray[i], SalesPersonLenOrIndArray[i],
            StatusLenOrIndArray[i]);
      }

      ASSERT(OrderIDArray[0] == 1, "order ID retrieved is incoorect");
      ASSERT(strcmp((char* )SalesPersonArray[0], "Abhishek") == 0,
          "SalesPersonArray retrieved is incoorect");
      ASSERT(strcmp((char* )StatusArray[0], "CONFIRM") == 0,
          "StatusArray retrieved is incoorect");

      ASSERT(OrderIDArray[1] == 2, "order ID retrieved is incoorect");
      ASSERT(strcmp((char* )SalesPersonArray[1], "Ajay") == 0,
          "SalesPersonArray retrieved is incoorect");
      ASSERT(strcmp((char* )StatusArray[1], "CANCEL") == 0,
          "StatusArray retrieved is incoorect");

      LOGF("Rows Fetched count is %d", NumRowsFetched);
      ASSERT(NumRowsFetched == ROW_ARRAY_SIZE, "Number of fetched rows is incorrect");

      //free sql handles
      FREE_SQLHANDLES
}
END_TEST(testSQLBindCol_ColumnBinding)

BEGIN_TEST(testSQLBindCol_RowBinding)
{
  // Define the ORDERINFO struct and allocate an array of 10 structs.
      typedef struct
      {
        SQLUINTEGER OrderID;
        SQLLEN OrderIDInd;
        SQLCHAR SalesPerson[20];
        SQLLEN SalesPersonLenOrInd;
        SQLCHAR Status[10];
        SQLLEN StatusLenOrInd;
      } ORDERINFO;
      ORDERINFO OrderInfoArray[ROW_ARRAY_SIZE];

      SQLULEN NumRowsFetched;
      SQLUSMALLINT RowStatusArray[ROW_ARRAY_SIZE], i;

      //initialize sql hanldles
      INIT_SQLHANDLES

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"drop table if exists Orders",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"create table Orders(OrderID int, SalesPerson VARCHAR(20), Status VARCHAR(10))",
              SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(1, 'Abhishek', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(2, 'Ajay', 'CANCEL')", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(3, 'Amol', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(4, 'Amey', 'CANCEL')", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(5, 'Amogh', 'CANCEL')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(6, 'Avinash', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(7, 'Deepak', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(8, 'Sachin', 'CANCEL')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(9, 'Virat', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(10, 'Ajinkya', 'CONFIRM')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(11, 'Ishan', 'CANCEL')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      for (int i = 0; i < ROW_ARRAY_SIZE; i++) {
      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"insert into Orders values(12, 'Atul', 'CANCEL')",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");
      }

      // Specify the size of the structure with the SQL_ATTR_ROW_BIND_TYPE
      // statement attribute. This also declares that row-wise binding will
      // be used. Declare the rowset size with the SQL_ATTR_ROW_ARRAY_SIZE
      // statement attribute. Set the SQL_ATTR_ROW_STATUS_PTR statement
      // attribute to point to the row status array. Set the
      // SQL_ATTR_ROWS_FETCHED_PTR statement attribute to point to
      // NumRowsFetched.
      SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(ORDERINFO), 0);
      SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)ROW_ARRAY_SIZE, 0);
      SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR, RowStatusArray, 0);
      SQLSetStmtAttr(hstmt, SQL_ATTR_ROWS_FETCHED_PTR, &NumRowsFetched, 0);

      // Bind elements of the first structure in the array to the OrderID,
      // SalesPerson, and Status columns.
      SQLBindCol(hstmt, 1, SQL_C_ULONG, &OrderInfoArray[0].OrderID,
          sizeof(OrderInfoArray[0].OrderID), &OrderInfoArray[0].OrderIDInd);
      SQLBindCol(hstmt, 2, SQL_C_CHAR, OrderInfoArray[0].SalesPerson,
          sizeof(OrderInfoArray[0].SalesPerson),
          &OrderInfoArray[0].SalesPersonLenOrInd);
      SQLBindCol(hstmt, 3, SQL_C_CHAR, OrderInfoArray[0].Status,
          sizeof(OrderInfoArray[0].Status), &OrderInfoArray[0].StatusLenOrInd);

      // Execute a statement to retrieve rows from the Orders table.
      SQLExecDirect(hstmt,
          (SQLCHAR*)"SELECT OrderID, SalesPerson, Status FROM Orders order by OrderID",
          SQL_NTS);

      // Fetch up to the rowset size number of rows at a time. Print the actual
      // number of rows fetched; this number is returned in NumRowsFetched.
      // Check the row status array to print only those rows successfully
      // fetched. Code to check if rc equals SQL_SUCCESS_WITH_INFO or
      // SQL_ERRORnot shown.

      retcode = SQLFetch(hstmt);


      for (int i = 0; i < ROW_ARRAY_SIZE; i++) {
        LOGF("OrderID = %d SalesPerson= %s Status= %s", OrderInfoArray[i].OrderID,
            (char* )OrderInfoArray[i].SalesPerson, (char* )OrderInfoArray[i].Status);
        LOGF(
            "Order ID indicator = %d SalesPerson Indicator = %d Status Indicator = %d",
            OrderInfoArray[i].OrderIDInd, OrderInfoArray[i].SalesPersonLenOrInd,
            OrderInfoArray[i].StatusLenOrInd);
      }

      ASSERT(OrderInfoArray[0].OrderID == 1, "order ID retrieved is incorrect");
       ASSERT(strcmp((char* )OrderInfoArray[0].SalesPerson, "Abhishek") == 0,
       "SalesPersonArray retrieved is incorrect");
       ASSERT(strcmp((char* )OrderInfoArray[0].Status, "CONFIRM") == 0,
       "StatusArray retrieved is incorrect");

       ASSERT(OrderInfoArray[1].OrderID == 2, "order ID retrieved is incorrect");
       ASSERT(strcmp((char* )OrderInfoArray[1].SalesPerson, "Ajay") == 0,
       "SalesPersonArray retrieved is incorrect");
       ASSERT(strcmp((char* )OrderInfoArray[1].Status, "CANCEL") == 0,
       "StatusArray retrieved is incorrect");

       LOGF("Rows Fetched count is %d", NumRowsFetched);

       ASSERT(NumRowsFetched == ROW_ARRAY_SIZE, "Number of fetched rows is incorrect");

      //free sql handles
      FREE_SQLHANDLES

}
END_TEST(testSQLBindCol_RowBinding)
