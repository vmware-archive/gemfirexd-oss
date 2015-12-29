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

#define EMPLOYEE_ID_LEN 10
#define TESTNAME "SQLBindParameter"
#define TABLE "BINDPARAM"

#define MAX_NAME_LEN 256
#define MAX_SCALE 2

SQLSMALLINT sCustID;
SQLCHAR szEmployeeID[EMPLOYEE_ID_LEN];
SQL_DATE_STRUCT dsOrderDate;
SQLLEN cbCustID = 0, cbOrderDate = 0, cbEmployeeID = SQL_NTS;
SQLCHAR szQuote[50] = "100084";
SQLINTEGER cbValue = SQL_NTS;
#define DESC_LEN 51
#define ARRAY_SIZE 2

typedef struct tagPartStruct
{
  SQLREAL Price;
  SQLINTEGER PartID;
  SQLCHAR Desc[DESC_LEN];
  SQLLEN PriceInd;
  SQLLEN PartIDInd;
  SQLLEN DescLenOrInd;
} PartStruct;


BEGIN_TEST(testSQLBindparameter)
{
  // -------------------------------------------------------------------------
      SQLINTEGER sErr = 0;
      CHAR create[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR insert[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR select[MAX_NAME_LEN + MAX_NAME_LEN + 1];
      CHAR drop[MAX_NAME_LEN + 1];
      CHAR tabname[MAX_NAME_LEN];
      CHAR buffer[MAX_NAME_LEN];

      CHAR rgbValueChar[MAX_NAME_LEN + 1];
      SQLSMALLINT rgbValueSmallint;
      SQLINTEGER rgbValueInteger;
      float rgbValueFloat;
      double rgbValueDouble;
      DATE_STRUCT rgbValueDate;
      TIME_STRUCT rgbValueTime;

      SQLLEN pcbValue;

      //initialize the sql handles
      INIT_SQLHANDLES

      // --- Create Table ---------------------------------------------
      strcpy(tabname, TABLE);
      strcpy(create, "CREATE TABLE ");
      strcat(create, tabname);
      strcat(create,
          " (TYP_CHAR CHAR(30), TYP_SMALLINT SMALLINT, TYP_INTEGER INTEGER,");
      strcat(create,
          " TYP_FLOAT FLOAT, TYP_DOUBLE DECIMAL(13,2), TYP_DATE DATE, TYP_TIME TIME) ");
      LOGF("Create Stmt = '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      // --- Insert Table ---------------------------------------------
      // --- 1. ---
      strcpy(insert, "INSERT INTO ");
      strcat(insert, tabname);
      strcat(insert,
          " (TYP_CHAR, TYP_SMALLINT, TYP_INTEGER, TYP_FLOAT, TYP_DOUBLE, TYP_DATE, TYP_TIME )");
      strcat(insert, "VALUES (?, ?, ?, ?, ?, ?, ?)");
      LOGF("Insert Stmt = '%s'\r\n", insert);

      retcode = SQLPrepare(hstmt, (SQLCHAR*)insert, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLPrepare");

      // *** SQLBindParameter --------------------------------------------
      pcbValue = SQL_NTS;
      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_CHAR, MAX_NAME_LEN, 0, rgbValueChar, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter (SQL_CHAR)");

      pcbValue = SQL_NTS;
      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_SHORT,
          SQL_SMALLINT, 0, 0, &rgbValueSmallint, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter (SQL_SMALLINT)");

      pcbValue = SQL_NTS;
      retcode = SQLBindParameter(hstmt, 3, SQL_PARAM_INPUT, SQL_C_LONG,
          SQL_INTEGER, 0, 0, &rgbValueInteger, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter (SQL_INTEGER)");

      pcbValue = SQL_NTS;
      retcode = SQLBindParameter(hstmt, 4, SQL_PARAM_INPUT, SQL_C_FLOAT,
          SQL_FLOAT, 0, 0, &rgbValueFloat, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter (SQL_FLOAT)");

      pcbValue = SQL_NTS;
      retcode = SQLBindParameter(hstmt, 5, SQL_PARAM_INPUT, SQL_C_DOUBLE,
          SQL_DOUBLE, 0, MAX_SCALE, &rgbValueDouble, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter (SQL_DOUBLE)");

      pcbValue = SQL_NTS;
      retcode = SQLBindParameter(hstmt, 6, SQL_PARAM_INPUT, SQL_C_TYPE_DATE,
          SQL_TYPE_DATE, 0, 0, &rgbValueDate, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter (SQL_TYPE_DATE)");

      pcbValue = SQL_NTS;
      retcode = SQLBindParameter(hstmt, 7, SQL_PARAM_INPUT, SQL_C_TYPE_TIME,
          SQL_TYPE_TIME, 0, 0, &rgbValueTime, 0, &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter (SQL_TYPE_TIME)");

      // *** Set values --------------------------------------------------
      strcpy(rgbValueChar, "Testname");
      rgbValueSmallint = 44;
      rgbValueInteger = 1234567890;
      rgbValueFloat = (SFLOAT)1234;
      rgbValueDouble = 1234567890.12;
      rgbValueDate.year = 1994;
      rgbValueDate.month = 12;
      rgbValueDate.day = 8;
      rgbValueTime.hour = 11;
      rgbValueTime.minute = 55;
      rgbValueTime.second = 30;

      retcode = SQLExecute(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecute");

      strcpy(rgbValueChar, "---");
      rgbValueSmallint = 0;
      rgbValueInteger = 0;
      rgbValueFloat = (SFLOAT)0;
      rgbValueDouble = 0;
      rgbValueDate.year = 0;
      rgbValueDate.month = 0;
      rgbValueDate.day = 0;
      rgbValueTime.hour = 0;
      rgbValueTime.minute = 0;
      rgbValueTime.second = 0;

      retcode = SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeHandle (HSTMT)");

      // --- Select Table ---------------------------------------------
      // --- 1. ---

      strcpy(select, "SELECT ");
      strcat(select,
          "TYP_CHAR, TYP_SMALLINT, TYP_INTEGER, TYP_FLOAT, TYP_DOUBLE, TYP_DATE, TYP_TIME ");
      strcat(select, " FROM ");
      strcat(select, tabname);
      LOGF("Select Stmt= '%s'\r\n", select);

      retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLAllocHandle (HSTMT)");

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)select, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 1, SQL_C_CHAR, rgbValueChar, MAX_NAME_LEN,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol (SQL_CHAR)");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 2, SQL_C_SHORT, &rgbValueSmallint, 0,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol (SQL_SMALLINT)");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 3, SQL_C_LONG, &rgbValueInteger, 0,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol (SQL_INTEGER)");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 4, SQL_C_FLOAT, &rgbValueFloat, 0,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol (SQL_FLOAT)");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 5, SQL_C_DOUBLE, &rgbValueDouble, 0,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol (SQL_DOUBLE)");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 6, SQL_C_TYPE_DATE, &rgbValueDate, 0,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol (SQL_DATE)");

      pcbValue = SQL_NTS;
      retcode = SQLBindCol(hstmt, 7, SQL_C_TYPE_TIME, &rgbValueTime, 0,
          &pcbValue);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol (SQL_TIME)");

      retcode = SQLFetch(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFetch");

      LOGF("Select Values --> ");
      printf("\trgbValueChar         = '%s'\n", rgbValueChar);
      printf("\trgbValueSmallint     = '%d'\n", rgbValueSmallint);
      printf("\trgbValueInteger      = '%ld'\n", rgbValueInteger);
      sprintf(buffer, "%.0f", rgbValueFloat);
      printf("\trgbValueFloat        = '%s'\n", buffer);
      sprintf(buffer, "%.2f", rgbValueDouble);
      printf("\trgbValueDouble       = '%s'\n", buffer);
      printf("\trgbValueDate(Year)   = '%d'\n", rgbValueDate.year);
      printf("\trgbValueDate(Month)  = '%d'\n", rgbValueDate.month);
      printf("\trgbValueDate(Day)    = '%d'\n", rgbValueDate.day);
      printf("\trgbValueTime(Hour)   = '%d'\n", rgbValueTime.hour);
      printf("\trgbValueTime(Minute) = '%d'\n", rgbValueTime.minute);
      printf("\trgbValueTime(second) = '%d'\n", rgbValueTime.second);

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      // --- Drop Table -----------------------------------------------
      strcpy(drop, "DROP TABLE ");
      strcat(drop, tabname);
      LOGF("Drop Stmt= '%s'\r\n", drop);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)drop, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      //free sql handles
      FREE_SQLHANDLES
}
END_TEST(testSQLBindparameter)

BEGIN_TEST(testSQLBindparameterColumnBinding)
{
      //initialize the sql handles
     INIT_SQLHANDLES

      //--------------------- Create table Parts----------------------------------------
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP TABLE IF EXISTS PARTS ",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"CREATE TABLE PARTS (PartID INTEGER, Description VARCHAR(100), Price FLOAT)",
              SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      //--------------------------------------------------------------------------------
      SQLCHAR * Statement =
          (SQLCHAR*)"INSERT INTO Parts (PartID, Description,  Price) "
              "VALUES (?, ?, ?)";
      SQLUINTEGER PartIDArray[ARRAY_SIZE];
      SQLCHAR DescArray[ARRAY_SIZE][DESC_LEN];
      SQLREAL PriceArray[ARRAY_SIZE];
      SQLLEN PartIDIndArray[ARRAY_SIZE], DescLenOrIndArray[ARRAY_SIZE],
          PriceIndArray[ARRAY_SIZE];
      SQLUSMALLINT i, ParamStatusArray[ARRAY_SIZE];
      SQLULEN ParamsProcessed;

      memset(DescLenOrIndArray, 0, sizeof(DescLenOrIndArray));
      memset(PartIDIndArray, 0, sizeof(PartIDIndArray));
      memset(PriceIndArray, 0, sizeof(PriceIndArray));

      // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
      // column-wise binding.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE,
          SQL_PARAM_BIND_BY_COLUMN, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Specify the number of elements in each parameter array.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE,
          (SQLPOINTER)ARRAY_SIZE, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Specify an array in which to return the status of each set of
      // parameters.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR,
          ParamStatusArray, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Specify an SQLUINTEGER value in which to return the number of sets of
      // parameters processed.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
          &ParamsProcessed, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Bind the parameters in column-wise fashion.
      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_ULONG,
          SQL_INTEGER, 5, 0, PartIDArray, 0, PartIDIndArray);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_CHAR, DESC_LEN - 1, 0, DescArray, DESC_LEN, DescLenOrIndArray);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 3, SQL_PARAM_INPUT, SQL_C_FLOAT,
          SQL_REAL, 7, 0, PriceArray, 0, PriceIndArray);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      // Set part ID, description, and price.
      for (i = 0; i < ARRAY_SIZE; i++) {
        PartIDArray[i] = i;
        sprintf((char*)DescArray[i], "Description for part %d", i);
        LOGF("DescArray[i] is %s", DescArray[i]);
        PriceArray[i] = i * 1.23;
        PartIDIndArray[0] = 0;
        DescLenOrIndArray[0] = SQL_NTS;
        PriceIndArray[0] = 0;
      }

      // Execute the statement.
      retcode = SQLExecDirect(hstmt, Statement, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      // Check to see which sets of parameters were processed successfully.
      LOGF("Parameter processed : %d\n", ParamsProcessed);

}
END_TEST(testSQLBindparameterColumnBinding)


BEGIN_TEST(testSQLBindparameterRowBinding)
{

      PartStruct PartArray[ARRAY_SIZE];
      SQLCHAR * Statement =
          (SQLCHAR*)"INSERT INTO Parts (PartID, Description, Price) "
              "VALUES (?, ?, ?)";
      SQLUSMALLINT i, ParamStatusArray[ARRAY_SIZE];
      SQLULEN ParamsProcessed;

      //initialize the sql handles
      INIT_SQLHANDLES

      //--------------------- Create table Parts----------------------------------------
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP TABLE IF EXISTS PARTS ",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"CREATE TABLE PARTS (PartID INTEGER, Description VARCHAR(100), Price FLOAT)",
              SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      // Set the SQL_ATTR_PARAM_BIND_TYPE statement attribute to use
      // column-wise binding.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_BIND_TYPE,
          (SQLPOINTER)sizeof(PartStruct), 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Specify the number of elements in each parameter array.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMSET_SIZE,
          (SQLPOINTER)ARRAY_SIZE, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Specify an array in which to return the status of each set of
      // parameters.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAM_STATUS_PTR,
          ParamStatusArray, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Specify an SQLUINTEGER value in which to return the number of sets of
      // parameters processed.
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
          &ParamsProcessed, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      // Bind the parameters in row-wise fashion.
      retcode = SQLBindParameter(hstmt, 1, SQL_PARAM_INPUT, SQL_C_ULONG,
          SQL_INTEGER, 5, 0, &PartArray[0].PartID, 0, &PartArray[0].PartIDInd);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR,
          SQL_CHAR, DESC_LEN - 1, 0, PartArray[0].Desc, DESC_LEN,
          &PartArray[0].DescLenOrInd);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      retcode = SQLBindParameter(hstmt, 3, SQL_PARAM_INPUT, SQL_C_FLOAT,
          SQL_REAL, 7, 0, &PartArray[0].Price, 0, &PartArray[0].PriceInd);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindParameter");

      // Set part ID, description, and price.
      for (i = 0; i < ARRAY_SIZE; i++) {
        PartArray[i].PartID = i;
        sprintf((char*)PartArray[i].Desc, "Description for part %d", i);
        LOGF("DescArray[i] is %s", PartArray[i].Desc);
        PartArray[i].Price = i * 1.23;
        PartArray[0].PartIDInd = 0;
        PartArray[0].DescLenOrInd = SQL_NTS;
        PartArray[0].PriceInd = 0;
      }

      // Execute the statement.
      retcode = SQLExecDirect(hstmt, Statement, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      // Check to see which sets of parameters were processed successfully.
      LOGF("Parameter processed : %d\n", ParamsProcessed);
}
END_TEST(testSQLBindparameterRowBinding)

