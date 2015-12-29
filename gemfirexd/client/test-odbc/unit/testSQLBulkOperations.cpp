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

#define MAX_NAME_LEN 100

// Define structure for customer data (assume 10 byte maximum bookmark size).
typedef struct tagCustStruct
{
  SQLCHAR Bookmark[10];SQLLEN BookmarkLen;
  SQLUINTEGER CustomerID;SQLLEN CustIDInd;
  SQLCHAR CompanyName[51];SQLLEN NameLenOrInd;
  SQLCHAR Address[51];SQLLEN AddressLenOrInd;
  SQLCHAR Phone[11];SQLLEN PhoneLenOrInd;
} CustStruct;

// Allocate 40 of these structures. Elements 0-9 are for the current rowset,
// elements 10-19 are for the buffered updates, elements 20-29 are for
// the buffered inserts, and elements 30-39 are for the buffered deletes.
#define ROWSET_SIZE 10000
CustStruct CustArray[ROWSET_SIZE];
SQLUSMALLINT RowStatusArray[ROWSET_SIZE];

BEGIN_TEST(testSQLBulkOperations1)
{
      CHAR create[MAX_NAME_LEN + 1];

      //initialize the sql handles
      INIT_SQLHANDLES

      // Set the following statement attributes:
      // SQL_ATTR_CURSOR_TYPE:           Keyset-driven
      // SQL_ATTR_ROW_BIND_TYPE:         Row-wise
      // SQL_ATTR_ROW_ARRAY_SIZE:        10
      // SQL_ATTR_USE_BOOKMARKS:         Use variable-length bookmarks
      // SQL_ATTR_ROW_STATUS_PTR:        Points to RowStatusArray
      // SQL_ATTR_ROW_BIND_OFFSET_PTR:   Points to BindOffset
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_CURSOR_TYPE,
          (SQLPOINTER)SQL_CURSOR_KEYSET_DRIVEN, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_TYPE,
          (SQLPOINTER)sizeof(CustStruct), 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
          (SQLPOINTER)ROWSET_SIZE, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR, RowStatusArray,
          0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_BIND_OFFSET_PTR, &CustArray,
          0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      /* --- Create Table . ------------------------------------------ */
      strcpy(create,
          "CREATE TABLE Customers (CustomerID INTEGER, CompanyName CHAR(51), Address CHAR(51), Phone CHAR(11))");
      printf("\tCreate Stmt (Table1: CUSTOMERS)= '%s'\r\n", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      // Bind arrays to the CustomerID, CompanyName, Address, and Phone columns.
      retcode = SQLBindCol(hstmt, 1, SQL_C_ULONG, &CustArray[0].CustomerID, 0,
          &CustArray[0].CustIDInd);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLBindCol(hstmt, 2, SQL_C_CHAR, CustArray[0].CompanyName,
          sizeof(CustArray[0].CompanyName), &CustArray[0].NameLenOrInd);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLBindCol(hstmt, 3, SQL_C_CHAR, CustArray[0].Address,
          sizeof(CustArray[0].Address), &CustArray[0].AddressLenOrInd);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLBindCol(hstmt, 4, SQL_C_CHAR, CustArray[0].Phone,
          sizeof(CustArray[0].Phone), &CustArray[0].PhoneLenOrInd);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      for (int i = 0; i < ROWSET_SIZE; i++) {
        CustArray[i].CustomerID = i;
        CustArray[i].CustIDInd = sizeof(CustArray[i].CustomerID);
        strcpy((char*)CustArray[i].CompanyName, "vmware software");
        CustArray[i].NameLenOrInd = sizeof(CustArray[i].CompanyName);
        strcpy((char*)CustArray[i].Address, "Pune India");
        CustArray[i].AddressLenOrInd = sizeof(CustArray[i].Address);
        strcpy((char*)CustArray[i].Phone, "9999999999");
        CustArray[i].PhoneLenOrInd = sizeof(CustArray[i].Phone);
      }

      // Execute a statement to retrieve rows from the Customers table.
      retcode =
          SQLExecDirect(hstmt,
              (SQLCHAR*)"SELECT CustomerID, CompanyName, Address, Phone FROM Customers",
              SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      SQLBulkOperations(hstmt, SQL_ADD);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      SQLLEN rowcount = 0;
      retcode = SQLBindCol(hstmt, 1, SQL_C_LONG, &rowcount, 0, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"SELECT count(*) AS ROWCOUNT FROM Customers", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLFetch(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFetch");

      LOGF("Rowcount returned is %d", rowcount);

      ASSERT(rowcount == ROWSET_SIZE,
          "ROWSET_SIZE and rowcount is not matching");

      // Execute a statement to retrieve rows from the Customers table.
      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"DROP TABLE IF EXISTS Customers", SQL_NTS);

      // Close the cursor.
      FREE_SQLHANDLES
}
END_TEST(testSQLBulkOperations1)

BEGIN_TEST(testSQLBulkOperations2)
{
      CHAR create[MAX_NAME_LEN + 1];

      //initialize the sql handles
      INIT_SQLHANDLES

      /* --- Create Table . ------------------------------------------ */
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP TABLE IF EXISTS CUSTOMER",
          SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      strcpy(create,
          "CREATE TABLE CUSTOMER (Cust_Num INTEGER, First_Name VARCHAR(51), Last_Name VARCHAR(51))");
      LOGF("Create Stmt (Table1: CUSTOMER)= '%s'", create);

      retcode = SQLExecDirect(hstmt, (SQLCHAR*)create, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* declare and initialize local variables        */
      SQLCHAR sqlstmt[] =
          "SELECT Cust_Num, First_Name, Last_Name FROM CUSTOMER";
      SQLLEN Cust_Num[ROWSET_SIZE];
      SQLCHAR First_Name[ROWSET_SIZE][21];
      SQLCHAR Last_Name[ROWSET_SIZE][21];
      SQLLEN Cust_Num_L[ROWSET_SIZE];
      SQLLEN First_Name_L[ROWSET_SIZE];
      SQLLEN Last_Name_L[ROWSET_SIZE];
      SQLUSMALLINT rowStatus[ROWSET_SIZE];

      /* Set pointer to row status array               */
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_STATUS_PTR,
          (SQLPOINTER)rowStatus, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      /* Execute query */
      retcode = SQLExecDirect(hstmt, sqlstmt, SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      /* Call SQLBindCol() for each result set column  */
      retcode = SQLBindCol(hstmt, 1, SQL_C_LONG, (SQLPOINTER)Cust_Num,
          (SQLLEN)sizeof(Cust_Num) / ROWSET_SIZE, Cust_Num_L);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLBindCol(hstmt, 2, SQL_C_CHAR, (SQLPOINTER)First_Name,
          (SQLLEN)sizeof(First_Name) / ROWSET_SIZE, First_Name_L);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLBindCol(hstmt, 3, SQL_C_CHAR, (SQLPOINTER)Last_Name,
          (SQLLEN)sizeof(Last_Name) / ROWSET_SIZE, Last_Name_L);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");
      /* For each column, place the new data values in */
      /* the rgbValue array, and set each length value */
      /* in the pcbValue array to be the length of the */
      /* corresponding value in the rgbValue array.    */

      for (int i = 0; i < ROWSET_SIZE; i++) {
        Cust_Num[i] = i + 1;
        Cust_Num_L[i] = sizeof(SQLLEN);
        strcpy((char*)First_Name[i], "abc");
        First_Name_L[i] = strlen("abc");
        strcpy((char*)Last_Name[i], "xyz");
        Last_Name_L[i] = strlen("xyz");
      }

      /* Set number of rows to insert                  */
      retcode = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE,
          (SQLPOINTER)ROWSET_SIZE, 0);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLSetStmtAttr");

      /* Perform the bulk insert                       */
      retcode = SQLBulkOperations(hstmt, SQL_ADD);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBulkOperations");

      retcode = SQLFreeStmt(hstmt, SQL_CLOSE);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLFreeStmt");

      retcode = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLAllocHandle");

      SQLLEN rowcount = 0;
      retcode = SQLBindCol(hstmt, 1, SQL_C_LONG, &rowcount, 0, NULL);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLBindCol");

      retcode = SQLExecDirect(hstmt,
          (SQLCHAR*)"SELECT count(*) AS ROWCOUNT FROM CUSTOMER", SQL_NTS);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode,
          "SQLExecDirect");

      retcode = SQLFetch(hstmt);
      DIAGRECCHECK(SQL_HANDLE_STMT, hstmt, 1, SQL_SUCCESS, retcode, "SQLFetch");

      LOGF("Rowcount returned is %d", rowcount);

      ASSERT(rowcount == ROWSET_SIZE,
          "ROWSET_SIZE and rowcount is not matching");

      // Execute a statement to retrieve rows from the Customers table.
      retcode = SQLExecDirect(hstmt, (SQLCHAR*)"DROP TABLE IF EXISTS CUSTOMER",
          SQL_NTS);

      // Close the handles.
      FREE_SQLHANDLES
}
END_TEST(testSQLBulkOperations)

