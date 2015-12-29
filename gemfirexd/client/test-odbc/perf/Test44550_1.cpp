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
 
// ODBC_simple_testConsole.cpp : Defines the entry point for the console application.
//
#include <ace/ACE.h>
#undef ACE_SCANDIR_CMP_USES_VOIDPTR
#include <ace/OS.h>
#include <ace/Task.h>
#include <ace/Barrier.h>



#include "stdafx.h"

#include "windows.h"
#include "psapi.h"
#include <string>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <fstream>
 

extern "C"
{
#include <sql.h>
#include <sqlext.h>
}

void printStatementError(SQLHSTMT stmt, int line)
{
  SQLCHAR sqlState[6];
  SQLINTEGER errorCode;
  SQLCHAR message[8192];
  SQLSMALLINT messageLen;
  ::SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, sqlState, &errorCode, message,
      8191, &messageLen);
  std::cout << "Statement before line " << line << " failed with SQLState("
      << sqlState << ", ErrorCode=" << errorCode << ": " << message << "\n";
}


class SelectTask: public ACE_Task_Base
{
private:
  SQLHENV m_env;
  std::string& m_connStr;
  ACE_Barrier& m_barrier;
  const int m_numRows;
  const int m_numRuns;
	const int m_numThreads;
	const int m_crudFlag;

public:
  inline SelectTask(SQLHENV env, std::string& connStr, ACE_Barrier& barrier,
      int numRows, int numRuns, int numThreads, int crudFlag) :
      m_env(env), m_connStr(connStr), m_barrier(barrier), m_numRows(numRows),
      m_numRuns(numRuns), m_numThreads(numThreads), m_crudFlag(crudFlag)
  {
  }

  int svc()
  {
    SQLHDBC conn;
    SQLHSTMT stmt;

		int Lower_Bound;
		int Upper_Bound;
		int Range;

		int custCount = 1000;
		int prodCount = 10000;


    ::SQLAllocHandle(SQL_HANDLE_DBC, m_env, &conn);

    if (!SQL_SUCCEEDED(::SQLDriverConnect(conn, NULL, (SQLCHAR*)m_connStr.c_str(),
            m_connStr.size(), NULL, 0, NULL, SQL_DRIVER_NOPROMPT))) {
      // connection failed
      SQLCHAR sqlState[6];
      SQLINTEGER errorCode;
      SQLCHAR message[8192];
      SQLSMALLINT messageLen;
      ::SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sqlState, &errorCode, message,
          8191, &messageLen);
      std::cout << "Connection failed for thread "
          << ACE_OS::thr_self() + " with SQLState(" << sqlState
          << ", ErrorCode=" << errorCode << ": " << message << "\n";
      return -1;
    }

    ::SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);



		// insert table data
		if (m_crudFlag)
		{
			if (!SQL_SUCCEEDED(::SQLPrepare(stmt, (SQLCHAR*)"insert into customer values "
							"(?, ?)", SQL_NTS))) {
				printStatementError(stmt, __LINE__);
				return 2;
			}

			int id;
			char* name;
			SQLRETURN ret;
			::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
					&id, sizeof(id), NULL);

			std::cout << "Starting inserts into customer table\n";
			for (id = 1; id <= custCount; id++) {
				name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "customer%d", id);
				::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
						0, 0, name, SQL_NTS, NULL);
				ret = ::SQLExecute(stmt);
				::free(name);
				if (!SQL_SUCCEEDED(ret)) {
					printStatementError(stmt, __LINE__);
					return 2;
				}

				SQLLEN count;
				if (!SQL_SUCCEEDED(::SQLRowCount(stmt, &count))) {
					printStatementError(stmt, __LINE__);
					return 2;
				}
				if (count != 1) {
					std::cerr << "unexpected count for single insert: " << count << "\n";
					return 2;
				}
				//if ((id % 500) == 0) {
				//  std::cout << "Completed " << id << " inserts ...\n";
				//}
			}

			if (!SQL_SUCCEEDED(::SQLPrepare(stmt, (SQLCHAR*)"insert into product values "
							"(?, ?)", SQL_NTS))) {
				printStatementError(stmt, __LINE__);
				return 2;
			}

			::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
					&id, sizeof(id), NULL);

			std::cout << "Starting inserts into product table\n";
			for (id = 1; id <= prodCount; id++) {
				name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "product%d", id);
				::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
						0, 0, name, SQL_NTS, NULL);
				ret = ::SQLExecute(stmt);
				::free(name);
				if (!SQL_SUCCEEDED(ret)) {
					printStatementError(stmt, __LINE__);
					return 2;
				}

				SQLLEN count;
				if (!SQL_SUCCEEDED(::SQLRowCount(stmt, &count))) {
					printStatementError(stmt, __LINE__);
					return 2;
				}
				if (count != 1) {
					std::cerr << "unexpected count for single insert: " << count << "\n";
					return 2;
				}
				//if ((id % 500) == 0) {
				//  std::cout << "Completed " << id << " inserts ...\n";
				//}
			}

			Lower_Bound = 1;
			Upper_Bound = custCount * m_numThreads;

			if (!SQL_SUCCEEDED(::SQLPrepare(stmt, (SQLCHAR*)"insert into new_order values "
							"(?, ?, ?)", SQL_NTS))) {
				printStatementError(stmt, __LINE__);
				return 2;
			}

			::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0,
					&id, sizeof(id), NULL);

			std::cout << "Starting inserts into new_order table\n";
			for (id = 1; id <= m_numRows; id++) {
				name = (char*)::malloc(100 * sizeof(char));
				Range = rand()% (Upper_Bound -  Lower_Bound + 1) + Lower_Bound;
				::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
						0, 0, &Range, sizeof(Range), NULL);
				::sprintf(name, "customer%d-with-order%d", Range, id);
				::SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
						0, 0, name, SQL_NTS, NULL);
				ret = ::SQLExecute(stmt);
				::free(name);
				if (!SQL_SUCCEEDED(ret)) {
					printStatementError(stmt, __LINE__);
					return 2;
				}

				SQLLEN count;
				if (!SQL_SUCCEEDED(::SQLRowCount(stmt, &count))) {
					printStatementError(stmt, __LINE__);
					return 2;
				}
				if (count != 1) {
					std::cerr << "unexpected count for single insert: " << count << "\n";
					return 2;
				}
				//if ((id % 500) == 0) {
				//  std::cout << "Completed " << id << " inserts ...\n";
				//}
			}

			Lower_Bound = 1;
			Upper_Bound = prodCount * m_numThreads;

			if (!SQL_SUCCEEDED(::SQLPrepare(stmt, (SQLCHAR*)"insert into order_detail values "
							"(?, ?, ?)", SQL_NTS))) {
				printStatementError(stmt, __LINE__);
				return 2;
			}

			std::cout << "Starting inserts into order_detail table\n";
			for (id = 1; id <= m_numRows; id++) {
				::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 
						0, 0,	&id, sizeof(id), NULL);
				Range = rand()% (Upper_Bound -  Lower_Bound + 1) + Lower_Bound;
				::SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
						0, 0, &Range, sizeof(Range), NULL);
				::SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER,
						0, 0, &id, sizeof(id), NULL);
				ret = ::SQLExecute(stmt);
				if (!SQL_SUCCEEDED(ret)) {
					printStatementError(stmt, __LINE__);
					return 2;
				}

				SQLLEN count;
				if (!SQL_SUCCEEDED(::SQLRowCount(stmt, &count))) {
					printStatementError(stmt, __LINE__);
					return 2;
				}
				if (count != 1) {
					std::cerr << "unexpected count for single insert: " << count << "\n";
					return 2;
				}
				//if ((id % 500) == 0) {
				//  std::cout << "Completed " << id << " inserts ...\n";
				//}
			}

		}


    //// wait for all threads
    m_barrier.wait();

    int status = 0;



		if (!m_crudFlag)
		{
			::SQLPrepare(stmt, (SQLCHAR*)"SELECT * FROM order_detail od JOIN new_order n ON od.order_id = n.no_o_id FETCH NEXT ? ROWS ONLY", SQL_NTS);

			std::cout << "Starting timed selects for thread " << ACE_OS::thr_self()
					<< "\n";


			int status = 0;
			int rowNum, count;
			::SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0,
					0, &count, sizeof(count), NULL);
			for (int i = 1; i <= m_numRuns; i++) {
				rowNum = (i % m_numRows) + 1;
				count = (rowNum % 98);
				if (i == 100 || i == 1000 || i == 10000 || i == 20000 || i == 30000 || i == 40000 || i == 50000)
					count = 100000;
				//if (count > 90000)
				//	std::cout << "count is " << count << "\n";
				::SQLExecute(stmt);

				::SQLCloseCursor(stmt);
			}
		}

    return status;
  }
};

int _tmain(int argc, char** argv)
{
  if (argc != 3 && argc != 4) {
    std::cerr << "Usage: <script> <server> <port> [<threads>]\n";
    return 1;
  }

	char* server = argv[1];
	char* port = argv[2];
	int numThreads = 1;
	char* name;

  if (argc == 4) {
    numThreads = std::atoi(argv[3]);
    if (numThreads <= 0) {
      std::cerr << "unexpected number of threads " << numThreads << "\n";
      return 1;
    }
  }

  std::string connStr;
	connStr.append("Server=").append(server).append(";Port=").append(port);
	//connStr.append("Server=").append("127.0.0.1").append(";Port=").append("1552");


  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT stmt;

  if (!SQL_SUCCEEDED(::SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env))) {
    SQLCHAR sqlState[6];
    SQLINTEGER errorCode;
    SQLCHAR message[8192];
    SQLSMALLINT messageLen;
    ::SQLGetDiagRec(SQL_HANDLE_ENV, env, 1, sqlState, &errorCode, message,
        8191, &messageLen);
    std::cout << "Initialization failed with SQLState(" << sqlState
      << ", ErrorCode=" << errorCode << ": " << message << "\n";
    return 1;
  }


	  if (!SQL_SUCCEEDED(::SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
          (void*)SQL_OV_ODBC3, 0))) {
    SQLCHAR sqlState[6];
    SQLINTEGER errorCode;
    SQLCHAR message[8192];
    SQLSMALLINT messageLen;
    ::SQLGetDiagRec(SQL_HANDLE_ENV, env, 1, sqlState, &errorCode, message,
        8191, &messageLen);
    std::cout << "Initialization failed with SQLState(" << sqlState
      << ", ErrorCode=" << errorCode << ": " << message << "\n";
    return 1;
  }

  ::SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  std::cout << "Connecting to " << server << ":" << port <<
    "; connection string: " << connStr << "\n";
  if (!SQL_SUCCEEDED(::SQLDriverConnect(conn, NULL, (SQLCHAR*)connStr.c_str(),
      connStr.size(), NULL, 0, NULL, SQL_DRIVER_NOPROMPT))) {
    // connection failed
    SQLCHAR sqlState[6];
    SQLINTEGER errorCode;
    SQLCHAR message[8192];
    SQLSMALLINT messageLen;
    ::SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sqlState, &errorCode, message,
        8191, &messageLen);
    std::cout << "Connection failed with SQLState(" << sqlState
      << ", ErrorCode=" << errorCode << ": " << message << "\n";
    return 1;
  }

  ::SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);

  ::SQLExecDirect(stmt, (SQLCHAR*)"drop table if exists order_detail", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"drop table if exists product", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"drop table if exists new_order", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"drop table if exists customer", SQL_NTS);

  // create the tables
  ::SQLExecDirect(stmt, (SQLCHAR*)"create table customer ("
    "c_id           integer generated by default as identity,"
    "c_name         varchar(100)"
    ") partition by (c_id) redundancy 1", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"create table new_order ("
    "no_o_id  integer generated by default as identity,"
		"c_id			integer	null,"
    "no_name  varchar(100) not null"
    ") partition by (no_o_id) colocate with (customer) redundancy 1", SQL_NTS);

  
  ::SQLExecDirect(stmt, (SQLCHAR*)"create index ndx_customer_name "
    "on customer (c_id, c_name)", SQL_NTS);

  ::SQLExecDirect(stmt, (SQLCHAR*)"create index ndx_neworder_o_id "
    "on new_order (no_o_id)", SQL_NTS);

  ::SQLExecDirect(stmt, (SQLCHAR*)"insert into customer values (1, 'customer1')", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"insert into customer values (DEFAULT, 'customer1')", SQL_NTS);

  ::SQLExecDirect(stmt, (SQLCHAR*)"insert into new_order values (1, 1, 'customer1-with-order1')", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"insert into new_order values (DEFAULT, 'customer1-with-order1')", SQL_NTS);

  ::SQLExecDirect(stmt, (SQLCHAR*)"create table product ("
    "no_p_id  integer generated by default as identity,"
    "no_productname  varchar(100) not null"
    ") partition by (no_p_id) redundancy 1", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"insert into product values (1, 'product1')", SQL_NTS);
  ::SQLExecDirect(stmt, (SQLCHAR*)"insert into product values (DEFAULT, 'product1')", SQL_NTS);


  ::SQLExecDirect(stmt, (SQLCHAR*)"create table order_detail ("
    "order_id			integer not null,"
		"product_id		integer	not null,"
    "quantity			integer not null"
    ") partition by (order_id) colocate with (new_order) redundancy 1", SQL_NTS);




  std::cout << "Created tables\n";
  std::cout << "Will use " << numThreads - 1 << " threads for inserts and selects\n";

  const int numRows = 150000;

  const int numRuns = 50000;




// system  info captured here at the beginning

	// Use to convert bytes to MB
#define DIV 1048576

#define WIDTH 7


  MEMORYSTATUSEX statex;

  statex.dwLength = sizeof (statex);

  GlobalMemoryStatusEx (&statex);

	std::ofstream log_file("log_file.txt", std::ios::out );

			log_file << "Before processing: \n" << std::endl;

			name = (char*)::malloc(100 * sizeof(char));
			time_t now = time(NULL);
			strftime(name, 100, "Begin Time: %Y-%m-%d %H:%M:%S\n\n", localtime(&now));
			log_file << name << std::endl;
			::free(name);


					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There is  %*ld percent of memory in use.\n", WIDTH, statex.dwMemoryLoad);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d total Mbytes of physical memory.\n", WIDTH, statex.ullTotalPhys/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of physical memory.\n", WIDTH, statex.ullAvailPhys/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d total Mbytes of paging file.\n", WIDTH, statex.ullTotalPageFile/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of paging file.\n", WIDTH, statex.ullAvailPageFile/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d total Mbytes of virtual memory.\n", WIDTH, statex.ullTotalVirtual/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of virtual memory.\n", WIDTH, statex.ullAvailVirtual/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of extended memory.\n", WIDTH, statex.ullAvailExtendedVirtual/DIV);
			log_file << name << std::endl;
				::free(name);




 

  ACE::init();

  // timed runs
  ACE_Barrier barrier(numThreads);
	ACE_Time_Value start, end;
  time_t startSecs, endSecs;
  suseconds_t startUSecs, endUSecs;
  SelectTask** tasks = NULL;

  if (numThreads > 1) {
    // create the other threads
    tasks = new SelectTask*[numThreads - 1];
    for (int i = 0; i < (numThreads - 1); i++) {
      tasks[i] = new SelectTask(env, connStr, barrier, numRows, numRuns, numThreads - 1, 1);
      tasks[i]->activate();
    }
  }

  barrier.wait();


  if (numThreads > 1) {
    // wait for other threads to join
    for (int i = 0; i < (numThreads - 1); i++) {
      tasks[i]->wait();
    }
  }


  // cleanup
  if (numThreads > 1) {
    for (int i = 0; i < (numThreads - 1); i++) {
      delete tasks[i];
    }
    delete tasks;
  }


  if (numThreads > 1) {
		// create the other threads
		tasks = new SelectTask*[numThreads - 1];
		for (int i = 0; i < (numThreads - 1); i++) {
			tasks[i] = new SelectTask(env, connStr, barrier, numRows, numRuns, numThreads - 1, 0);
			tasks[i]->activate();
		}
  }

  barrier.wait();
  start = ACE_OS::gettimeofday();

  if (numThreads > 1) {
    // wait for other threads to join
    for (int i = 0; i < (numThreads - 1); i++) {
      tasks[i]->wait();
    }
  }
  end = ACE_OS::gettimeofday();

  startSecs = start.sec();
  startUSecs = start.usec();
  endSecs = end.sec();
  endUSecs = end.usec();

  if (startUSecs > endUSecs) {
    endSecs--;
    endUSecs += 1000000;
  }
  std::cout << "Total Time taken for Selects: " << (endSecs - startSecs) << '.'
    << (endUSecs - startUSecs) << "s\n";

  // cleanup
  if (numThreads > 1) {
    for (int i = 0; i < (numThreads - 1); i++) {
      delete tasks[i];
    }
    delete tasks;
  }


	  GlobalMemoryStatusEx (&statex);

		log_file << "After processing: \n" << std::endl;

				name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There is  %*ld percent of memory in use.\n", WIDTH, statex.dwMemoryLoad);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d total Mbytes of physical memory.\n", WIDTH, statex.ullTotalPhys/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of physical memory.\n", WIDTH, statex.ullAvailPhys/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d total Mbytes of paging file.\n", WIDTH, statex.ullTotalPageFile/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of paging file.\n", WIDTH, statex.ullAvailPageFile/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d total Mbytes of virtual memory.\n", WIDTH, statex.ullTotalVirtual/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of virtual memory.\n", WIDTH, statex.ullAvailVirtual/DIV);
			log_file << name << std::endl;
				::free(name);

					name = (char*)::malloc(100 * sizeof(char));
				::sprintf(name, "There are %*I64d free Mbytes of extended memory.\n\n", WIDTH, statex.ullAvailExtendedVirtual/DIV);
			log_file << name << std::endl;
				::free(name);

				name = (char*)::malloc(100 * sizeof(char));
			now = time(NULL);
			strftime(name, 100, "End Time: %Y-%m-%d %H:%M:%S\n", localtime(&now));
			log_file << name << std::endl;
			::free(name);

	log_file.close();

  ACE::fini();

  //::GFXDreeHandle(SQL_HANDLE_STMT, stmt);
  ::SQLDisconnect(conn);
  //::GFXDreeHandle(SQL_HANDLE_DBC, conn);
  ::GFXDreeHandle(SQL_HANDLE_ENV, env);

	return 0;
}

