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

/** ODBC 3.51 conformance */
#define ODBCVER 0x0351

#ifdef WIN32
#include <stdio.h>
#include <windows.h>
#endif
#ifdef PDSODBC
#include <stdio.h>
#include <pwd.h>
#include <sys/types.h> /* open() */
#include <sys/stat.h>  /* open */
#include <fcntl.h>    /* For O_RDWR */
#include <unistd.h>   /* For open(), creat() access(filename, mode) */
#include <cstring>
#endif
#include <odbcinst.h>
#include <stdlib.h>
#include <string.h>
#include <climits>
#include "gemfirexdodbcSetup/AutoPtr.h"
#include "gemfirexdodbcSetup/AutoArrayPtr.h"
#include "gemfirexdodbcSetup/ConfigSettings.h"

#ifdef IODBC
  #define TOOLNAME "iodbc-installer"
  #define DriverUnicodeType ""
#elif PDSODBC
  #define TOOLNAME "progress-installer"
  #define DriverUnicodeType ";DriverUnicodeType=1"
#else
  #define TOOLNAME "odbc-installer"
  #define DriverUnicodeType ""
#endif

#ifdef WIN32
  #define DriverDll "gemfirexdodbc.dll"
  #define SetupDll  "gemfirexdodbcSetup.dll"
#endif
#ifdef __APPLE__ 
  #ifdef IODBC
    #define DriverDll "libgemfirexdiodbc.dylib"
    #define SetupDll  "libgemfirexdiodbc.dylib"
  #else
    #define DriverDll "libgemfirexdodbc.dylib"
    #define SetupDll  "libgemfirexdodbc.dylib"
  #endif
#endif
#ifdef __linux__
  #ifdef IODBC
    #define DriverDll "libgemfirexdiodbc.so"
    #define SetupDll  "libgemfirexdiodbc.so"
  #else
    #define DriverDll "libgemfirexdodbc.so"
    #define SetupDll  "libgemfirexdodbc.so"
  #endif
#endif
const char *usage =
  "\n" \
  "---------------------------------------------------------------------------------\n" \
  "                                "TOOLNAME"                                       \n" \
  "---------------------------------------------------------------------------------\n" \
  "\n" \
  " Summary                                                                         \n" \
  "  This program can be used to install/uninstall driver                           \n" \
  "  create, edit or remove a DSN, and query DSN or driver                          \n" \
  "\n" \
  " Syntax                                                                          \n" \
  "  "TOOLNAME" <Object> <Action> [Options]                                         \n" \
  "                                                                                 \n" \
  " Object                                                                          \n" \
  "  -d driver                                                                      \n" \
  "  -s datasource (default user DSN)                                               \n" \
  "  -su user datasource                                                            \n" \
  "  -ss system datasource                                                          \n" \
  "                                                                                 \n" \
  " Action                                                                          \n" \
  "  -q query (query data source or driver)                                         \n" \
  "  -a add (add data source or install driver)                                     \n" \
  "  -e edit (edit data source)                                                     \n" \
  "  -r remove (remove data source or uninstall driver)                             \n" \
  "                                                                                 \n" \
  " Options                                                                         \n" \
  "  -n<name> (name of data source or driver)                                       \n" \
  "  -t<attribute string> String of semi delim key=value                            \n" \
  "  pairs follows this.                                                            \n" \
  "                                                                                 \n" \
  " Examples                                                                        \n" \
  "  Query all installed driver/s                                                   \n" \
  "  1. "TOOLNAME" -d -q -n                                                         \n" \
  "                                                                                 \n" \
  "  Query driver                                                                   \n" \
  "  1. "TOOLNAME" -d -q -n\"GemFire XD ODBC 1.0 Driver\"                           \n" \
  "                                                                                 \n" \
  "  Query data source                                                              \n" \
  "  1. "TOOLNAME" -s -q -n\"GemFire XD ODBC DSN\"                                  \n" \
  "                                                                                 \n" \
  "  Install driver                                                                 \n" \
  "  1. "TOOLNAME" -d -a -n\"GemFire XD ODBC 1.0 Driver\"                           \n" \
  "      -t\"GemFire XD ODBC 1.0 Driver;Driver="DriverDll";                   		  \n" \
  "    Setup="SetupDll";APILevel=2;UID=;PWD=\"               						            \n" \
  "                                                                                 \n" \
  "  Uninstall driver                                                               \n" \
  "  1. "TOOLNAME" -d -r -n\"GemFire XD ODBC 1.0 Driver\"                           \n" \
  "                                                                                 \n" \
  "  Add data source                                                                \n" \
  "  1. "TOOLNAME" -s -a -n\"GemFire XD ODBC 1.0 Driver\"                           \n" \
  "      -t\"DSN=GemFire XD ODBC DSN;Description=GemFire XD ODBC DSN;               \n" \
  "    SERVER=localhost"DriverUnicodeType"\"														            \n" \
  "                                                                                 \n" \
  "  Edit data source                                                               \n" \
  "  1."TOOLNAME" -s -e -n\"GemFire XD ODBC 1.0 Driver\"                            \n" \
  "      -t\"DSN=GemFire XD ODBC DSN;Description=GemFire XD ODBC DSN;               \n" \
  "    SERVER=127.0.0.1"DriverUnicodeType"\"					                              \n" \
  "                                                                                 \n" \
  "  Remove Data Source                                                             \n" \
  "  1. "TOOLNAME" -s -r -n\"GemFire XD ODBC DSN\"                                  \n" \
  "                                                                                 \n" \
  "---------------------------------------------------------------------------------\n" \
  "\n" \
  ;
char    cAction         = '\0';
char    cObject         = '\0';
char    cObjectSub      = '\0';
char *  pszName         = NULL;
char *  pszAttributes   = NULL;
const char *  pszOdbcDriversName = "ODBC Drivers";
const char *  pszInstalled    = "Installed";


void doPrintInstallerError( const char *pszFile, int nLine )
{
  WORD      nRecord = 1;
  DWORD     nError;
  char      szError[SQL_MAX_MESSAGE_LENGTH];
  RETCODE   nReturn;

  nReturn = SQLInstallerError( nRecord, &nError, szError, SQL_MAX_MESSAGE_LENGTH - 1, 0 );
  if ( SQL_SUCCEEDED( nReturn ) )
    fprintf( stderr, "[%s][%d][ERROR] GemFire XD ODBC Installer error %d: %s\n", pszFile, nLine, (int)nError, szError );
  else
    fprintf( stderr, "[%s][%d][ERROR] GemFire XD ODBC Installer error (unknown)\n", pszFile, nLine );
}

// All function prototypes
int doAddSqlFireDSN();
int doQuery();
int doQueryDriver();
int doQueryDriverName();
int doQueryDataSourceName();
int doAdd();
int doAddDriver();
int doEdit();
int doEditDriver();
int doEditDataSource();
int doRemove();
int doRemoveDriver();
int doRemoveDataSource();
int doRemoveDataSourceName();
char* search_ini(char* buf, int bIsInst, int doCreate);

//==================================================================================================
/// @brief Parse the specified string into a configuration map.
///
/// @param in_attributes        A list of attributes in the form of key-value pairs. The pairs will
///                             take the form of <key>=<value> with ; used as the delimiter.
///                             An example would be "DSN=sample;UID=APP;PWD=APP".
///
/// @return The specified string parsed into a map of the key/value pairs.
//==================================================================================================
ConfigurationMap* ParseAttributes(const std::string& in_attributeString)
{
  AutoPtr<ConfigurationMap> attributeMap(new ConfigurationMap());
  if (!in_attributeString.empty())
  {
	  static const std::string pairDelimiter = ";";
		static const std::string entryDelimiter = "=";
    // Split the attribute string into key/value pairs, so each entry will have a single
    // key/value.
    gemfirexd_handle beginPairSeparator = 0;
    gemfirexd_handle endPairSeparator = in_attributeString.find(pairDelimiter);

    while (beginPairSeparator != endPairSeparator)
    {
		  std::string entry(in_attributeString.substr(beginPairSeparator, (endPairSeparator - beginPairSeparator)));

      // Split this pair into a key and value.
      gemfirexd_handle entrySeparator = entry.find(entryDelimiter);
			std::string key;
			std::string value;

			if (std::string::npos != entrySeparator)
      {
        key = entry.substr(0, entrySeparator);
        value = entry.substr(entrySeparator + 1);
      }
      // Yes value can be empty
      if ((0 != key.length()) /*&& (0 != value.length())*/)
      {
        attributeMap->insert(make_pair(key, value));
      }

      // Advance to the next pair.
       beginPairSeparator = endPairSeparator + 1;
       if (beginPairSeparator == 0) break;
       endPairSeparator = in_attributeString.find(pairDelimiter, endPairSeparator + 1);
    }
  }
  return attributeMap.Detach();
}

int main( int argc, char *argv[] )
{
  int nArg;
  int nReturn;

  if ( argc <= 1 )
  {
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    exit( 1 );
  }

  for ( nArg = 1; nArg < argc; nArg++ )
  {
    if ( argv[nArg][0] == '-' )
    {
      switch ( argv[nArg][1] )
      {
        /* actions */
      case 'e':
      case 'q':
      case 'a':
      case 'r':
        cAction = argv[nArg][1];
        break;
        /* objects */
      case 'd':
      case 's':
        cObject     = argv[nArg][1];
        cObjectSub  = argv[nArg][2]; /* '\0' if not provided */
        break;
        /* options */
      case 'n':
        pszName = &(argv[nArg][2]);
        break;
      case 't':
        pszAttributes = &(argv[nArg][2]);
        break;
      default:
        {
          printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
          printf( "%s\n", usage );
          exit( 1 );
        }
      }
    }
    else
    {
      printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
      printf( "%s\n", usage );
      exit( 1 );
    }
  }

  /*
  process args
  */
  switch ( cAction )
  {
  case 'q':
    nReturn = doQuery();
    break;
  case 'a':
    nReturn = doAdd();
    break;
  case 'e':
    nReturn = doEdit();
    break;
  case 'r':
    nReturn = doRemove();
    break;
  default:
    fprintf( stderr, "[ERROR] Invalid, or missing, action.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME"\n");
    printf( "%s\n", usage );
    exit( 1 );
  }

  return ( !nReturn );
}

int doQuery()
{
  switch ( cObject )
  {
  case 'd':
    return doQueryDriver();
  case 's':
    return doQueryDataSourceName();
  default:
    fprintf( stderr, "[ERROR] Missing or invalid object type specified.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }
}

//Example: -d -q -n"GemFireXD Network Client 6.0"
//Example: -d -q
int doQueryDriver()
{
  if ( pszName && strlen(pszName) > 0)
    return doQueryDriverName();

#ifndef PDSODBC
  switch (cObjectSub) {
  case '\0':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 'u':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 's':
    if (!SQLSetConfigMode(ODBC_SYSTEM_DSN))
      return 0;
    break;
  }

  char buf[1024] = {0};
  char *drivers= buf;

  if (!SQLGetInstalledDrivers(buf, 1023, NULL))
  {
    doPrintInstallerError( __FILE__, __LINE__ );
    fprintf( stderr, "[ERROR] Failed to query driver\n");
    return 0;
  }

  while (*drivers)
  {
    printf("%s\n", drivers);
    drivers += strlen(drivers) + 1;
  }

  return 1;
#else
  char iniPath[1024];
  int isINST = 1;
  int doCreate = 0;
  char* inibuf = iniPath;
  char* buf = search_ini(inibuf, isINST, doCreate);
  if (buf == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbcinst.ini file. No drivers installed. \n");
    return 0;
  }

  std::string fn(buf); //odbcinst.ini is for driver
  std::string defaultValue; //Empty string
  AutoArrayPtr<char> keyBuf(4096); //Use 4096 as initial buffer length
  int keyBufLen = 0;
  while (keyBuf.GetLength() < INT_MAX) // Choose 32KB as max size to ensure the loop will always terminate.
  {
    keyBufLen = SQLGetPrivateProfileString(
    pszOdbcDriversName,         // The DSN or Driver to check.
    NULL,                   // NULL means get key names in this DSN or Driver.
    defaultValue.c_str(),   // Default value of empty string is unused for the case where getting key names.
    (LPSTR)keyBuf.Get(),           // Buffer to store key names in.
    (int)keyBuf.GetLength(),     // Length of the buffer.
    fn.c_str());           // Section/filename to get the keys/values from (ODBC.INI or ODBCINST.INI).
    if (keyBufLen >= ((int)keyBuf.GetLength() - 1))
    {
      // SQLGetPrivateProfileString returns bufLen - 1 if it fills the buffer completely.
      // In this case, loop and retry with a larger buffer to see if it has more keys.
      keyBuf.Attach(new char[keyBuf.GetLength() * 2], keyBuf.GetLength() * 2);
    }
    else
    {
       break;
    }
   }
   if (keyBufLen > 0)
   {
     char* keyNamePtr = keyBuf.Get();
     while (*keyNamePtr)
     {
       printf("%s\n", keyNamePtr);
       keyNamePtr += strlen(keyNamePtr) + 1;
     }
   }
  return 1;
#endif
}

//Example: -d -q -n"GemFireXD Network Client 6.0"
int doQueryDriverName()
{
  std::string str(pszName);
#ifndef PDSODBC
  std::string fn("odbcinst.ini"); //odbcinst.ini is for driver
#else
  char iniPath[1024];
  int isINST = 1;
  int doCreate = 0;
  char* inibuf = iniPath;
  char* buf = search_ini(inibuf, isINST, doCreate);
  if (buf == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbcinst.ini file. Please install driver before querying. \n");
    return 0;
  }
  std::string fn(buf); //odbcinst.ini is for driver
#endif
  std::string defaultValue; //Empty string
  AutoArrayPtr<char> keyBuf(4096); //Use 4096 as initial buffer length
  int keyBufLen = 0;
  while (keyBuf.GetLength() < INT_MAX) // Choose 32KB as max size to ensure the loop will always terminate.
  {
    keyBufLen = SQLGetPrivateProfileString(
    str.c_str(),         // The DSN or Driver to check.
    NULL,                   // NULL means get key names in this DSN or Driver.
    defaultValue.c_str(),   // Default value of empty string is unused for the case where getting key names.
    (LPSTR)keyBuf.Get(),           // Buffer to store key names in.
    (int)keyBuf.GetLength(),     // Length of the buffer.
    fn.c_str());           // Section/filename to get the keys/values from (ODBC.INI or ODBCINST.INI).
    if (keyBufLen >= ((int)keyBuf.GetLength() - 1))
    {
      // SQLGetPrivateProfileString returns bufLen - 1 if it fills the buffer completely.
      // In this case, loop and retry with a larger buffer to see if it has more keys.
      keyBuf.Attach(new char[keyBuf.GetLength() * 2], keyBuf.GetLength() * 2);
    }
    else
    {
       break;
    }
   }
   if (keyBufLen > 0)
   {
     AutoArrayPtr<char> valBuf(4096); //Use 4096 as initial buffer length
     int valBufLen = 0;
     char* keyNamePtr = keyBuf.Get();
     char *values = valBuf.Get();
     while(*keyNamePtr)
     {
       *valBuf.Get() = '\0'; //Null terminate the empty buffer in case no characters are copied into it.
       while (valBuf.GetLength() < INT_MAX)
       {
         valBufLen = SQLGetPrivateProfileString(
         str.c_str(),     // The DSN to check.
         keyNamePtr,             // Get a value for a particular key
         defaultValue.c_str(),   // Default value of empty string is unused because we know the key must exist.
         valBuf.Get(),           // Buffer to store the value in.
         (int)valBuf.GetLength(),     // Length of the buffer.
         fn.c_str());       // Section/filename to get the keys/values from (ODBC.INI or ODBCINST.INI).

         if (valBufLen >= ((int)valBuf.GetLength() - 1))
         {
           valBuf.Attach(new char[valBuf.GetLength() * 2], valBuf.GetLength() * 2);
         }
         else
         {
           break;
         }
       }
       std::string tmp(keyNamePtr);
       std::string keyName(tmp.begin(), tmp.end());
       printf("%s \t", keyName.c_str());
       std::string tmp2(valBuf.Get());
       if (tmp2.length() > 0) {
         std::string value(tmp2.begin(), tmp2.end());
         printf("%s \n", value.c_str());
       }else {
         printf(" \n");
       }
       keyNamePtr += keyName.size() + 1;
    }
   }
  return 1;
}
//Example: -d -q -n"GemFireXD Network Client 6.0"
int doQueryDataSourceName()
{
  if ( !pszName || strlen(pszName)==0)
  {
    fprintf( stderr, "[ERROR] Please provide correct data source name.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }
#ifndef PDSODBC
  std::string fn("odbc.ini"); //odbc.ini for DSN
#else
  char iniPath[1024];
  int isINST = 0;
  int doCreate = 0;
  char* inibuf = iniPath;
  char* buf = search_ini(inibuf, isINST, doCreate);
  if (buf == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbc.ini file. Please add DSN before querying. \n");
    return 0;
  }
  std::string fn(buf); //odbcinst.ini is for driver
#endif
  std::string str(pszName);
  std::string defaultValue; //Empty string
  AutoArrayPtr<char> keyBuf(4096); //Use 4096 as initial buffer length
  int keyBufLen = 0;
  while (keyBuf.GetLength() < INT_MAX) // Choose 32KB as max size to ensure the loop will always terminate.
  {
    keyBufLen = SQLGetPrivateProfileString(
    str.c_str(),         // The DSN or Driver to check.
    NULL,                   // NULL means get key names in this DSN or Driver.
    defaultValue.c_str(),   // Default value of empty string is unused for the case where getting key names.
    (LPSTR)keyBuf.Get(),           // Buffer to store key names in.
    (int)keyBuf.GetLength(),     // Length of the buffer.
    fn.c_str());           // Section/filename to get the keys/values from (ODBC.INI or ODBCINST.INI).
    if (keyBufLen >= ((int)keyBuf.GetLength() - 1))
    {
      // SQLGetPrivateProfileString returns bufLen - 1 if it fills the buffer completely.
      // In this case, loop and retry with a larger buffer to see if it has more keys.
      keyBuf.Attach(new char[keyBuf.GetLength() * 2], keyBuf.GetLength() * 2);
    }
    else
    {
       break;
    }
   }
   if (keyBufLen > 0)
   {
     AutoArrayPtr<char> valBuf(4096); //Use 4096 as initial buffer length
     int valBufLen = 0;
     char* keyNamePtr = keyBuf.Get();
     char *values = valBuf.Get();
     while(*keyNamePtr)
     {
       *valBuf.Get() = '\0'; //Null terminate the empty buffer in case no characters are copied into it.
       while (valBuf.GetLength() < INT_MAX)
       {
         valBufLen = SQLGetPrivateProfileString(
         str.c_str(),     // The DSN to check.
         keyNamePtr,             // Get a value for a particular key
         defaultValue.c_str(),   // Default value of empty string is unused because we know the key must exist.
         valBuf.Get(),           // Buffer to store the value in.
         (int)valBuf.GetLength(),     // Length of the buffer.
         fn.c_str());       // Section/filename to get the keys/values from (ODBC.INI or ODBCINST.INI).

         if (valBufLen >= ((int)valBuf.GetLength() - 1))
         {
           valBuf.Attach(new char[valBuf.GetLength() * 2], valBuf.GetLength() * 2);
         }
         else
         {
           break;
         }
       }
       std::string tmp(keyNamePtr);
       std::string keyName(tmp.begin(), tmp.end());
       printf("%s \t", keyName.c_str());
       std::string tmp2(valBuf.Get());
       if (tmp2.length() > 0) {
         std::string value(tmp2.begin(), tmp2.end());
         printf("%s \n", value.c_str());
       }
       else {
         printf(" \n");
       }
       keyNamePtr += keyName.size() + 1;
    }
   }
  return 1;
}


#ifdef PDSODBC
char* search_ini(char* buf, int bIsInst, int doCreate)
{
  int size = 1024;
  char *ptr;

   /*
   **  1. Check $ODBCINST or $ODBCINI environment variable
   */

  //printf( "[INFO] ODBCINST = %s\n", getenv("ODBCINST"));
  if ((ptr = getenv (bIsInst ? "ODBCINST" : "ODBCINI")) != NULL)
  {
    strncpy(buf, ptr, size);
    if (access (buf, R_OK) == 0){
      return buf;
    }
    else if (doCreate)
    {
      int f = open ((char *) buf, O_CREAT,
      S_IREAD | S_IWRITE | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
      if (f != -1)
      {
        close (f);
        return buf;
      }
    }
  }

   /*
   ** 2. Check $DD_INSTALLDIR 
   */

  //printf( "[INFO] DD_INSTALLDIR = %s\n", getenv("DD_INSTALLDIR"));
  /*if ((ptr = getenv ("DD_INSTALLDIR")) != NULL)
  {
    snprintf (buf, size, bIsInst ? "%s/odbcinst.ini" : "%s/odbc.ini",ptr);
    if (access (buf, R_OK) == 0)
    {
      return buf;
    }
   else if (doCreate)
   {
     int f = open ((char *) buf, O_CREAT,
     S_IREAD | S_IWRITE | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
     if (f != -1)
     {
       close (f);
       return buf;
     }
   }
  }*/

   /*
   ** 3. Check either $HOME/.odbcinst.ini or ~/.odbcinst.ini
   */

  //printf( "[INFO] HOME = %s\n", getenv("HOME"));
  if ((ptr = getenv ("HOME")) == NULL)
  {
    ptr = (char *) getpwuid (getuid ());
    if (ptr != NULL)
      ptr = ((struct passwd *) ptr)->pw_dir;
  }
  if (ptr != NULL)
  {
    snprintf (buf, size, bIsInst ? "%s/.odbcinst.ini" : "%s/.odbc.ini",ptr);
    if (access (buf, R_OK) == 0)
    {
      return buf;
    }
    else if (doCreate)
    {
      int f = open ((char *) buf, O_CREAT,
      S_IREAD | S_IWRITE | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
      if (f != -1)
      {
        close (f);
        return buf;
      }
    }
  }
  return NULL;
}
#endif

int doAdd()
{
  switch ( cObject )
  {
  case 'd':
    return doAddDriver();
  case 's':            
    return doAddSqlFireDSN();
  default:
    fprintf( stderr, "[ERROR] Missing or invalid object type specified.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }
}

/*!
\brief  Register the driver (or just increase usage count).
*/
//Example: -a -d -t"GemFireXD Network Client 4.0;Driver=E:\workspace\cheetah_dev_May13\build-artifacts\win\gemfirexd\odbc\32\gemfirexdodbc.dll;
//         Setup=E:\workspace\cheetah_dev_May13\build-artifacts\win\gemfirexd\odbc\32\gemfirexdodbcSetup.dll;APILevel=2;UID=am;PWD=am;
//         DriverODBCVer=03.51;ConnectFunctions=YYN;CPTimeout=0;Server=127.0.0.1;Port=40404"
int doAddDriver()
{
  char  *pszDriverInfo = NULL;
  char  szLoc[FILENAME_MAX];
  WORD  nLocLen;
  DWORD nUsageCount;
  int   nChar;

  if ( !pszAttributes || strlen(pszAttributes)==0 )
  {
    fprintf( stderr, "[ERROR] Please provide correct driver attributes.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }
#ifndef PDSODBC
  switch (cObjectSub) {
  case '\0':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 'u':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 's':
    if (!SQLSetConfigMode(ODBC_SYSTEM_DSN))
      return 0;
    break;
  }

  pszDriverInfo = (char *)malloc( strlen(pszAttributes) + 2 );

  for ( nChar = 0; pszAttributes[nChar]; nChar++ )
  {
    if ( pszAttributes[nChar] == ';' )
      pszDriverInfo[nChar] = '\0';
    else
      pszDriverInfo[nChar] = pszAttributes[nChar];
  }

  pszDriverInfo[nChar++]  = '\0';
  pszDriverInfo[nChar]    = '\0';
   
  if ( !SQLInstallDriverEx( pszDriverInfo, 0, szLoc, FILENAME_MAX, &nLocLen, ODBC_INSTALL_COMPLETE, &nUsageCount ) )
  {
    doPrintInstallerError( __FILE__, __LINE__ );
    fprintf( stderr, "[ERROR] Failed to install driver\n");
    free( pszDriverInfo );
    return 0;
  }

  #ifdef IODBC
  printf( "[INFO] Driver installed. \n");
  #else
  printf( "[INFO] Driver installed. Usage count is %d. Location \"%s\" \n",(int)nUsageCount, szLoc );
  #endif
  free( pszDriverInfo );

#else
  char iniPath[1024];
  int isINST = 1;
  int doCreate = 1;
  char* inibuf = iniPath;
  char* buf = search_ini(inibuf, isINST, doCreate);
  if (buf == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbcinst.ini file. Please specify correct ODBCINST environment variable. \n");
    return 0;
  }
  // This is for [ODBC Drivers] Section
  std::string iniOdbcDrivers(pszOdbcDriversName);
  std::string iniInstalled(pszInstalled);
  std::string iniDriverName(pszName);
  if (!SQLWritePrivateProfileString(
    iniOdbcDrivers.c_str(),iniDriverName.c_str(),iniInstalled.c_str(),buf))
  {
    doPrintInstallerError( __FILE__, __LINE__ );
    fprintf( stderr, "[ERROR] Failed to install driver\n");
    return 0;
  }

  //This is for actual Driver Section
  std::string attributes(pszAttributes);
  AutoPtr<ConfigurationMap> configurationMap(ParseAttributes(attributes));
  ConfigurationMap::iterator mapIter, mapEnd;
  std::string iniKey, iniValue;
  mapIter = configurationMap->begin();
  mapEnd  = configurationMap->end();
  while (mapIter != mapEnd)
  {
    iniKey.assign(mapIter->first.begin(), mapIter->first.end());
    iniValue.assign(mapIter->second.begin(), mapIter->second.end());
    if (!SQLWritePrivateProfileString(
      iniDriverName.c_str(),iniKey.c_str(),iniValue.c_str(),buf))
    {
      doPrintInstallerError( __FILE__, __LINE__ );
      fprintf( stderr, "[ERROR] Failed to install driver\n");
      return 0;
    }
    mapIter++;
  }
  printf( "[INFO] Driver installed. \n");

#endif
  return 1;
}

//Example: -s -a -n"GemFireXD Network Client 6.0" -t"DSN=GEMFIREXDODBC Data Source;Description=GEMFIREXDODBC Data Source;SERVER=localhost"
int doAddSqlFireDSN()
{
  if ( !pszName || strlen(pszName)==0)
  {
    fprintf( stderr, "[ERROR] Please provide correct DSN name.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }

  if ( !pszAttributes || strlen(pszAttributes)==0)
  {
    fprintf( stderr, "[ERROR] Please provide correct DSN attributes.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }

#ifndef PDSODBC
  switch (cObjectSub) {
  case '\0':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 'u':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 's':
    if (!SQLSetConfigMode(ODBC_SYSTEM_DSN))
      return 0;
    break;
  }
#else
  char iniPath[1024];
  int isINST = 0;
  int doCreate = 1;
  char* inibuf = iniPath;
  char* buf = search_ini(inibuf, isINST, doCreate);
  if (buf == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbc.ini file. Please specify correct ODBCINI environment variable. \n");
    return 0;
  }

 char instPath[1024];
 isINST = 1;
 doCreate = 0;
 char* instbuf = instPath;
 char* buf2 = search_ini(instbuf, isINST, doCreate);
 if (buf2 == NULL)
 {
   fprintf( stderr, "[ERROR] Could not find or open odbcinst.ini file. Please specify correct ODBCINST environment variable. \n");
   return 0;
 }

#endif
  // Parse the attribute string into a map of attributes.
	std::string attributes(pszAttributes);
  AutoPtr<ConfigurationMap> configurationMap(ParseAttributes(attributes));
  // Add the driver to the attributes.
	configurationMap->insert(make_pair(SETTING_DRIVER, std::string(pszName)));
  // Create the settings modified by the dialog.
  ConfigSettings configSettings;
  configSettings.UpdateConfiguration(*configurationMap);
#ifdef PDSODBC
  configSettings.ReadConfigurationDriver(buf2);
  configSettings.WriteConfiguration(buf);
#else
  configSettings.ReadConfigurationDriver("odbcinst.ini");
  configSettings.WriteConfiguration("odbc.ini");
#endif
  printf("[INFO] Add Data Source Done. \n");

  return 1;    
}

int doRemove()
{
  switch ( cObject )
  {
  case 'd':
    return doRemoveDriver();
  case 's':
    return doRemoveDataSource();
  default:
    fprintf( stderr, "[ERROR] Missing or invalid object type specified.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }
}

//Example: -d -r -n"GemFireXD Network Client 1.0"
int doRemoveDriver()
{
  DWORD nUsageCount;
  BOOL  bRemoveDSNs = false;

  if ( !pszName || strlen(pszName) ==0)
  {
    fprintf( stderr, "[ERROR] Please provide correct driver name.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }

#ifndef PDSODBC
  if ( !SQLRemoveDriver( pszName, bRemoveDSNs, &nUsageCount ) )
  {        
    doPrintInstallerError( __FILE__, __LINE__ );
    fprintf( stderr, "[ERROR] Failed to remove driver.\n");
    return 0;
  }

  #ifdef IODBC
  printf( "[INFO] Driver Uninstalled. \n");
  #else
  printf( "[INFO] Driver Uninstalled. Usage count is %d.\n", (int)nUsageCount );
  #endif
#else

  char iniPath[1024];
  int isINST = 1;
  int doCreate = 0;
  char* inibuf = iniPath;
  char* buf = search_ini(inibuf, isINST, doCreate);
  if (buf == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbcinst.ini file. Please install driver before its Uninstallation. \n");
    return 0;
  }
  
  //This is for actual Driver Section
  std::string iniDriverName(pszName);
  if (!SQLWritePrivateProfileString(
    iniDriverName.c_str(),NULL,NULL,buf))
  {
    doPrintInstallerError( __FILE__, __LINE__ );
    fprintf( stderr, "[ERROR] Failed to remove driver.\n");
    return 0;
  }

  // This is for [ODBC Drivers] Section
  std::string iniOdbcDrivers(pszOdbcDriversName);
  if (!SQLWritePrivateProfileString(
    iniOdbcDrivers.c_str(),iniDriverName.c_str(),NULL,buf))
  {
    doPrintInstallerError( __FILE__, __LINE__ );
    fprintf( stderr, "[ERROR] Failed to remove driver.\n");
    return 0;
  }

  printf( "[INFO] Driver Uninstalled. \n");
#endif

  return 1;
}
 //Example: -s -r -n"GEMFIREXDODBC Data Source"
int doRemoveDataSource()
{
  int bReturn = 1;
#ifndef PDSODBC
  switch ( cObjectSub )
  {
  case '\0':
    if ( !SQLSetConfigMode( ODBC_USER_DSN ) )
    {
      doPrintInstallerError( __FILE__, __LINE__ );
      fprintf( stderr, "[ERROR] Failed to remove data source.\n");
      return 0;
    }
    bReturn = doRemoveDataSourceName();
    break;
  case 'u':
    if ( !SQLSetConfigMode( ODBC_USER_DSN ) )
    {
      doPrintInstallerError( __FILE__, __LINE__ );
      fprintf( stderr, "[ERROR] Failed to remove data source.\n");
      return 0;
    }
    bReturn = doRemoveDataSourceName();
    SQLSetConfigMode( ODBC_BOTH_DSN );
    break;
  case 's':
    if ( !SQLSetConfigMode( ODBC_SYSTEM_DSN ) )
    {
      doPrintInstallerError( __FILE__, __LINE__ );
      fprintf( stderr, "[ERROR] Failed to remove data source.\n");
      return 0;
    }
    bReturn = doRemoveDataSourceName();
    SQLSetConfigMode( ODBC_BOTH_DSN );
    break;
  default:
    fprintf( stderr, "[ERROR] Missing or invalid object sub-type specified.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }

#else
  bReturn = doRemoveDataSourceName();
#endif

  return bReturn;
}

/*!
\brief  Remove data source name from ODBC system information.
*/
int doRemoveDataSourceName()
{
  if ( !pszName || strlen(pszName) == 0)
  {
    fprintf( stderr, "[ERROR] Please provide correct data source name.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }
 
  if ( SQLRemoveDSNFromIni( pszName ) == false )
  {
    doPrintInstallerError( __FILE__, __LINE__ );
    fprintf( stderr, "[ERROR] Could not remove data source .\n");
    return 0;
  }
  printf("[INFO] Remove Data Source Done. \n");
  return 1;
}

int doEdit()
{
  switch ( cObject )
  {
  case 'd':
    return doEditDriver();
  case 's':
    return doEditDataSource();
  default:
    fprintf( stderr, "[ERROR] Missing or invalid object type specified.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }
}

int doEditDriver()
{
  return 1;
}

int doEditDataSource()
{
  if ( !pszName || strlen(pszName) ==0 )
  {
    fprintf( stderr, "[ERROR] Please provide correct DSN name.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }

  if ( !pszAttributes || strlen(pszAttributes)==0 )
  {
    fprintf( stderr, "[ERROR] Please provide Correct DSN attributes.\n");
    printf("Incorrect usage, please see below for correct usage of "TOOLNAME" \n");
    printf( "%s\n", usage );
    return 0;
  }

#ifndef PDSODBC
  switch (cObjectSub) {
  case '\0':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 'u':
    if (!SQLSetConfigMode(ODBC_USER_DSN))
      return 0;
    break;
  case 's':
    if (!SQLSetConfigMode(ODBC_SYSTEM_DSN))
      return 0;
    break;
  }
#else
  char iniPath[1024];
  int isINST = 0;
  int doCreate = 1;
  char* inibuf = iniPath;
  char* buf = search_ini(inibuf, isINST, doCreate);
  if (buf == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbc.ini file. Please specify correct ODBCINI environment variable. \n");
    return 0;
  }

  char instPath[1024];
  isINST = 1;
  doCreate = 0;
  char* instbuf = instPath;
  char* buf2 = search_ini(instbuf, isINST, doCreate);
  if (buf2 == NULL)
  {
    fprintf( stderr, "[ERROR] Could not find or open odbcinst.ini file. Please specify correct ODBCINST environment variable. \n");
    return 0;
  }

#endif
  // Parse the attribute string into a map of attributes.
	std::string attributes(pszAttributes);
  AutoPtr<ConfigurationMap> configurationMap(ParseAttributes(attributes));    
  // Add the driver to the attributes.
	configurationMap->insert(make_pair(SETTING_DRIVER, std::string(pszName)));
  // Create the settings modified by the dialog.
  ConfigSettings configSettings;
  configSettings.UpdateConfiguration(*configurationMap);
#ifdef PDSODBC
  configSettings.ReadConfigurationDriver(buf2);
  configSettings.WriteConfiguration(buf);
#else
  configSettings.ReadConfigurationDriver("odbcinst.ini");
  configSettings.WriteConfiguration("odbc.ini");
#endif

  printf( "[INFO] Edit Data Source Done.\n" );
  return 1;
}

