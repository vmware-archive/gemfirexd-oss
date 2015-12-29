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

#include "stdafx.h"

#include "AutoPtr.h"
#include <odbcinst.h>
#include <map>
#include <vector>

#include "ConfigSettings.h"
#include "MainDialog.h"

#include <ctime>
#include <fstream>




static gemfirexd_handle s_moduleId = 0;


//==================================================================================================
/// @brief DLL entry point.
///
/// @param in_module        Handle of the current module.
/// @param in_reasonForCall DllMain called with parameter telling if DLL is loaded into process or 
///                         thread address space.
/// @param in_reserved      Unused.
///
/// @return TRUE if successful, FALSE otherwise.
//==================================================================================================
BOOL APIENTRY DllMain( HMODULE hModule,
                       DWORD  ul_reason_for_call,
                       LPVOID lpReserved
					 )
{
	switch (ul_reason_for_call)
	{
	case DLL_PROCESS_ATTACH:
		s_moduleId = reinterpret_cast<gemfirexd_handle>(hModule);
		break;
	case DLL_THREAD_ATTACH:
	case DLL_THREAD_DETACH:
	case DLL_PROCESS_DETACH:
		break;
	}

	return TRUE;
}

//==================================================================================================
/// @brief String splitter based on some delimiter
///
/// @param source        A source string that needs to be tokenized.
///
///				 delimiter		 Provided delimiter used to split the source string
///
/// @return Tokenized string in a vector
//==================================================================================================
std::vector<std::string> StringSplit(const std::string &source, const char *delimiter = "|", bool keepEmpty = false)
{
    std::vector<std::string> results;

    size_t prev = 0;
    size_t next = 0;

    while ((next = source.find_first_of(delimiter, prev)) != std::string::npos)
    {
        if (keepEmpty || (next - prev != 0))
        {
            results.push_back(source.substr(prev, next - prev));
        }
        prev = next + 1;
    }

    if (prev < source.size())
    {
        results.push_back(source.substr(prev));
    }

    return results;
}

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
					std::string entry(in_attributeString.substr(beginPairSeparator, endPairSeparator));

            // Split this pair into a key and value.
            gemfirexd_handle entrySeparator = entry.find(entryDelimiter);

						std::string key;
						std::string value;

						if (std::string::npos != entrySeparator)
            {
                key = entry.substr(0, entrySeparator);
                value = entry.substr(entrySeparator + 1);
            }

            if ((0 != key.length()) && (0 != value.length()))
            {
                attributeMap->insert(make_pair(key, value));
            }

            // Advance to the next pair.
            beginPairSeparator = endPairSeparator;
            endPairSeparator = in_attributeString.find(pairDelimiter, endPairSeparator + 1);
        }
    }

    return attributeMap.Detach();
}

//==================================================================================================
/// @brief Install, config, or deletes driver from the system information. 
///
///
/// @param hwndParent           The parent window handle.
/// @param fRequest             Contains the type of request, one of the following:
///                                 ODBC_INSTALL_DRIVER
///                                 ODBC_CONFIG_DRIVER
///                                 ODBC_CONFIG_DRIVER_MAX
///                                 ODBC_REMOVE_DRIVER
/// @param lpszDriver						The name of the driver as registered in the Odbcinst.ini key 
///															of the system information
///
/// @param lpszArgs						  A null-terminated string containing arguments for a driver-specific fRequest.
///
/// @param lpszMsg							A null-terminated string containing an output message from the driver setup.				
///
/// @param cbMsgMax							Length of lpszMsg. 
///
/// @param pcbMsgOut						Total number of bytes available to return in lpszMsg. 
///
/// @return TRUE if the function succeeds; FALSE otherwise.
//==================================================================================================
extern "C" { 
BOOL INSTAPI
ConfigDriver (HWND hwndParent,
    WORD fRequest,
    LPCSTR lpszDriver,
    LPCSTR lpszArgs, LPSTR lpszMsg, WORD cbMsgMax, WORD FAR * pcbMsgOut)
{
	const int _MAX_LEN = 1024;
	char driverread[_MAX_LEN] = {0};
	BOOL retcode = false;
	UWORD confMode = ODBC_USER_DSN;
	int out = 0;
	WORD	x;
	DWORD	y;
	DWORD dwUsageCount = 0;
	BOOL bResult = false;
	char	sDesc[_MAX_LEN] = {0};					
	char	szInPath[_MAX_LEN] = {0};	
	char	szOutPath[_MAX_LEN] = {0};	
	char	szInputArgs[_MAX_LEN] = {0};	
	char drive[_MAX_DRIVE] = {0};
	char dir[_MAX_DIR] = {0};
	char fname[_MAX_FNAME] = {0};
	char ext[_MAX_EXT] = {0};


  // Map the request User/System
  if (fRequest < ODBC_INSTALL_DRIVER || fRequest > ODBC_CONFIG_DRIVER_MAX)
    {
      SQLPostInstallerError (ODBC_ERROR_INVALID_REQUEST_TYPE, NULL);
      goto done;
    }

  if (!lpszDriver || !strlen (lpszDriver))
    {
      SQLPostInstallerError (ODBC_ERROR_INVALID_NAME, NULL);
      goto done;
    }

  // Get config mode
  SQLGetConfigMode (&confMode);

  // Check type of request
  switch (fRequest)
    {
    case ODBC_INSTALL_DRIVER:
		{
      if (!lpszArgs || !strlen (lpszArgs))
				{
					SQLPostInstallerError (ODBC_ERROR_REQUEST_FAILED,
							"No enough parameters for installation.");
					goto done;
				}

			// local initializations
			memset ( szOutPath, 0, _MAX_LEN );
			memset ( szInPath, 0, _MAX_LEN );
			memset ( driverread, 0, _MAX_LEN );

			// prepare in string as per above shown format
			memset ( sDesc, 0, _MAX_LEN );
			strcpy_s ( sDesc, lpszDriver );
			strcpy_s ( szInPath, lpszArgs );
			
		  out = SQLGetPrivateProfileString ("ODBC 32 bit Drivers", sDesc,
			 "", driverread, sizeof (driverread), "odbcinst.ini");

			// Install driver
			SQLSetConfigMode (confMode);

			if (!SQLInstallDriverEx ( "GemFireXD Network Client 1.0\0Driver=gemfirexdodbc.dll\0Setup=gemfirexdodbcSetup.dll\0\0", (strlen(szInPath) > 0) ? szInPath : NULL, szOutPath,
								_MAX_LEN, &x, ODBC_INSTALL_COMPLETE, &y))
				{
					SQLPostInstallerError (ODBC_ERROR_REQUEST_FAILED,
							"Could not add the driver informations.");
					goto done;
				}

			break;
		}
    case ODBC_CONFIG_DRIVER:
		{
      if (!lpszArgs || !strlen (lpszArgs))
				{
					SQLPostInstallerError (ODBC_ERROR_REQUEST_FAILED,
							"No enough parameters for configururation.");
					goto done;
				}

			std::vector<std::string> results = StringSplit(std::string(lpszArgs), "|");

      // Add each key-value pair
      for(std::vector<std::string>::iterator it = results.begin(); it != results.end(); ++it)
			{
					std::vector<std::string> drvName = StringSplit(*it, "=");

					strcpy_s (driverread, drvName[0].c_str());

					SQLSetConfigMode (confMode);
					if (!SQLWritePrivateProfileString (lpszDriver, driverread, (drvName.size() > 1) ? drvName[1].c_str() : "",
						"odbcinst.ini"))
						goto done;
			}
      break;
		}
    case ODBC_REMOVE_DRIVER:
		{
      // Removing driver from ODBC store
      if (!lpszArgs || !strlen (lpszArgs))
      {
        SQLPostInstallerError (ODBC_ERROR_REQUEST_FAILED,
          "No enough parameters for removing driver.");
        goto done;
      }

      // local initializations
      // prepare in string as per above shown format
      memset ( sDesc, 0, _MAX_LEN );
      strcpy_s ( sDesc, lpszDriver );

      SQLSetConfigMode (confMode);

      // call SQLConfigDriver instead of SQLRemoveDriver to prevent recursion.
      bResult = SQLConfigDriver(NULL, ODBC_REMOVE_DRIVER, lpszDriver, lpszArgs, lpszMsg, cbMsgMax, pcbMsgOut );

			if (bResult && (dwUsageCount <= 0))
			{
				//  Removes information about the driver from the Odbcinst.ini entry in the system information.  
				SQLConfigDriver(
                 NULL,                          
                 ODBC_REMOVE_DRIVER,            
                 sDesc,    
                 "",                           
                 "",                           
                 0,                             
                 NULL );     
			}

      if (!bResult)
			{
				SQLPostInstallerError (ODBC_ERROR_REQUEST_FAILED,
						"Could not remove driver informations.");
				goto done;
			}
		  break;
		}
    default:
      SQLPostInstallerError (ODBC_ERROR_REQUEST_FAILED, NULL);
      goto done;
    };

    retcode = true;

done:
		{
			if (pcbMsgOut)
				*pcbMsgOut = 0;
		}

  return retcode;
}
}

//==================================================================================================
/// @brief Adds, modifies, or deletes data sources from the system information. This is done through
/// a dialog which will prompt for user interaction.
///
/// @param in_parentWindow      The parent window handle.
/// @param in_requestType       Contains the type of request, one of the following:
///                                 ODBC_ADD_DSN
///                                 ODBC_CONFIG_DSN
///                                 ODBC_REMOVE_DSN
///                                 ODBC_ADD_SYS_DSN
///                                 ODBC_CONFIG_SYS_DSN
///                                 ODBC_REMOVE_SYS_DSN
///                                 ODBC_REMOVE_DEFAULT_DSN
/// @param in_driverDescription The driver description (usually the name of the DBMS) presented to
///                             the user instead of the physical driver name.
/// @param in_attributes        A list of attributes in the form of key-value pairs. The pairs will
///                             take the form of <key>=<value> with ; used as the delimiter.
///                             An example would be "DSN=sample;UID=APP;PWD=APP".
///
/// @return TRUE if the function succeeds; FALSE otherwise.
//==================================================================================================
BOOL INSTAPI ConfigDSN(
   HWND in_parentWindow,
   WORD in_requestType,
   LPCSTR in_driverDescription,
   LPCSTR in_attributes)
{
    BOOL isSuccessful = false;

    // Parse the attribute string into a map of attributes.
		std::string attributes(in_attributes);
    AutoPtr<ConfigurationMap> configurationMap(ParseAttributes(attributes));

    // Add the driver to the attributes.
		configurationMap->insert(make_pair(SETTING_DRIVER, std::string(in_driverDescription)));

    switch (in_requestType)
    {
        case ODBC_REMOVE_DSN:
        case ODBC_REMOVE_SYS_DSN:
        {
            // Take the same action if removing either a User or System DSN.
					std::string dsn((*configurationMap)[SETTING_DSN]);
          isSuccessful = SQLRemoveDSNFromIni(dsn.c_str());

            break;
        }

        case ODBC_ADD_DSN:
        case ODBC_ADD_SYS_DSN:
        {                
            try
            {
                // Create the main dialog.
                MainDialog mainDialog;

                // Create the settings modified by the dialog.
                ConfigSettings configSettings;
                configSettings.UpdateConfiguration(*configurationMap);
                configSettings.ReadConfigurationDriver("odbcinst.ini");

                mainDialog.Show(in_parentWindow, s_moduleId, configSettings, false);
                mainDialog.Dispose(in_parentWindow);
            }
            catch (...)
            {
                ; // Do nothing, allow following code to cleanup.
            }

            break;
        }

        case ODBC_CONFIG_DSN:
        case ODBC_CONFIG_SYS_DSN:
        {
            try
            {
                // Create the main dialog.
                MainDialog mainDialog;

                // Create the settings modified by the dialog.
                ConfigSettings configSettings;
                configSettings.UpdateConfiguration(*configurationMap);
                configSettings.ReadConfigurationDSN();

                mainDialog.Show(in_parentWindow, s_moduleId, configSettings, true);
                mainDialog.Dispose(in_parentWindow);
            }
            catch (...)
            {
                ; // Do nothing, allow following code to cleanup.
            }

            break;
        }
    }

    return isSuccessful;
}

