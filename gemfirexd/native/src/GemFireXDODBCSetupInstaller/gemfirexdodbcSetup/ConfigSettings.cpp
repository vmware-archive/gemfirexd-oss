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

//#include "stdafx.h"
#include "ConfigSettings.h"
#include "AutoArrayPtr.h"
#include <odbcinst.h>
#include <climits>



// Public ==========================================================================================
////////////////////////////////////////////////////////////////////////////////////////////////////
ConfigSettings::ConfigSettings()
{
    // Clear configuration map
    m_configuration.clear();
}

////////////////////////////////////////////////////////////////////////////////////////////////////
ConfigSettings::~ConfigSettings()
{
    ; // Do nothing.
}

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& ConfigSettings::GetDSN()
{
    return m_configuration[SETTING_DSN];
}

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& ConfigSettings::GetDescription()
{
    return m_configuration[SETTING_DESCRIPTION];
}

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& ConfigSettings::GetSERVER()
{
    return m_configuration[SETTING_SERVER];
}

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& ConfigSettings::GetPORT()
{
    return m_configuration[SETTING_PORT];
}

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& ConfigSettings::GetPWD()
{
    return m_configuration[SETTING_PWD];
}

////////////////////////////////////////////////////////////////////////////////////////////////////
const std::string& ConfigSettings::GetUID()
{
    return m_configuration[SETTING_UID];
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::SetDSN(const std::string& in_dsn)
{
#ifndef PDSODBC
    if (SQLValidDSN((LPCSTR)in_dsn.c_str()))
    {
        m_configuration[SETTING_DSN] = in_dsn;
        m_dsn = in_dsn;
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::SetDescription(const std::string& in_description)
{
    m_configuration[SETTING_DESCRIPTION] = in_description;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::SetSERVER(const std::string& in_server)
{
    m_configuration[SETTING_SERVER] = in_server;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::SetPORT(const std::string& in_port)
{
    m_configuration[SETTING_PORT] = in_port;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::SetPWD(const std::string& in_pwd)
{
    m_configuration[SETTING_PWD] = in_pwd;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::SetUID(const std::string& in_uid)
{
    m_configuration[SETTING_UID] = in_uid;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// Reads DSN's key/value pairs from ODBCINST.INI and inserts them into the configuration map.  
/// There is no validation on the odbc.ini entries at this point.
///
/// New ODBCINST.ini entries are added to the configuration map.
/// Existing odbc.ini entries are updated in the configuration map.
/// Missing odbc.ini entries are kept in the configuration map (using defaults not previously set
/// value).
////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::ReadConfigurationDSN()
{
	std::string dsnKey(SETTING_DSN);

    // Retrieves DSN's key value pairs
    // Note: m_dsn should have been specified via UpdateConfiguration before the first attempt to
    // read configuration.
    if (!m_dsn.empty())
    {
        AutoArrayPtr<wchar_t> keyList;
				ConfigurationMap tmpMap = ReadSettings(m_dsn.c_str(), "odbc.ini");
        ConfigurationMap::iterator current = tmpMap.begin();
        ConfigurationMap::iterator end = tmpMap.end();

        while (current != end)
        {

            // Do not add the "DSN" key into a configuration map.
            // If this is the DSN, be sure that we're not clearing it.
            // Otherwise, keep its original value.
            if ((dsnKey != current->first) || !current->second.empty())
            {
                // Update the configuration map.
                m_configuration[current->first] = current->second;
            }
            ++current;

        }
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////
/// Reads DSN's key/value pairs from ODBCINST.INI and inserts them into the configuration map.  
/// There is no validation on the odbcinst.ini entries at this point.
///
/// New odbcinst.ini entries are added to the configuration map.
/// Existing odbcinst.ini entries are updated in the configuration map.
/// Missing odbcinst.ini entries are kept in the configuration map (using defaults not previously
/// set value).
////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::ReadConfigurationDriver(const char* fileName)
{    
    std::string setupKey(SETTING_SETUP);

    // Retrieves DSN's key value pairs from ODBCINST.INI
    // Note: m_driver should have been specified via UpdateConfiguration before the first attempt
    // to read configuration.
    if (!m_driver.empty())
    {
			ConfigurationMap tmpMap = ReadSettings(m_driver.c_str(), fileName);
        ConfigurationMap::iterator current = tmpMap.begin();
        ConfigurationMap::iterator end = tmpMap.end();

        while (current != end)
        {

            // Do not add the "Setup" key into a configuration map.
            //if (setupKey != current->first)
            //{
                // Update the configuration map.
                if ((0 != current->first.length()) && (0 != current->first.length())) 
                {
                  m_configuration[current->first] = current->second;
                }
            //}
            current++;
        }

    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////
/// Write configuration map to ODBC.INI.
////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::WriteConfiguration(const char* fileName)
{
    // Add data source to the system information.
    // If the data source specification section already exists, 
    // SQLWriteDSNToIni removes the old section before creating the new one.
    if (SQLWriteDSNToIni(
        (LPCSTR)m_dsn.c_str(), 
        (LPCSTR)m_driver.c_str()))
    {
        // Write DSN's key/value pairs to odbc.ini
        ConfigurationMap::iterator mapIter, mapEnd;
        std::string iniKey, iniValue;

        std::string iniDsn(m_dsn.c_str());
        std::string iniDsnOrig(m_dsnOrig.c_str());

        mapIter = m_configuration.begin();
        mapEnd  = m_configuration.end();

        // Writes value names and data to the odbc.ini subkeys of the system information
        while (mapIter != mapEnd)
        {
            iniKey.assign(mapIter->first.begin(), mapIter->first.end());
            iniValue.assign(mapIter->second.begin(), mapIter->second.end());

            if (iniKey == SETTING_DSN)
            {
                // Don't write out the DSN.
                mapIter++;
                continue;
            }

            if (!SQLWritePrivateProfileString(
                iniDsn.c_str(), 
                iniKey.c_str(), 
                iniValue.c_str(), 
                fileName))
            {
                std::vector<std::string> msgParams;
                msgParams.push_back(iniKey);
            }

            // Advance to next key/value pair in the configuration map.
            mapIter++;
        }

        // DSN name change.
        if (!m_dsnOrig.empty() && 
            (0 != m_dsnOrig.length()) && 
            (m_dsnOrig != m_dsn))
        {
            // Remove the odbc.ini entries for the old DSN.
            if (SQLRemoveDSNFromIni(iniDsnOrig.c_str()))
            {
                // Successfully removed original DSN, so update the original to be the new DSN.
                m_dsnOrig = m_dsn;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// Updates all the values of the map to those of the in_attributeMap.
////////////////////////////////////////////////////////////////////////////////////////////////////
void ConfigSettings::UpdateConfiguration(ConfigurationMap& in_dsnConfigurationMap)
{
    ConfigurationMap::iterator mapIter = in_dsnConfigurationMap.begin();
    ConfigurationMap::iterator mapEnd  = in_dsnConfigurationMap.end();
    std::string key;
    std::string value;

    // Update configuration map to include the values of the in_attributeMap
    // (Whether they previously existed or not.)
    while (mapIter != mapEnd)
    {    
        key = mapIter->first;
        value = mapIter->second;

        // Update the configuration map
        m_configuration[key] = value;

        // Preserve the original DSN value; set the current DSN.
        // Used later to determine if DSN was modified in odbc.ini.
				if (SETTING_DSN == key)
        {
            m_dsnOrig = value;
            m_dsn = m_dsnOrig;
        }
        else if (SETTING_DRIVER == key)
        {
            m_driver = value;
        }

        // Advance to next map entry
        mapIter++;
    }
}


////////////////////////////////////////////////////////////////////////////////////////////////////
/// Read settings from odbc.ini or odbcinst.ini
////////////////////////////////////////////////////////////////////////////////////////////////////
ConfigurationMap ConfigSettings::ReadSettings(std::string sectionName, std::string filename)
{
    ConfigurationMap retValue;

		std::string sn(sectionName.begin(), sectionName.end());
		std::string fn(filename.begin(), filename.end());
    std::string defaultValue; //Empty string
    AutoArrayPtr<char> keyBuf(4096); //Use 4096 as initial buffer length

    int keyBufLen = 0;
    #ifdef _WIN32
    while (keyBuf.GetLength() < _I16_MAX) // Choose 32KB as max size to ensure the loop will always terminate.
    #else
    while (keyBuf.GetLength() < SHRT_MAX) // Choose 32KB as max size to ensure the loop will always terminate.
    #endif
    {
        keyBufLen = SQLGetPrivateProfileString(
            (LPCSTR)sn.c_str(),         // The DSN or Driver to check.
            (LPCSTR)NULL,                   // NULL means get key names in this DSN or Driver.
            (LPCSTR)defaultValue.c_str(),   // Default value of empty string is unused for the case where getting key names.
            (LPSTR)keyBuf.Get(),           // Buffer to store key names in.
            (int)keyBuf.GetLength(),     // Length of the buffer.
            (LPCSTR)fn.c_str());           // Section/filename to get the keys/values from (ODBC.INI or ODBCINST.INI).

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

        while(*keyNamePtr)
        {
            *valBuf.Get() = '\0'; //Null terminate the empty buffer in case no characters are copied into it.

            #ifdef _WIN32
            while (valBuf.GetLength() < _I16_MAX)
            #else
            while (valBuf.GetLength() < SHRT_MAX)
            #endif
            {
                valBufLen = SQLGetPrivateProfileString(
                    (LPCSTR)sn.c_str(),     // The DSN to check.
                    (LPCSTR)keyNamePtr,             // Get a value for a particular key
                    (LPCSTR)defaultValue.c_str(),   // Default value of empty string is unused because we know the key must exist.
                    (LPSTR)valBuf.Get(),           // Buffer to store the value in.
                    (int)valBuf.GetLength(),     // Length of the buffer.
                    (LPCSTR)fn.c_str());       // Section/filename to get the keys/values from (ODBC.INI or ODBCINST.INI).

                if (valBufLen >= ((int)valBuf.GetLength() - 1))
                {
                    // SQLGetPrivateProfileString returns bufLen - 1 if it fills the buffer completely. 
                    // In this case, loop and retry with a larger buffer
                    valBuf.Attach(new char[valBuf.GetLength() * 2], valBuf.GetLength() * 2);
                }
                else
                {
                    break;
                }
            }

						std::string tmp(keyNamePtr);
            std::string keyName(tmp.begin(), tmp.end());
						std::string tmp2(valBuf.Get());
            if (tmp2.length() > 0)
            {
              std::string value(tmp2.begin(), tmp2.end());
              // Update the configuration map.
              if ((0 != keyName.length())/* && (0 != value.length())*/) 
              {
                if (!tmp2.empty())
                {
                  std::string value(tmp2.begin(), tmp2.end());
                  retValue[keyName] = value;
                }
                else
                {
                  std::string emptyStr = "";
                  retValue[keyName] = value;
                }
              }
            }

            keyNamePtr += keyName.size() + 1;
        }
    }
    return retValue;
}
