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
#include "MainDialog.h"
#include "ConfigSettings.h"
#include "AutoArrayPtr.h"
#include "resource.h"

#include <ShlObj.h>
#include <odbcinst.h>



// Public ==========================================================================================
////////////////////////////////////////////////////////////////////////////////////////////////////
MainDialog::MainDialog()
{
    ; // Do nothing.
}

////////////////////////////////////////////////////////////////////////////////////////////////////
INT_PTR MainDialog::ActionProc(
    HWND hwndDlg,
    UINT message,
    WPARAM wParam,
    LPARAM lParam)
{
    // This static variable might be disturbing to you.
    // However, the lParam (used to pass the dialog context) is only available during the 
    // WM_INITDIALOG message.
    // All other messages (WM_COMMAND, WM_DESTROY, etc.) do not have access to this.
    // Therefore, we will preserve a static pointer accessible only within this procedure, to 
    // permit access to the context during subsequent messaging from the dialog box.
    // See Programming Windows 5th Edition pg 509
    static ConfigSettings* configSettings = NULL;

    INT_PTR returnValue = static_cast<INT_PTR> (true);    

    // Switch through the different messages that can be sent to the dialog by Windows and take the
    // appropriate action.
    switch (message) 
    { 
        case WM_INITDIALOG:
        {
            configSettings = reinterpret_cast<ConfigSettings*>(lParam);
            MainDialog::Initialize(hwndDlg, configSettings);
            break;
        }

        case WM_COMMAND:
        {
            // The user has done some action on the dialog box.
            WORD controlIdentifier = LOWORD(wParam);
            WORD controlNotification = HIWORD(wParam);
            
            switch (controlIdentifier)
            {
                case IDOK:
                {
                    // OK button pressed.
                    DoOkAction(hwndDlg, configSettings);
                    break;
                }

                case IDCANCEL:
                {
                    // Cancel button pressed.
                    DoCancelAction(hwndDlg);
                    break;
                }

                case IDC_DSNEDIT:
				case IDC_UIDEDIT:
				case IDC_PWDEDIT:
                {
                    // Only changes to required fields will be checked for disabling/enabling of 
                    // OK button.
                    if (EN_CHANGE == controlNotification)
                    {
                        // Enable/Disable the OK button if required fields are filled/empty.
                        CheckEnableOK(hwndDlg);
                    }

                    break;
                }

                default:
                {
                    // Unknown command.
                    returnValue = static_cast<INT_PTR> (false);
                    break;
                }
            }
            break;
        }

        case WM_DESTROY:
        case WM_NCDESTROY:
        {
            // WM_DESTROY - Destroy the dialog box. No action needs to be taken.
            // WM_NCDESTROY - Sent after the dialog box has been destroyed.  No action needs to be taken.
            break;
        }

        default:
        {
            // Unrecognized message.
            returnValue = static_cast<INT_PTR> (false);
            break;
        }
    } 

    return returnValue;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void MainDialog::Dispose(HWND in_dialogHandle)
{
    // Avoid compiler warnings.
    //UNUSED(in_dialogHandle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
bool MainDialog::Show(
    HWND in_parentWindow, 
    gemfirexd_handle in_moduleHandle, 
    ConfigSettings& in_configSettings,
    bool edit_DSN)
{
    // Use the windows functions to show a dialog box which returns true if the user OK'ed it, 
    // false otherwise.

    // Show the dialog box and get if the user pressed OK or cancel.
    INT_PTR userOK;
    
    if (edit_DSN)
    {
        userOK = DialogBoxParam(
            reinterpret_cast<HINSTANCE>(in_moduleHandle),
            MAKEINTRESOURCE(IDD_ConnectDLG_EDIT),
            in_parentWindow,
            reinterpret_cast<DLGPROC>(ActionProc),
            reinterpret_cast<LPARAM>(&in_configSettings));
    }
    else
    {
        userOK = DialogBoxParam(
            reinterpret_cast<HINSTANCE>(in_moduleHandle),
            MAKEINTRESOURCE(IDD_ConnectDLG),
            in_parentWindow,
            reinterpret_cast<DLGPROC>(ActionProc),
            reinterpret_cast<LPARAM>(&in_configSettings));
    }

    // Return true/false depending on what the user pressed in the dialog box.
    if (1 == userOK)
    {
        return 1;
    }
    else 
    {
        return 0;
    }
}

// Private =========================================================================================
////////////////////////////////////////////////////////////////////////////////////////////////////
void MainDialog::Initialize(HWND in_dialogHandle, ConfigSettings* in_configSettings)
{
    CenterDialog(in_dialogHandle);

    SetWindowText(in_dialogHandle, "GemFireXDODBC Configuration Dialog");

    if (NULL != in_configSettings)
    {
        // Set the DSN textbox.
        SetDlgItemText(
            in_dialogHandle, 
            IDC_DSNEDIT, 
            (LPCSTR)in_configSettings->GetDSN().c_str());

        // Set the DSN textbox.
        SetDlgItemText(
            in_dialogHandle, 
            IDC_SERVEREDIT, 
            (LPCSTR)in_configSettings->GetSERVER().c_str());

        // Set the DSN textbox.
        SetDlgItemText(
            in_dialogHandle, 
            IDC_PORTEDIT, 
            (LPCSTR)in_configSettings->GetPORT().c_str());

        // Set the PWD textbox.
        SetDlgItemText(
            in_dialogHandle, 
            IDC_PWDEDIT, 
            (LPCSTR)in_configSettings->GetPWD().c_str());

				// Set the UID textbox.
        SetDlgItemText(
            in_dialogHandle, 
            IDC_UIDEDIT, 
            (LPCSTR)in_configSettings->GetUID().c_str());
    }

    // Check to make sure that the required fields are filled.
    CheckEnableOK(in_dialogHandle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void MainDialog::DoCancelAction(HWND in_dialogHandle)
{
    EndDialog(in_dialogHandle, false);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void MainDialog::DoOkAction(HWND in_dialogHandle, ConfigSettings* in_configSettings)
{
    // End dialog at the end of the OK action, unless there is a recoverable exception.
    bool endDialog = true;

    if (NULL != in_configSettings)
    {
        try
        {
            // Verify and set the DSN
						std::string text = MainDialog::GetEditText(IDC_DSNEDIT, in_dialogHandle);

            if (SQLValidDSN((LPCSTR)text.c_str()))
            {
                in_configSettings->SetDSN(text);            
            }
            else
            {
                throw;
            }

						// Set the SERVER
						in_configSettings->SetSERVER(GetEditText(IDC_SERVEREDIT, in_dialogHandle));
            
						// Set the PORT
						in_configSettings->SetPORT(GetEditText(IDC_PORTEDIT, in_dialogHandle));
            
						// Set the PWD.
						in_configSettings->SetPWD(GetEditText(IDC_PWDEDIT, in_dialogHandle));

            // Set the UID.
            in_configSettings->SetUID(GetEditText(IDC_UIDEDIT, in_dialogHandle));
            
            // Save the settings to the registry.
            in_configSettings->WriteConfiguration("ODBC.INI");
        }
        catch( char *str  )
        {
            // Display the error message.
            MessageBox(
                NULL,
                str,
                in_configSettings->GetDSN().c_str(),
                MB_ICONEXCLAMATION | MB_OK);

            endDialog = false;
        }
    }

    if (endDialog)
    {
        EndDialog(in_dialogHandle, true);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// Check the lengths of the required fields.
////////////////////////////////////////////////////////////////////////////////////////////////////
void MainDialog::CheckEnableOK(HWND in_dialogHandle)
{
	std::string controlText = GetEditText(IDC_DSNEDIT, in_dialogHandle);    
    bool enableOK = (0 != controlText.length());

    // If any required fields are empty, then disable the OK button.
    EnableWindow(GetDlgItem(in_dialogHandle, IDOK), enableOK);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
void MainDialog::CenterDialog(HWND in_dialogHandle)
{
    // Get the parent window, or the desktop if there is no parent.
    HWND parentWindowHandle = GetParent(in_dialogHandle);

    if (NULL == parentWindowHandle)
    {
        parentWindowHandle = GetDesktopWindow();
    }

    RECT parentRect;
    RECT dialogRect;

    // Get the rectangles for the parent and the dialog.
    GetWindowRect(parentWindowHandle, &parentRect);
    GetWindowRect(in_dialogHandle, &dialogRect);

    // Get the height and width of the dialog.
    int width = dialogRect.right - dialogRect.left;
    int height = dialogRect.bottom - dialogRect.top;

    // Determine the top left point for the new centered position of the dialog.
    // The computations are a bit odd to avoid negative numbers, which would result in overflow.
    int leftPoint = 
        ((parentRect.left * 2) + (parentRect.right - parentRect.left) - width) / 2;

    int topPoint = 
        ((parentRect.top * 2) + (parentRect.bottom - parentRect.top) - height) / 2;

    // Ensure that the dialog stays on the screen.
    RECT desktopRect;

    GetWindowRect(GetDesktopWindow(), &desktopRect);

    // Horizontal adjustment.
    if (desktopRect.left > leftPoint)
    {
        leftPoint = desktopRect.left;
    }
    else if (desktopRect.right < (leftPoint + width))
    {
        leftPoint = desktopRect.right - width;
    }

    // Vertical adjustment.
    if (desktopRect.top > topPoint)
    {
        topPoint = desktopRect.top;
    }
    else if (desktopRect.bottom < (topPoint + height))
    {
        topPoint = desktopRect.top - height;
    }

    // Place the dialog.
    MoveWindow(in_dialogHandle, leftPoint, topPoint, width, height, true);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
std::string MainDialog::GetEditText(int in_component, HWND in_dialogHandle)
{
    // Get the length of the selection.
    LRESULT textLength = SendDlgItemMessage(
        in_dialogHandle, 
        in_component, 
        WM_GETTEXTLENGTH, 
        0, 
        0);
    if (CB_ERR == textLength)
    {
        // An error happened, so just return an empty string.
			return "";
    }

    // Allocate a buffer for the selection.
    AutoArrayPtr<char> buffer(textLength + 1);

    int textLength2 = (int)(textLength);

    // Retrieve the selected text.
    GetDlgItemText(
        in_dialogHandle, 
        in_component, 
        buffer.Get(),
        textLength2 + 1);

    // Trim off spaces.
		std::string text = MainDialog::Trim(buffer.Get(), " \t");

    return text.c_str();
}

////////////////////////////////////////////////////////////////////////////////////////////////////
std::string MainDialog::Trim(const std::string& in_string, const std::string& in_what) 
{
    if (0 == in_string.length())
    {
        return in_string;
    }

    gemfirexd_size_t beginning = in_string.find_first_not_of(in_what);
    gemfirexd_size_t ending = in_string.find_last_not_of(in_what);

    if (-1 == beginning)
    {
			return "";
    }

    return std::string(in_string, beginning, ending - beginning + 1);
}
