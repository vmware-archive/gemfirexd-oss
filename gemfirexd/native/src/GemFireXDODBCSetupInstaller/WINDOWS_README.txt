
odbc-installer / Pivotal GemFire XD v__VERSION__


CONTENTS
* Introduction
* Binaries
* Usage
* Resources


Introduction
----------------------------------------------------------------------------


This README describes the odbc-installer command-line tool 
that allow application developers to easily install or uninstall
the GemFire XD ODBC driver and to configure DSN without requiring
manual modification of system odbcinst.ini and odbc.ini files.


Binaries
----------------------------------------------------------------------------


The tool works on 32-bit Windows platform 
and supports the Microsoft Driver Manager.

Before running odbc-installer, install 32 bit Pivotal GemFire XD odbc-driver
by double clicking Pivotal_GemFire_XD_ODBC_Driver_32bit_XXXX_XXXX.msi.

For 32 bit installation check entry inside windows registry
Path: HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\ODBC\ODBCINST.INI
It should contain 32 bit Pivotal GemFIre XD ODBC Driver entry.

In addition to this if you want to configure the driver/dsn, you can use 
odbc-installer tool.
The odbc-installer binary is copied inside installation directory
and works with the Microsoft driver manager.
Open windows console and go to installation directory.

Usage
------------------------------------------------------------------------------

------------------------------------------------------------------------------
 odbc-installer
------------------------------------------------------------------------------


 Summary                                                                         
  This tool can be used to install or uninstall the GemFire XD ODBC driver,
  to create, edit or remove a DSN, and to query DSN or the driver.


 Syntax
  odbc-installer <Object> <Action> [Options]
 
 Object
  -d driver
  -s datasource (default user DSN)
  -su user datasource
  -ss system datasource


 Action
  -q query (query data source or driver)
  -a add (add data source or install driver)
  -e edit (edit data source)
  -r remove (remove data source or uninstall driver)


 Options
  -n<name> (name of data source or driver)
  -t<attribute string> String of semi-colon delimited
  key=value pairs follows this.


 Examples
  Query all installed driver/s
  1. odbc-installer -d -q -n


  Query driver
  1. odbc-installer -d -q -n"GemFire XD ODBC __VERSION__ Driver"  


  Query data source
  1. odbc-installer -s -q -n"GemFire XD ODBC DSN"  


  Install driver
  1. odbc-installer -d -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"GemFire XD ODBC __VERSION__ Driver;Driver=gemfirexdodbc.dll;
    Setup=gemfirexdodbcSetup.dll;APILevel=2;UID=;PWD="

	
  Uninstall driver
  1. odbc-installer -d -r -n"GemFire XD ODBC __VERSION__ Driver"  


  Add data source
  1. odbc-installer -s -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN;Description=GFXDODBCDSN"
    SERVER=localhost"


  Edit data source
  1. odbc-installer -s -e -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN; Description=GFXDODBCDSN;
    SERVER=127.0.0.1"


  Remove Data Source
  1. odbc-installer -s -r -n"GemFire XD ODBC DSN"  
---------------------------------------------------------------------------


RESOURCES
---------------------------------------------------------------------------

For more information about using GemFire XD ODBC drivers see:
http://gemfirexd.docs.pivotal.io/latest/userguide/index.html#developers_guide/topics/odbc/using_odbc_functions.html

