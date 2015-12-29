
odbc-installer, iodbc-installer, and progress-installer / Pivotal GemFire XD v__VERSION__


CONTENTS
* Introduction
* Binaries
* Environment Variables
* Usage
* Resources


Introduction
----------------------------------------------------------------------------


This README describes the odbc-installer, iodbc-installer, and 
progress-installer command-line tools that allow application developers to 
easily install or uninstall the GemFire XD ODBC driver and to configure DSN
without requiring manual modification of system odbcinst.ini and odbc.ini
files.

Binaries
----------------------------------------------------------------------------


The tool works on both 32-bit and 64-bit Linux
platforms and supports the unixODBC, iODBC and 
progress DataDirect driver managers.


The odbc-installer binary inside odbc/32 and odbc/64
will work with the unixODBC driver manager.


The iodbc-installer binary inside odbc/32 and odbc/64 
will work with the iODBC driver manager.

The progress-installer binary inside odbc/32 and odbc/64 
will work with the progress DataDirect driver manager.


Environment Variables
-----------------------------------------------------------------------------


Before running odbc-installer or iodbc-installer or progress-installer, 
set LD_LIBRARY_PATH to the appropriate shared library.

For the unixODBC driver manager, odbc-installer should pick up
libodbcinst.so.2


Example:
  export LD_LIBRARY_PATH=/unixODBC/lib:$LD_LIBRARY_PATH

For the iODBC driver manager, iodbc-installer should pick up
libiodbcinst.so.2


Example:
  export LD_LIBRARY_PATH=/iODBC/lib:$LD_LIBRARY_PATH

For the progress driver manager, progress-installer should pick up
libodbcinst.so


Example:
  export LD_LIBRARY_PATH=/progress/lib:$LD_LIBRARY_PATH


You can see the list of the shared libraries used by odbc-installer
or iodbc-installer or progress-installer by using ldd(1).


Example:
  ldd odbc-installer
  ldd iodbc-installer
  ldd progress-installer

Before running the odbc-installer tool with the unixODBC driver manager,
set ODBCINI, and ODBCSYSINI to accessible paths.


Example:
  export ODBCINI=/home/user/odbc.ini
  export ODBCSYSINI=/home/user

Before running the iodbc-installer tool with the iODBC driver manager,
set ODBCINI, ODBCINSTINI, and ODBCSYSINI to accessible paths.


Example:
  export ODBCINI=/home/user/odbc.ini
  export ODBCINSTINI=/home/user/odbcinst.ini
  export ODBCSYSINI=/home/user/odbcinst.ini

Before running the progress-installer tool with the progress driver manager,
set ODBCINI, and ODBCINST to accessible paths.
please refer below link for setting Environment Variables.
http://media.datadirect.com/download/docs/odbc/allodbc/wwhelp/wwhimpl/js/html/wwhelp.htm#href=userguide/rfi1363233738400.html


Example:
  export ODBCINI=/home/user/odbc.ini
  export ODBCINST=/home/user/odbcinst.ini

Usage
------------------------------------------------------------------------------

------------------------------------------------------------------------------
 odbc-installer (or iodbc-installer or progress-installer)                                      
------------------------------------------------------------------------------


 Summary                                                                         
  This tool can be used to install or uninstall the GemFire XD ODBC driver,
  to create, edit or remove a DSN, and to query DSN or the driver.


 Syntax
  odbc-installer <Object> <Action> [Options]
  iodbc-installer <Object> <Action> [Options]
  progress-installer <Object> <Action> [Options]
 
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
  2. iodbc-installer -d -q -n
  3. progress-installer -d -q -n


  Query driver
  1. odbc-installer -d -q -n"GemFire XD ODBC __VERSION__ Driver"
  2. iodbc-installer -d -q -n"GemFire XD ODBC __VERSION__ Driver"
  3. progress-installer -d -q -n"GemFire XD ODBC __VERSION__ Driver"


  Query data source
  1. odbc-installer -s -q -n"GemFire XD ODBC DSN"
  2. iodbc-installer -s -q -n"GemFire XD ODBC DSN"
  3. progress-installer -s -q -n"GemFire XD ODBC DSN"


  Install driver
  1. odbc-installer -d -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"GemFire XD ODBC __VERSION__ Driver;Driver=libgemfirexdodbc.so;
    Setup=libgemfirexdodbc.so;APILevel=2;UID=;PWD="
  2. iodbc-installer -d -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"GemFire XD ODBC __VERSION__ Driver;Driver=libgemfirexdiodbc.so;
    Setup=libgemfirexdiodbc.so,APILevel=2;UID=;PWD="
  3. progress-installer -d -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"GemFire XD ODBC __VERSION__ Driver;Driver=libgemfirexdodbc.so;
    Setup=libgemfirexdodbc.so,APILevel=2;UID=;PWD="


  Uninstall driver
  1. odbc-installer -d -r -n"GemFire XD ODBC __VERSION__ Driver"
  2. iodbc-installer -d -r -n"GemFire XD ODBC __VERSION__ Driver"
  3. progress-installer -d -r -n"GemFire XD ODBC __VERSION__ Driver"


  Add data source
  1. odbc-installer -s -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN;Description=GFXDODBCDSN"
    SERVER=localhost"
  2. iodbc-installer -s -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN;Description=GFXDODBCDSN"
	SERVER=localhost"
  3. progress-installer -s -a -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN;Description=GFXDODBCDSN"
	SERVER=localhost;DriverUnicodeType=1"

  Edit data source
  1. odbc-installer -s -e -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN; Description=GFXDODBCDSN;
    SERVER=127.0.0.1"
  2. iodbc-installer -s -e -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN; Description=GFXDODBCDSN;
    SERVER=127.0.0.1"
  3. progress-installer -s -e -n"GemFire XD ODBC __VERSION__ Driver"
      -t"DSN=GemFire XD ODBC DSN; Description=GFXDODBCDSN;
    SERVER=127.0.0.1;DriverUnicodeType=1"


  Remove Data Source
  1. odbc-installer -s -r -n"GemFire XD ODBC DSN"
  2. iodbc-installer -s -r -n"GemFire XD ODBC DSN"
  3. progress-installer -s -r -n"GemFire XD ODBC DSN"
---------------------------------------------------------------------------


RESOURCES
---------------------------------------------------------------------------

For more information about using GemFire XD ODBC drivers see:
http://gemfirexd.docs.pivotal.io/latest/userguide/index.html#developers_guide/topics/odbc/using_odbc_functions.html

