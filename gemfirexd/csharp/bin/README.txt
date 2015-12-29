The runCSTests.sh script is used to run nunit-console runnner to execute the C# NUnit tests for the ADO.NET driver. It expects OSBUILDDIR environment variable to be set to determine the product directory and the output directory which is set to ${OSBUILDDIR}/tests/results/csharp/{Debug,Release}. GFXD server/locator output goes into "gfxdserver" sub-directory of the output directory while unit test logging itself goes into "output" sub-directory of the same.

Arguments accepted by the script:
 -logLevel=xxx  ==> sets the log-level for the GFXD server/locator logging as well as the unit test logging
 -serverLogLevel=xxx ==> sets the log-level only for the GFXD server/locator logging
 -serverSecLogLevel=xxx ==> sets the security-log-level for the GFXD server/locator logging
 -debug=<true|false> ==> use the debug build with output going into Debug/output directory (rather than default of Release/output)
 -clean ==> kills any running nunit-console process and deletes the output directories

If nunit-console executable in /gcm/where/csharp resides on a remote share/network drive, then nunit assemblies will need to be added as trusted assemblies. Run the setNUnitTrust.bat file on the machine before running the nunit tests.
