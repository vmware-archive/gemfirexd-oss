Normally the tests can be run from within IDE (Visual Studio or monodevelop) with "gfxd-start-devenv" target or explicitly with "gfxd-run-csharp-tests" target.

Steps for running these tests manually using external tool like NUnit GUI:
 * Compile the tests as a DLL project into a DLL
 * Compile java procedure: javac tests/TestProcedures.java
 * Set the environment variables: GEMFIREXD ==> point to GemFireXD product dir, GFXDADOOUTDIR ==> point to an existing location where output can go
 * Copy java class compiled above to GFXDADOOUTDIR: copy tests/ as GFXDADOOUTDIR/tests/
 * Load the DLL in NUnit GUI and run the tests
