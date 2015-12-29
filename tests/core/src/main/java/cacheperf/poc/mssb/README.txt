The first test in this suite, mssb01, was built to reproduce a problem at MSSB in June 2010 where all client would hang when a server was restarted after being brought down.  MSSB used tcp-port to fix the ports used by the bridge servers.  There were 8 servers on 4 hosts, and each pair used the same two tcp-port values.  They also used membership-port-range, but this was not involved in the hang.  The test reproduced the problem and Bruce provided a fix.

The next two tests are based on other tests in the MSSB test plan (see mssb_test_plan.txt).  The final test is based on another MSSB issue that at this date looks to be a non-gemfire issue.

The tests that bounce clients do not generate performance reports due to issues with trim intervals.
