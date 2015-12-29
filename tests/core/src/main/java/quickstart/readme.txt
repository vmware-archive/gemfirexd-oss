The Quickstart tests are all under tests/quickstart. All the Quickstart tests
extend QuickstartTestCase. 
 
QuickstartTestCase does a couple things. For one, it sets the log-level to
warning which greatly reduces the logging from the Quickstart example. The
golden files contain the example output which is produced by System.out.println
statements in the example, NOT by logging. Thus warning-level logging means the
logs will be empty unless something goes wrong. So, the golden file will
contain only that output which is produced by the System.out.println
statements.
 
You'll also notice the method printOutput(ProcessWrapper process) along with
some related methods in QuickstartTestCase. Each test uses these printOutput
methods to print out the ProcessWrapper output.
 
So if you look at a test such as HierarchicalTest, you'll see two important
lines...
 
    printConsumerOutput(this.client);
and
    printProducerOutput(this.server);
 
These lines are printing out the output. Such that if you execute
HierarchicalTest...
 
build.sh -Dquickstart.testcase=quickstart/HierarchicalTest.class
run-quickstart-tests
 
...you'll find the results of the above two printXxxOutput methods in the file
quickstart.HierarchicalTest.txt in your tests/results/quickstart directory.
 
Within that file is...
 
------------------ BEGIN PRODUCER ------------------
 
This example demonstrates hierarchical caching. This program is a server,
listening on a port for client requests. The client program connects and
requests data. The client in this example is also configured to forward
information on data destroys and updates.
 
Connecting to the distributed system and creating the cache.
Example region, /exampleRegion, created in cache.
 
Please start the HierarchicalClient now.
 
    Loader called to retrieve value for key0
    Received afterCreate event for entry: key0, LoadedValue0
    Loader called to retrieve value for key1
    Received afterCreate event for entry: key1, LoadedValue1
    Loader called to retrieve value for key2
    Received afterCreate event for entry: key2, LoadedValue2
    Received afterUpdate event for entry: key0, ClientValue0
    Received afterDestroy event for entry: key2
Closing the cache and disconnecting.
 
------------------- END PRODUCER -------------------
 
So what you do is you copy everything between the header and footer (the two
lines that have BEGIN and END in them) and THAT is your golden file for the
producer in the Hierarchical example.
 
1) write the test
2) leave out the compareOutputTo statement in the test for now
3) add "fail();" as the last line of your test method (this causes the test to
fail which causes quickstart.HierarchicalTest.txt to not be deleted)
4) run the example
5) open the output file, copy the lines between the correct header and footer
6) paste that output to create your golden files
7) add in your compareOutputTo statements to your test and remove "fail();"
8) rerun and repeat to tweak the test so that it's working correct
9) make a small change to one golden file, rerun to make sure the test fails,
correct the change to pass again
10) repeat with the other golden file (one golden file for each process in the
example)
