
Pivotal GemFire XD Examples 

JMXMonitor Example:
This example illustrates how to use JMX APIs to retrieve and monitor GemFireXD statistics, and to raise notifications when certain conditions are met.

Mapreduce Example:
This is an example of GemfireXD's integration with Hadoop Mapreduce.

Storedprocedure Example:
This example illustrates the working of a custom result processor in Data Aware Procedures.

JoinQueryToDAP Example:
This example illustrates that Data Aware Procedure (DAP) performed 100+ times better in terms of execution time over corresponding join query.

See example specific README.txt for more details. 

============================================================

JoinQueryToDAP Example
  - This example illustrates that Data Aware Procedure (DAP) performed 100+ times better in terms of execution time over corresponding join query.
  - Source location: gemfirexd/GemFireXDPackaging/examples/joinquerytodap
  - compiled jar 'JoinQueryToDap.jar' is located at gemfirexd/GemFireXDTests/lib
  - Steps to compile source, create jar 'JoinQueryToDap.jar'
		javac -cp <gfxd-build>/product-gfxd/lib/gemfirexd.jar -d temp <gfxd-checkout>/gemfirexd/GemFireXDPackaging/examples/joinquerytodap/*.java 
		jar -cfv JoinQueryToDap.jar -C temp/ . <gfxd-checkout>/gemfirexd/GemFireXDPackaging/examples/joinquerytodap/README.txt
		mv JoinQueryToDap.jar <gfxd-checkout>/gemfirexd/GemFireXDTests/lib
