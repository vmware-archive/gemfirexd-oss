MapReduce Example Overview
============================

   Gemfire XD integration with Hadoop Mapreduce

Building MapReduce Example
============================

   NOTE: Compiled JAR is already included with distribution at
   /usr/lib/gphd/gfxd/examples/mapreduce/target/mapreduce-1.0-RELEASE.jar.
   The source and Maven build POM are provided in the event the source
   needs to be recompiled.
   
   Build Dependencies: Maven 3 + Access To Maven Repositories
   
   1. Change to the directory that contains the mapreduce example

        cd /usr/lib/gphd/gfxd/examples/mapreduce

   2. Copy gemfirexd.jar to maven_dist/gemfirexd-__VERSION__.jar

        cp /usr/lib/gphd/gfxd/lib/gemfirexd.jar \
        /usr/lib/gphd/gfxd/examples/mapreduce/maven_dist/gemfirexd-__VERSION__.jar
   
   3. Install the gemfirexd.jar into local Maven repository
   
        mvn install:install-file \
        -Dfile=maven_dist/gemfirexd-__VERSION__.jar \
        -DpomFile=maven_dist/gemfirexd-__VERSION__.pom
   
   4. Compile the example code using maven :
   
        mvn clean package
          
   5. Verify JAR file, mapreduce-1.0-RELEASE.jar, created in target directory

Running MapReduce Example
============================

    NOTE: The instructions assume GemfireXD has been installed using the RPM
    and Pivotal HD installed locally. 

   1. Verify Pivotal HD service has been started
   2. Move into `$GFXD_HOME/examples/mapreduce/scripts` directory of GFXD
      installation.
   3. Open `mr_driver.sh` and if you do not already have GFXD binaries in your
      PATH, update the GFXD and GFXD_HOME environment variable if needed. Also,
      set the BIND_ADDRESS to the hostname of your machine

        GFXD=/usr/bin/gfxd
        BIND_ADDRESS=HOSTNAME
        GFXD_HOME=/usr/lib/gphd/gfxd
        
   4. Open 'create_hdfs_schema.sql' 

        a. Replace 'LOCATOR' on line 18 with the name of the locator
        b. Replace 'HOSTNAME' with the namenode of the HDFS store at line 35

   5. Let's then **clear** hdfs previous run of this sample. If you have not
      run it before, skip this step.

        $./mr_driver.sh clear
      
   6. Create locator1 and server1 directories
   
        $mkdir locator1 server1
        
   7. Now start GFXD locator and server:

        $./mr_driver.sh start
        NOTE: Verify the ports identified in mr_driver.sh are available as it
        may cause issues with starting GemfireXD processes

   8. Then we're going to **create the database schema**, the **HDFS persistent
      store** and **load data** into **FLIGHTS_HISTORY** table. Note that
      everytime we execute this step, the script will clear all previous data
      under /tmp/gfxd of HDFS and recreate GFXD tables.

        $./mr_driver.sh load

   9. Now let's start the MapReduce job. This step can take sometime depending
      on your machine configuration.

        $./mr_driver.sh runMR

   10. When the last step completes, these should be the last lines of the
       output, indicating a sucessful execution. 

        ...
        [fine 2013/09/05 11:16:10.592 EDT <Distributed system shutdown hook> tid=0x15] <Hoplog:RegionDirector>      Closing hoplog region manager for /APP/FLIGHTAVAILABILITY

        real    5m15.344s
        user    1m51.597s
        sys 0m32.675s
        Found 2 items
        -rw-r--r--   3 gpadmin hadoop          0 2013-09-05 11:16 /output_int/_SUCCESS
        -rw-r--r--   3 gpadmin hadoop        691 2013-09-05 11:16 /output_int/part-r-00000
        gfxd> select * from busy_airport;
        AIR&|FLIGHTS    |STAMP                     
        -------------------------------------------
        JFK |860        |2013-09-05 13:00:14.28    

        1 rows selected

        In these lines we're checking the HDFS for job successful execution and querying the GFXD table for the job result with the busiest airport.

   11. Execute 'clear' command to stop the GFXD server, locator, and remove
       directories.

        $./mr_driver.sh clear
