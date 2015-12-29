-- This example illustrates the working of a custom result processor in Data Aware Procedures.

-- Setup for the test

   o Multiple servers can be started through the gfxd server script
   o Start a peer client using the Embedded driver
   o Create table EMP.PARTITIONTESTTABLE
     table ddl = "create table EMP.PARTITIONTESTTABLE1 (ID int NOT NULL,"
                      + " SECONDID int not null, THIRDID varchar(10) not null,"
                      + " PRIMARY KEY (SECONDID, THIRDID)) PARTITION BY COLUMN (ID)"
   o Do some inserts
     insert dmls = "INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '3') "
                   "INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (3, 3, '3') "
                   "INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (4, 4, '3') "
                   "INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (5, 5, '3') "
                   "INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '2') "
                   "INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '4') "

  o Register a stored procedure called MergeSort().

-- This procedure is then called using the call statement in which the GemFireXD custom extension of the call syntax
-- that executes the procedure only on nodes which are hosting the data for the EMP.PARTITIONTESTTABLE.
-- The procedure body makes uses of the '<local>' escape syntax to fire a sql query
-- "<local> SELECT * FROM " + tableName + " ORDER BY SECONDID";
-- Because of the '<local>' escape the query works on only the data which are local to that data node.
-- The results from each node are then processed by the custom merge processor, MergeSortProcessor, to merge the results 
-- from individual nodes and return the results sorted across all the involved nodes.

-- The MergeSortProcedure.java contains the actual java method which will be called on each node, hosting the data, when the procedure is invoked

-- First register the procedure:

   Let's say you want to name this processor as MergeSort()
   Statement st = conn.createStatement();
   st.execute("CREATE PROCEDURE MergeSort () "
               + "LANGUAGE JAVA PARAMETER STYLE JAVA "
               + "READS SQL DATA DYNAMIC RESULT SETS 1 "
               + "EXTERNAL NAME '"
               +  MergeSortProcedure.class.getName() + ".mergeSort' ");
  (For details on the CREATE PROCEDURE syntax please see the reference documentation

-- So this procedure's execution will fetch one result set.

-- MergeSortProcessor.java is the implementation of a custom result processor.

-- Next create an alias for the custom processor

   st.execute("CREATE ALIAS MergeSortProcessor FOR '"
               + MergeSortProcessor.class.getName()+"'"");

-- Then call the procedure

   String sql = "CALL MergeSort() " 
                + "WITH RESULT PROCESSOR MergeSortProcessor "
                + "ON TABLE EMP.PARTITIONTESTTABLE1 WHERE 1=1"

   CallableStatement cs = conn.prepareCall(sql);
   cs.execute();

   ResultSet rs = cs.getResultSet();

-- verify the sorted results merged from all data stores

   for(int i = 1; i <= 6; i++) {
     assert rs.next() == true;;
     switch (i) {
       case 1:
       case 2:
       case 3:
         assert rs.getInt(2) == 2;
         break;
 
       case 4:
         assert rs.getInt(2) == 3;
         break;
                                                                                     
       case 5:
         assert rs.getInt(2) == 4;
         break;
 
       case 6:
         assert rs.getInt(2) == 5;
         break;
     }
   }
   assert rs.next() == false;
