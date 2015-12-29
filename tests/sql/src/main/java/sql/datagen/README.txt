USAGE:
	DataGenerator <table-names> [<host:port> <row-counts> <mapper-file>]
    Where,
    	table-names  => comma separated table names
    	row-counts   => comma separated row-counts
    	mapper-file  => column mapper files



Mapper file:
	The mapper file (say datagentestMapper.inc) contains information about how CSVs are generated for the table. It has properties file structure where key is fully qualified column name and value is the pattern. DataGenerator generated data for the column based on it’s associated pattern. If no pattern is defined for a column then random data is generated for that column. 

Format: 
	<schema-name>.<table-name>.<column-name> = <pattern>

Examples:
	trade.customers.cid = ::format 9999
	trade.customers.cust_name = ::random [15] len

Available patterns:
	::format 999999   		- number format of fixed length
	::format 999.99    		- decimal format of fixed length 
	::random [5] lenth  	- fixed length for both number and characters
	::ramdom [2-3] lenth	- variable length values values for number or characters
	::UDT MyClass.myUDTFunctionRandomValue() 	- UDT type
	::foreignkey APP.ACC.ID repeat 3    	- FK relationship with 3 cardinality   
	::foreignkey APP.ACC.ID repeat 5-10    	- for FK relationship with 5 to 10 cardinality 
	::valuelist [select col1 from table1]  	- get values from other tables sequentially
	::valuelist [select col1 from table1] randomize 	- get values from other tables
	::valuelist [javaexecute Classname.methodName()] 	- get values from the java function sequential
	::valuelist [javaexecute Classname.methodName()] randomize	- values from java function
	::valuelist {val1, val2, val3} 		- select value from given list sequentially
	::valuelist {val1, val2, val3}  randomize	- select value randomly from given list
	[skip]              	- skip generating data for this column in csv
	[null]				- generate nothing (‘blank’) for this column in csv


	
