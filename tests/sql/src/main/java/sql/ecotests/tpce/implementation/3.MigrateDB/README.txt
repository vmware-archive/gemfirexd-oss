0. Modify corresponding scripts and xml files to set up right data file location and database connection
Migrate DDL/Data from Oracle to GemFireXD:
1. DDLUtils: writeDDLToXML/writeDDLToSQL/createDBFromXML/ImportDataToDB (see ddlutils/build.xml as the example)
2. GFXD commands: write-schema-to-sql/write-schema-to-xml/write-data-to-xml/write-schema-to-db/write-data-to-db (cmd_oratogfxd.sh)
3. Export Oracle data using native method (export_ora_data.sh) and import to GFXD using its import_table (with some customized dataconverter for incompatible data type)
