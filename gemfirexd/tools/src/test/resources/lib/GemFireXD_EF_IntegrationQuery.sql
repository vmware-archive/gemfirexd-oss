SELECT 
"Project6"."C11" AS "C1", 
"Project6"."CatalogName" AS "CatalogName", 
"Project6"."SchemaName" AS "SchemaName", 
"Project6"."Name" AS "Name", 
"Project6"."C1" AS "C2", 
"Project6"."C3" AS "C3", 
"Project6"."C4" AS "C4", 
"Project6"."C5" AS "C5", 
"Project6"."C6" AS "C6", 
"Project6"."C7" AS "C7", 
"Project6"."C8" AS "C8", 
"Project6"."C9" AS "C9", 
"Project6"."C10" AS "C10", 
"Project6"."C12" AS "C11"
FROM ( SELECT 
"Extent1"."CatalogName" AS "CatalogName", 
"Extent1"."SchemaName" AS "SchemaName", 
"Extent1"."Name" AS "Name", 
"UnionAll1"."Name" AS "C1", 
"UnionAll1"."Ordinal" AS "C2", 
"UnionAll1"."IsNullable" AS "C3", 
"UnionAll1"."TypeName" AS "C4", 
"UnionAll1"."MaxLength" AS "C5", 
"UnionAll1"."Precision" AS "C6", 
"UnionAll1"."DateTimePrecision" AS "C7", 
"UnionAll1"."Scale" AS "C8", 
"UnionAll1"."IsIdentity" AS "C9", 
"UnionAll1"."IsStoreGenerated" AS "C10", 
1 AS "C11", 
CASE WHEN ("Project5"."C2" IS NULL) THEN 0 ELSE "Project5"."C2" END AS "C12"
FROM   (
SELECT
TABLE_NAME as "Id"
,   TABLE_CATALOG as "CatalogName"
,   TABLE_SCHEMA as "SchemaName"
,   TABLE_NAME  as "Name"
FROM
TEMP.SCHEMATABLES
WHERE
TABLE_TYPE LIKE 'TABLE' AND TABLE_SCHEMA != 'TEMP'

) AS "Extent1"
INNER JOIN  (SELECT 
"Extent2"."Id" AS "Id", 
"Extent2"."Name" AS "Name", 
"Extent2"."Ordinal" AS "Ordinal", 
"Extent2"."IsNullable" AS "IsNullable", 
"Extent2"."TypeName" AS "TypeName", 
"Extent2"."MaxLength" AS "MaxLength", 
"Extent2"."Precision" AS "Precision", 
"Extent2"."DateTimePrecision" AS "DateTimePrecision", 
"Extent2"."Scale" AS "Scale", 
"Extent2"."IsIdentity" AS "IsIdentity", 
"Extent2"."IsStoreGenerated" AS "IsStoreGenerated", 
0 AS "C1", 
"Extent2"."ParentId" AS "ParentId"
FROM (
SELECT
c.TABLE_NAME || '.' || c.COLUMN_NAME as "Id"
,   c.TABLE_NAME as "ParentId"
,   c.COLUMN_NAME   as "Name"
,   c.ORDINAL_POSITION as "Ordinal"
,   1  as "IsNullable"
,   c.EDM_TYPE as "TypeName"
,   c.COLUMN_SIZE as "MaxLength"
,   c.PRECISION_RADIX as "Precision"
,   0 as  "DateTimePrecision"
,   c.SCALE as "Scale"
,   CAST(NULL as varchar(1)) as  "CollationCatalog"
,   CAST(NULL as varchar(1)) as  "CollationSchema"
,   CAST(NULL as varchar(1)) as  "CollationName"
,   CAST(NULL as varchar(1)) as  "CharacterSetCatalog"
,   CAST(NULL as varchar(1)) as  "CharacterSetSchema"
,   CAST(NULL as varchar(1)) as  "CharacterSetName"
,   0 as "IsMultiSet"
,   0 as "IsIdentity"
,   0 as "IsStoreGenerated"
, c.COLUMN_DEFAULT as "Default"
FROM
TEMP.SCHEMACOLUMNS c
WHERE TABLE_SCHEMA != 'SYS'
) AS "Extent2"
UNION ALL
SELECT 
"Extent3"."Id" AS "Id", 
"Extent3"."Name" AS "Name", 
"Extent3"."Ordinal" AS "Ordinal", 
"Extent3"."IsNullable" AS "IsNullable", 
"Extent3"."TypeName" AS "TypeName", 
"Extent3"."MaxLength" AS "MaxLength", 
"Extent3"."Precision" AS "Precision", 
"Extent3"."DateTimePrecision" AS "DateTimePrecision", 
"Extent3"."Scale" AS "Scale", 
"Extent3"."IsIdentity" AS "IsIdentity", 
"Extent3"."IsStoreGenerated" AS "IsStoreGenerated", 
6 AS "C1", 
"Extent3"."ParentId" AS "ParentId"
FROM (
SELECT
c.TABLE_NAME || '.' || c.COLUMN_NAME as "Id"
,   c.TABLE_NAME as "ParentId"
,   c.COLUMN_NAME   as "Name"
,   c.ORDINAL_POSITION as "Ordinal"
,   1  as "IsNullable"
,   c.EDM_TYPE as "TypeName"
,   c.COLUMN_SIZE as "MaxLength"
,   c.PRECISION_RADIX as "Precision"
,   0 as  "DateTimePrecision"
,   c.SCALE as "Scale"
,   CAST(NULL as varchar(1)) as  "CollationCatalog"
,   CAST(NULL as varchar(1)) as  "CollationSchema"
,   CAST(NULL as varchar(1)) as  "CollationName"
,   CAST(NULL as varchar(1)) as  "CharacterSetCatalog"
,   CAST(NULL as varchar(1)) as  "CharacterSetSchema"
,   CAST(NULL as varchar(1)) as  "CharacterSetName"
,   0 as "IsMultiSet"
,   0 as "IsIdentity"
,   0 as "IsStoreGenerated"
, c.COLUMN_DEFAULT as "Default"
FROM
TEMP.SCHEMACOLUMNS c
WHERE TABLE_SCHEMA != 'SYS'
) AS "Extent3") AS "UnionAll1" ON (0 = "UnionAll1"."C1") AND ("Extent1"."Id" = "UnionAll1"."ParentId")
LEFT OUTER JOIN  (SELECT 
"UnionAll2"."Id" AS "C1", 
1 AS "C2"
FROM  (
SELECT
t.TABLESCHEMANAME || '.' || c.CONSTRAINTNAME as "Id",
t.TABLESCHEMANAME || '.' || t.TABLENAME as "ParentId",
c.CONSTRAINTNAME as "Name",
c.TYPE as "ConstraintType",
0 as "IsDeferrable",
0 as "IsInitiallyDeferred"
FROM SYS.SYSCONSTRAINTS c JOIN SYS.SYSTABLES t ON t.TABLEID = c.TABLEID
WHERE c.TYPE != 'C' AND t.TABLENAME IS NOT NULL
) AS "Extent4"
INNER JOIN  (SELECT 
7 AS "C1", 
"Extent5"."ConstraintId" AS "ConstraintId", 
"Extent6"."Id" AS "Id"
FROM  (
SELECT
CONSTRAINT_NAME AS "ConstraintId"
, TABLE_NAME || '.' || COLUMN_NAME    AS "ColumnId"
FROM
TEMP.SCHEMACONSTRAINTCOLUMNS
) AS "Extent5"
INNER JOIN (
SELECT
c.TABLE_NAME || '.' || c.COLUMN_NAME as "Id"
,   c.TABLE_NAME as "ParentId"
,   c.COLUMN_NAME   as "Name"
,   c.ORDINAL_POSITION as "Ordinal"
,   1  as "IsNullable"
,   c.EDM_TYPE as "TypeName"
,   c.COLUMN_SIZE as "MaxLength"
,   c.PRECISION_RADIX as "Precision"
,   0 as  "DateTimePrecision"
,   c.SCALE as "Scale"
,   CAST(NULL as varchar(1)) as  "CollationCatalog"
,   CAST(NULL as varchar(1)) as  "CollationSchema"
,   CAST(NULL as varchar(1)) as  "CollationName"
,   CAST(NULL as varchar(1)) as  "CharacterSetCatalog"
,   CAST(NULL as varchar(1)) as  "CharacterSetSchema"
,   CAST(NULL as varchar(1)) as  "CharacterSetName"
,   0 as "IsMultiSet"
,   0 as "IsIdentity"
,   0 as "IsStoreGenerated"
, c.COLUMN_DEFAULT as "Default"
FROM
TEMP.SCHEMACOLUMNS c
WHERE TABLE_SCHEMA != 'SYS'
) AS "Extent6" ON "Extent6"."Id" = "Extent5"."ColumnId"
UNION ALL
SELECT 
11 AS "C1", 
"Extent7"."ConstraintId" AS "ConstraintId", 
"Extent8"."Id" AS "Id"
FROM  (
SELECT
CAST(NULL as varchar(1))   as "ConstraintId",
CAST(NULL as varchar(1)) as "ColumnId"
FROM SYS.MEMBERS
WHERE 1=2
) AS "Extent7"
INNER JOIN (
SELECT
c.TABLE_NAME || '.' || c.COLUMN_NAME as "Id"
,   c.TABLE_NAME as "ParentId"
,   c.COLUMN_NAME   as "Name"
,   c.ORDINAL_POSITION as "Ordinal"
,   1  as "IsNullable"
,   c.EDM_TYPE as "TypeName"
,   c.COLUMN_SIZE as "MaxLength"
,   c.PRECISION_RADIX as "Precision"
,   0 as  "DateTimePrecision"
,   c.SCALE as "Scale"
,   CAST(NULL as varchar(1)) as  "CollationCatalog"
,   CAST(NULL as varchar(1)) as  "CollationSchema"
,   CAST(NULL as varchar(1)) as  "CollationName"
,   CAST(NULL as varchar(1)) as  "CharacterSetCatalog"
,   CAST(NULL as varchar(1)) as  "CharacterSetSchema"
,   CAST(NULL as varchar(1)) as  "CharacterSetName"
,   0 as "IsMultiSet"
,   0 as "IsIdentity"
,   0 as "IsStoreGenerated"
, c.COLUMN_DEFAULT as "Default"
FROM
TEMP.SCHEMACOLUMNS c
WHERE TABLE_SCHEMA != 'SYS'
) AS "Extent8" ON "Extent8"."Id" = "Extent7"."ColumnId") AS "UnionAll2" ON (7 = "UnionAll2"."C1") AND ("Extent4"."Id" = "UnionAll2"."ConstraintId")
WHERE "Extent4"."ConstraintType" = 'PRIMARY KEY' ) AS "Project5" ON "UnionAll1"."Id" = "Project5"."C1"
WHERE (("Extent1"."CatalogName" LIKE '') AND ("Extent1"."SchemaName" LIKE 'TESTAPP')) AND ("Extent1"."Name" LIKE 'TESTTABLE')
)  AS "Project6"
ORDER BY "Project6"."SchemaName" ASC, "Project6"."Name" ASC, "Project6"."C2" ASC 

--with 3 parameters ;value=,type=0;value=APP,type=0;value=TEST1,type=0
