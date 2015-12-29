.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

.. _`GemFireXD`: http://www.vmware.com/products/application-platform/vfabric-gemfirexd
.. _`Reference Manual`: https://www.vmware.com/support/pubs/vfabric-gemfirexd.html

GemFireXD
=========

This version of DdlUtils packaged with `Pivotal GemFireXD`_ supports `GemFireXD` version 1.0 and newer.
The SQL syntax and datatypes supported by `GemFireXD`_ are described in the Datatypes section
in the `vFabric GemFireXD Reference`_.

Constraints
-----------

Platform identifier
  ``GemFireXD``

Recognized JDBC drivers
  | ``com.pivotal.gemfirexd.jdbc.ClientDriver``
  | ``com.pivotal.gemfirexd.jdbc.EmbeddedDriver``

Recognized JDBC sub protocols
  ``jdbc:gemfirexd``

Supports SQL comments
  yes

Supports delimited identifiers
  yes

Maximum identifier length
  128

Supports default values for ``LONG`` types
  yes

Supports non-unique indices
  yes

Supports non-primary key columns as identity columns
  yes

Allows ``INSERT``/``UPDATE`` statements to set values for identity columns
  yes

DdlUtils uses sequences for identity columns
  yes

DdlUtils can read back the auto-generated value of an identity column
  no

DdlUtils can create a database via JDBC
  n/a

DdlUtils can drop a database via JDBC
  n/a

Datatypes
---------

+-----------------+--------------------------------+---------------------------------------------+
|JDBC Type        |Database Type                   |Additional comments                          |
+=================+================================+=============================================+
|``ARRAY``        |``BLOB``                        |Will be read back as ``BLOB``                |
+-----------------+--------------------------------+---------------------------------------------+
|``BIGINT``       |``BIGINT``                      |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``BINARY``       |``CHAR(n) FOR BIT DATA``        |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``BIT``          |``SMALLINT``                    |GemFireXD has no native boolean type.          |
|                 |                                |Will be read back as ``SMALLINT``            |
+-----------------+--------------------------------+---------------------------------------------+
|``BLOB``         |``BLOB``                        |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``BOOLEAN``      |``SMALLINT``                    |GemFireXD has no native boolean type.          |
|                 |                                |Will be read back as ``SMALLINT``            |
+-----------------+--------------------------------+---------------------------------------------+
|``CHAR``         |``CHAR``                        |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``CLOB``         |``CLOB``                        |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``DATALINK``     |``LONG VARCHAR FOR BIT DATA``   |Will be read back as ``LONGVARBINARY``       |
+-----------------+--------------------------------+---------------------------------------------+
|``DATE``         |``DATE``                        |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``DECIMAL``      |``DECIMAL``                     |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``DISTINCT``     |``BLOB``                        |Will be read back as ``BLOB``                |
+-----------------+--------------------------------+---------------------------------------------+
|``DOUBLE``       |``DOUBLE``                      |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``FLOAT``        |``DOUBLE``                      |Will be read back as ``DOUBLE``              |
+-----------------+--------------------------------+---------------------------------------------+
|``INTEGER``      |``INTEGER``                     |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``JAVA_OBJECT``  |``BLOB``                        |Will be read back as ``BLOB``                |
+-----------------+--------------------------------+---------------------------------------------+
|``LONGVARBINARY``|``LONG VARCHAR FOR BIT DATA``   |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``LONGVARCHAR``  |``LONG VARCHAR``                |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``NULL``         |``LONG VARCHAR FOR BIT DATA``   |Will be read back as ``LONGVARBINARY``       |
+-----------------+--------------------------------+---------------------------------------------+
|``NUMERIC``      |``NUMERIC``                     |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``OTHER``        |``BLOB``                        |Will be read back as ``BLOB``                |
+-----------------+--------------------------------+---------------------------------------------+
|``REAL``         |``REAL``                        |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``REF``          |``LONG VARCHAR FOR BIT DATA``   |Will be read back as ``LONGVARBINARY``       |
+-----------------+--------------------------------+---------------------------------------------+
|``SMALLINT``     |``SMALLINT``                    |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``STRUCT``       |``BLOB``                        |Will be read back as ``BLOB``                |
+-----------------+--------------------------------+---------------------------------------------+
|``TIME``         |``TIME``                        |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``TIMESTAMP``    |``TIMESTAMP``                   |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``TINYINT``      |``SMALLINT``                    |Will be read back as ``SMALLINT``            |
+-----------------+--------------------------------+---------------------------------------------+
|``VARBINARY``    |``VARCHAR(n) FOR BIT DATA``     |                                             |
+-----------------+--------------------------------+---------------------------------------------+
|``VARCHAR``      |``VARCHAR``                     |                                             |
+-----------------+--------------------------------+---------------------------------------------+
