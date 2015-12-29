--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
--
-- Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you
-- may not use this file except in compliance with the License. You
-- may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
-- implied. See the License for the specific language governing
-- permissions and limitations under the License. See accompanying
-- LICENSE file.
--
-- test to verify that RTS functionality is stubbed out
call SYSCS_UTIL.SET_RUNTIMESTATISTICS(1);
create schema "check";
set schema 'check';
values 1, 2, 3;
values SYSCS_UTIL.GET_RUNTIMESTATISTICS();
-- GemStone added test for async insert below (#43403)

create table test.firsttable (id int primary key, name varchar(12));
insert into test.firsttable values (10,'TEN'),(20,'TWENTY'),(30,'THIRTY');
set isolation read committed;
autocommit off;
-- first check rollback
async aInsert 'insert into test.firsttable values (40,''Forty'')';
insert into test.firsttable values (50,'Fifty');
wait for aInsert;
select * from test.firsttable;
rollback;
select * from test.firsttable;
-- now insert again and check commit
async aInsert 'insert into test.firsttable values (40,''Forty'')';
insert into test.firsttable values (50,'Fifty');
wait for aInsert;
select * from test.firsttable;
commit;
select * from test.firsttable;
-- check with another connection
connect peer 'mcast-port=0';
select * from test.firsttable;
