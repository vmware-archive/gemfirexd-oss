CREATE TABLE dunit_run(id serial primary key, user_name text, path text, sites integer, runtime_env integer, base_env integer);
CREATE TABLE dunit_test_run_detail(id serial primary key, dunit_run_id integer references dunit_run(id), pass boolean default null, message text);
CREATE TABLE dunit_test_method(id serial primary key, test_class_id integer references dunit_test_class(id),name text not null);
CREATE TABLE dunit_test_class(id serial primary key,name text not null);

CREATE TABLE dunit_test_method_detail(id serial primary key,method_id integer, status text,error text);

ALTER TABLE dunit_test_run_detail add column failCount integer;
ALTER TABLE dunit_test_run_detail add column tookMs bigint;
ALTER TABLE dunit_test_method_detail add column tookMs bigint;

ALTER TABLE dunit_run add column revision text;
ALTER TABLE dunit_run add column branch text;
ALTER TABLE dunit_run add column os_name text;
ALTER TABLE dunit_run add column os_version text;
ALTER TABLE dunit_run add column java_version text;
ALTER TABLE dunit_run add column java_vm_version text;
ALTER TABLE dunit_run add column java_vm_vendor text;

alter table dunit_test_method_detail add column run_id integer references dunit_run(id);
ALTER TABLE dunit_run add column time timestamp default current_timestamp;
ALTER TABLE dunit_test_run_detail add column time timestamp default current_timestamp;
ALTER TABLE dunit_test_method_detail add column time timestamp default current_timestamp;