set schema app;
grant delete on table employee_table to public;
grant select(empid),select(empname),select(empgrade),select(salary),select(doj) on table employee_table to test;
grant delete on table UNICODEPARSINGTABLETEST to public;
grant select(empid),select(empname),select(empgrade),select(salary),select(doj) on table UNICODEPARSINGTABLETEST to test;
