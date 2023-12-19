alter session set container=XEPDB1;
create user orcl identified by orcl;
grant dba to orcl;
grant create session to orcl;
grant connect, resource to orcl;
grant all privileges to orcl;

