# openGauss-tools-backup

#### Introduction
JdbcGsBackup is a Java tool, which uses JDBC to backup and recover the objects in openGauss databases. The database objects supported for backup and recovery include:

- Tables (table, unlogged table, global temporary table, partition table) and table data

- Views and materialized views

- Sequences and sequence value

- Constraints

- Indexes

The function of this tool is similar to that of gs_dump、gs_dumpall、gs_restore in openGauss databases.

#### Usage
The source code or jar package can be used to import and export the objects in openGauss databases, of which the parameters to be input are as follows:

```
-m dump|restore [-h hostname] [-p port] [-t (timing)] [-d database] [-U user] [-P password] [-f filename] [-o (schema only)] [-s schema[,schema...]] [-n schema[,schema...]]
```

The meaning of each parameter is as follows:

- Parameter **-m** specifies the mode, which is dump or restore, corresponding to the export (backup) or import (restore) operation, and it's a required parameter;
- Parameter **-h** specifies the host name. If not specified, the default value is localhost;

- Parameter **-p** specifies the port. If not specified, the default value is 5432;
- Parameter **-t** collects and displays time information and occupancy percentage of each step, which is an optional parameter;
- Parameter **-d** specifies the database name. If not specified, the default value is the user name;
- Parameter **-U** specifies the user name. If not specified, the default value is postgres;
- Parameter **-P** specifies password, which is a required parameter;
- Parameter **-f** specifies the file name to import and export. If not specified, the default file name is opengauss_ backup.zip；
- Parameter **-o** specifies not to import or export data, only to export or import DDL, which is an optional parameter;
- Parameter **-s** specifies the schemas to import and export, with multiple schemas separated by commas;
- Parameter **-n** specifies the schemas to import. If it is specified, the number of schemas should be the same as that specified by the - s parameter, and multiple schemas should be separated by commas.

JdbcGsBackup tool adopts the standard zip file format, parameter - f specifies the file name to import and export, and the specific file structure is as follows:

```
gs_backup/  
gs_backup/schemas.sql  
gs_backup/schemas/  
gs_backup/schemas/<schema1>/  
gs_backup/schemas/<schema1>/indexes.sql  
gs_backup/schemas/<schema1>/views.sql  
gs_backup/schemas/<schema1>/constraints.sql  
gs_backup/schemas/<schema1>/sequences.sql  
gs_backup/schemas/<schema1>/tables.sql  
gs_backup/schemas/<schema1>/tables/  
gs_backup/schemas/<schema1>/tables/<table1>  
gs_backup/schemas/<schema1>/tables/<table2>  
...  
gs_backup/schemas/<schema2>/  
...  
```

The xxx.sql file includes the corresponding SQL statement, and tables/<table_name> is the corresponding table data file, which is stored in binary format.

#### Source code analysis

- **Dump function:** The database objects are obtained based on the system tables, including the tables, views, sequences, constraints and indexes of the specific schema, and the DDL statement is constructed and wrote to the specified file according to the query result set. The copyOut function of CopyManager in JDBC is called to export the table data.

- **Restore function:** According to the specified file, read xxx.sql file in turn and according to semicolon, separate different SQL statements and execute them to complete the import operation of database objects. The copyIn function of CopyManager in JDBC is called to import the table data.

