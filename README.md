# openGauss-tools-backup

#### 介绍

JdbcGsBackup为一个Java工具，其利用JDBC实现对openGauss数据库中的对象进行备份和恢复。现支持的备份和恢复的数据库对象包括：

- 表（普通表、无日志表、全局临时表、分区表）及表数据
- 视图和物化视图
- 序列和序列值
- 约束
- 索引

该工具实现的功能与openGauss内置的工具gs_dump、gs_dumpall、gs_restore类似。

#### 工具用法

可以用源代码或者Jar包实现对数据库对象的导入和导出，需传入的参数为：

```
-m dump|restore [-h hostname] [-p port] [-t (timing)] [-d database] [-U user] [-P password] [-f filename] [-o (schema only)] [-s schema[,schema...]] [-n schema[,schema...]]
```

各参数含义说明如下：

- -m 指定模式，取值为dump或者restore，对应导出（备份）或者导入（恢复）操作，必需参数；
- -h 指定主机名，若不指定，默认为localhost；
- -p 指定端口号，若不指定，默认为5432；
- -t 收集并显示每个步骤的时间信息及占用百分比，可选参数；
- -d 指定数据库名称，若不指定，默认为用户名；
- -U 指定用户名，若不指定，默认为postgres；
- -P 指定密码，必需参数；
- -f 指定导入导出的文件名，若不指定，默认导出导入文件名为openGauss_backup.zip；
- -o 指定不导入导出数据，只导出导入DDL，可选参数；
- -s 指定导入导出的schema名称，多个schema以逗号分隔；
- -n 指定导入的schema名称，若指定，需与-s参数指定的schema个数相同，多个schema以逗号分隔。

JdbcGsBackup工具采用标准的zip文件格式，-f指定导入导出的文件名，具体的文件结构为：

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

其中xxx.sql文件即为对应的SQL语句，tables/<table_name>即为对应表table_name的数据，其以二进制格式存储。

#### 源码分析

- dump功能：根据系统表查询相应的数据库对象，包括schema下的表、视图、序列、约束、索引，根据查询结果集构造相应的DDL语句，并写入到指定的文件中；对于表数据的导出，调用了JDBC中CopyManager的copyOut函数。
- restore功能：按照指定的文件，依次读取xxx.sql文件，按照分号；分隔不同的SQL语句并执行，即可完成数据库对象的导入操作；对于表数据的导入，调用了JDBC中CopyManager的copyIn函数。

