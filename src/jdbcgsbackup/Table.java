/*
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012 Tomislav Gountchev <tomi@gountchev.net>
 */

package jdbcgsbackup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

final class Table extends DbBackupObject {
    /*
     * {orientation=row,compression=no}
     */
    private String options;
    public String getOptions() {
        return options;
    }
    public void setOptions(String options) {
        this.options = options;
    }
    /*
     * tableType: for unloggged, Global temporary TABLE
     */
    private String tableType = "";
    public String getTableType() {
        return tableType;
    }
    public void setTableType(String tableType) {
        this.tableType = tableType;
    }
    /*
     * partition table
     */
    private boolean isPartitionTable = false;
    
    public boolean getIsPartitionTable() {
        return isPartitionTable;
    }
    public void setIsPartitionTable(boolean isPartitionTable) {
        this.isPartitionTable = isPartitionTable;
    }
    
    private String partitionContent;
    public String getPartitionContent() {
        return partitionContent;
    }
    public void setPartitionContent(String partitionContent) {
        this.partitionContent = partitionContent;
    }
    
    private String partitionType = "n";
    public String getPartitionType() {
        return partitionType;
    }
    public void setPartitionType(String partitionType) {
        this.partitionType = partitionType;
    }

    static class TableFactory implements DBOFactory<Table> {

        @Override
        public Iterable<Table> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
            ZipBackup.timerStart("tables");
            List<Table> tables = new ArrayList<Table>();
            PreparedStatement stmt = null;
            try {
                stmt = con.prepareStatement("SELECT pg_get_userbyid(c.relowner) AS tableowner, " +
                        "c.relname AS tablename, c.oid AS table_oid " +
                        ", c.parttype as table_parttype " +
                        ", c.relpersistence as table_type " +
                        ", c.reloptions as options " +
                        "FROM pg_class c " +
                        "WHERE c.relkind = 'r'::\"char\" AND c.relnamespace = ?");
                stmt.setInt(1, schema.getOid());
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    Table table = new Table(rs.getString("tablename"), schema, rs.getString("tableowner"));
                    String tableType = rs.getString("table_type");
                    if ("u".equals(tableType)) {
                        table.setTableType("UNLOGGED ");
                    } else if ("g".equals(tableType)) {
                        table.setTableType("GLOBAL TEMPORARY ");
                    }
                    String options = rs.getString("options");
                    table.setOptions("(" + options.substring(1, options.length() - 1) + ")");
                    loadColumns(con, table, rs.getInt("table_oid"));
                    if (!"n".equals(rs.getString("table_parttype"))) {
                        table.setIsPartitionTable(true);
                        table.setPartitionType(rs.getString("table_parttype"));
                        String partition = loadPartitions(con, table, rs.getInt("table_oid"), rs.getString("table_parttype"));
                        table.setPartitionContent(partition);
                    }
                    tables.add(table);
                }
                rs.close();
            } finally {
                if (stmt != null) stmt.close();
            }
            ZipBackup.timerEnd("tables");
            return tables;
        }

        // does not load columns
        @Override
        public Table getDbBackupObject(Connection con, String tableName, Schema schema) throws SQLException {
            return getDbBackupObject(con, tableName, schema, false);
        }

        public Table getDbBackupObject(Connection con, String tableName, Schema schema, boolean loadColumns) throws SQLException {
            Table table = null;
            PreparedStatement stmt = null;
            try {
                stmt = con.prepareStatement("SELECT pg_get_userbyid(c.relowner) AS tableowner, c.oid AS table_oid " +
                        "FROM pg_class c " +
                        "WHERE c.relkind = 'r'::\"char\" AND c.relnamespace = ? " +
                        "AND c.relname = ?");
                stmt.setInt(1, schema.getOid());
                stmt.setString(2, tableName);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    table = new Table(tableName, schema, rs.getString("tableowner"));
                    if (loadColumns) loadColumns(con, table, rs.getInt("table_oid"));
                } else {
                    throw new RuntimeException("no such table: " + tableName);
                }
                rs.close();
            } finally {
                if (stmt != null) stmt.close();
            }
            return table;
        }

        private void loadColumns(Connection con, Table table, int tableOid) throws SQLException {
            PreparedStatement stmt = null;
            try {
                stmt = con.prepareStatement(
                        "SELECT a.attname,a.atttypid," +
                                "a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod," +
                                "row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, " +
                                "pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,t.typtype " +
                                "FROM pg_catalog.pg_attribute a " +
                                "JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) " +
                                "LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) " +
                                "WHERE a.attnum > 0 AND NOT a.attisdropped " +
                        "AND a.attrelid = ? ");
                stmt.setInt(1, tableOid);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    table.columns.add(table.new Column((BaseConnection)con, rs));
                }
                rs.close();
            } finally {
                if (stmt != null) stmt.close();
            }
        }
        
        public static String loadPartitions(Connection con, Table table, int tableOid, String partType) throws SQLException {
            StringBuffer buf = new StringBuffer();
            if (partType.equals("p")) {
                buf.append("\r\nPARTITION BY RANGE(");
            } else if (partType.equals("v")) {
                buf.append("\r\nPARTITION BY VALUES(");
            } else {
                buf.append("");
            }
            List <Column> list = new ArrayList<Column>(table.columns);
            PreparedStatement stmt = null;
            String query = "SELECT array_length(partkey, 1) AS partkeynum, "
                    + "partkey FROM pg_partition WHERE "
                    + "parentid = ? AND parttype = 'r';";
            try {
                stmt = con.prepareStatement(query);
                stmt.setInt(1, tableOid);
                ResultSet rs = stmt.executeQuery();
                int size = -1;
                List<Integer> partkey = null;
                while (rs.next()) {
                    size = rs.getInt("partkeynum");
                    String[] partKeyString = rs.getString("partkey").split(" ");
                    partkey = new ArrayList<Integer>();
                    for(int k = 0;k < partKeyString.length; k++) {
                        partkey.add(Integer.parseInt(partKeyString[k]));
                    }
                    for(int i=0;i<size;i++) {
                        if (i != 0) {
                            buf.append(", ");
                        }
                        int columnIndex = partkey.get(i);
                        buf.append(list.get(columnIndex - 1).name);
                    }
                }
                buf.append(")\r\n");
                if (!partType.equals("v")) {
                    buf.append("(");
                    buf.append(buildPartitionRangeNumber(con, table, tableOid, size, partkey));
                    buf.append("\r\n)");
                    buf.append("\r\nENABLE ROW MOVEMENT");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return buf.toString();
            
            
        }
        
        private static String buildPartitionRangeNumber(Connection con, Table table, int tableOid, int size, List<Integer> partkey) {
            StringBuilder query = new StringBuilder();
            query.append("SELECT p.relname AS partName, ");
            String partBoundaryTitle = "partBoundary_";
            for(int i = 1; i <= size; i++) {
                query.append(String.format("p.boundaries[%d] AS %s%d, ", i, partBoundaryTitle, i));
            }
            String subQuery = "t.spcname AS reltblspc "
                    + "FROM pg_partition p LEFT JOIN pg_tablespace t "
                    + "ON p.reltablespace = t.oid "
                    + "WHERE p.parentid = %d AND p.parttype = '%s' "
                    + "AND p.partstrategy = '%s' ORDER BY ";
            query.append(String.format(subQuery, tableOid, "p", "r"));
            for(int i = 1; i <= size; i++) {
                List<Column> list = new ArrayList<Column>(table.columns);
                String curAttTypeName = list.get(partkey.get(i - 1) - 1).typeName;
                if (i == size) {
                    query.append(String.format("p.boundaries[%d]::%s ASC", i, curAttTypeName));
                } else {
                    query.append(String.format("p.boundaries[%d]::%s, ", i, curAttTypeName));
                }
            }
            StringBuilder sb = new StringBuilder();
            try {
                PreparedStatement ps = con.prepareStatement(query.toString());
                ResultSet rs = ps.executeQuery();
                while(rs.next()) {
                    sb.append(buildPartitionRangeNumberOne(rs, size, partBoundaryTitle));
                }
                sb.deleteCharAt(sb.length()-1);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return sb.toString();
        }
        
        private static StringBuilder buildPartitionRangeNumberOne(ResultSet rs, int size, String partBoundaryTitle) {
            StringBuilder sb = new StringBuilder();
            String partName = null;
            String relTableSpace = null;
            List<Integer> results = new ArrayList<>();
            try {
                partName = rs.getString("partName");
                if (rs.wasNull()) {
                    partName = null;
                }
                for(int i = 1; i <= size; i++) {
                    Integer tmpV = Integer.parseInt(rs.getString(partBoundaryTitle + i));
                    if(rs.wasNull()) {
                        tmpV = null;
                    }
                    results.add(tmpV);
                }
                relTableSpace = rs.getString("reltblspc");
                if(rs.wasNull()) {
                    relTableSpace = "pg_default";
                }
                sb.append("\r\n");
                sb.append("PARTITION " + partName + " VALUES LESS THAN (");
                for(int i = 0; i < size; i++) {
                    Integer tmpV = results.get(i);
                    if (i > 0) {
                        sb.append(", ");
                    }
                    if (tmpV == null) {
                        sb.append("MAXVALUE");
                    } else {
                        sb.append(tmpV);
                    }
                }
                sb.append(")");
                sb.append(" TABLESPACE " + relTableSpace);
                sb.append(",");
            } catch(SQLException e) {
                e.printStackTrace();
            }
            return sb;
        }
    }

    static class CachingTableFactory extends CachingDBOFactory<Table> {

        private final Map<Integer,Table> oidMap;

        protected CachingTableFactory(Schema.CachingSchemaFactory schemaFactory) {
            super(schemaFactory);
            oidMap = new HashMap<Integer,Table>();
        }

        @Override
        protected PreparedStatement getAllStatement(Connection con) throws SQLException {
            return con.prepareStatement("SELECT c.relnamespace AS schema_oid, c.relname AS tablename, " + 
                    "pg_get_userbyid(c.relowner) AS tableowner, c.oid " +
                    "FROM pg_class c " +
                    "WHERE c.relkind = 'r'::\"char\"");
        }

        @Override
        protected Table newDbBackupObject(Connection con, ResultSet rs, Schema schema) throws SQLException {
            Table table = new Table(rs.getString("tablename"), schema, rs.getString("tableowner"));
            oidMap.put(rs.getInt("oid"), table);
            return table;
        }

        @Override
        protected void loadMap(Connection con) throws SQLException {
            ZipBackup.timerStart("tables");
            super.loadMap(con);
            ZipBackup.timerEnd("tables");
            loadColumns(con);
        }

        Table getTable(int table_oid) {
            return oidMap.get(table_oid);
        }

        private void loadColumns(Connection con) throws SQLException {
            ZipBackup.timerStart("load columns");
            ZipBackup.debug("begin loading columns...");
            PreparedStatement stmt = null;
            try {
                stmt = con.prepareStatement(
                        "SELECT a.attrelid AS table_oid, a.attname, a.atttypid," +
                                "a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull, a.atttypmod, " +
                                "row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, " +
                                "pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc, t.typtype " +
                                "FROM pg_catalog.pg_attribute a " +
                                "JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) " +
                                "LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) " +
                        "WHERE a.attnum > 0 AND NOT a.attisdropped ");
                int count = 0;
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    int oid = rs.getInt("table_oid");
                    Table table = oidMap.get(oid);
                    if (table != null) {
                        table.columns.add(table.new Column((BaseConnection)con, rs));
                        if (++count%100000 == 1) ZipBackup.debug("loaded " + count + " columns");
                        String query = "select c.oid as table_oid, c.relname as tablename, c.parttype as table_parttype, " + 
                                "c.relpersistence as table_type, c.reloptions as options from pg_class c where c.oid = ?";
                        PreparedStatement stmt1 = con.prepareStatement(query);
                        stmt1.setInt(1, oid);
                        ResultSet rs1 = stmt1.executeQuery();
                        while(rs1.next()) {
                            String tableType = rs1.getString("table_type");
                            if ("u".equals(tableType)) {
                                table.setTableType("UNLOGGED ");
                            } else if ("g".equals(tableType)) {
                                table.setTableType("GLOBAL TEMPORARY ");
                            }
                            String options = rs1.getString("options");
                            table.setOptions("(" + options.substring(1, options.length() - 1) + ")");
                            if (!"n".equals(rs1.getString("table_parttype"))) {
                                table.setIsPartitionTable(true);
                                table.setPartitionType(rs1.getString("table_parttype"));
                                String partition = Table.TableFactory.loadPartitions(con, table, rs1.getInt("table_oid"), rs1.getString("table_parttype"));
                                table.setPartitionContent(partition);
                            }        
                        }
                    }
                }
                rs.close();
            } finally {
                if (stmt != null) stmt.close();
            }
            ZipBackup.debug("end loading columns");
            ZipBackup.timerEnd("load columns");        
        }
        
        private void loadPartition() throws SQLException {
            ZipBackup.timerStart("load partitions");
            ZipBackup.debug("begin loading partitions...");
            ZipBackup.debug("end loading partitions");
            ZipBackup.timerEnd("load partitions");    
        }

    }

    public final Set<Column> columns = new TreeSet<Column>(
            new Comparator<Column>() { 
                public int compare(Column a, Column b) {
                    return a.position - b.position;
                };
            });
    
    private final Set<Partition> partitions = new TreeSet<Partition>(
            new Comparator<Partition>() { 
                public int compare(Partition a, Partition b) {
                    return a.partitionValue - b.partitionValue;
                };
            });

    private Table(String name, Schema schema, String owner) {
        super(name, schema, owner);
    }

    @Override
    protected StringBuilder appendCreateSql(StringBuilder buf) {
        buf.append("CREATE ");
        buf.append(getTableType());
        buf.append("TABLE ");
        buf.append(getName());
        buf.append(" (");
        for (Column column : columns) {
            buf.append("\r\n");
            column.appendSql(buf);
            buf.append(",");
        }
        buf.deleteCharAt(buf.length()-1);
        buf.append("\r\n");
        buf.append(")\r\n");
        buf.append("WITH " + getOptions());
        if (isPartitionTable) {
            buf.append(getPartitionContent());
        }
        buf.append(";\n");
        for (Column column : columns) {
            column.appendSequenceSql(buf);
        }
        return buf;
    }

    void dump(Connection con, OutputStream os) throws SQLException, IOException {
        CopyManager copyManager = ((PGConnection)con).getCopyAPI();
        copyManager.copyOut("COPY " + getFullname() + " TO STDOUT BINARY", os);
    }

    void restore(InputStream is, Connection con) throws SQLException, IOException {
        CopyManager copyManager = ((PGConnection)con).getCopyAPI();
        copyManager.copyIn("COPY " + getFullname() + " FROM STDIN BINARY", is);
    }

    private static final Set<String> appendSizeTo = new HashSet<String>(
            Arrays.asList("bit", "varbit", "bit varying", "bpchar", "char", "varchar", "character", "character varying"));

    private static final Set<String> appendPrecisionTo = new HashSet<String>(
            Arrays.asList("time", "timestamp", "timetz", "timestamptz"));

    private class Column {

        private final String name;
        private final String typeName;
        private /*final*/ int columnSize;
        private final int decimalDigits;
        private final int nullable;
        private final String defaultValue;
        private final boolean isAutoincrement;
        private final String sequenceName;
        private final int position;

        private Column(BaseConnection con, ResultSet rs) throws SQLException {

            int typeOid = (int)rs.getLong("atttypid");
            int typeMod = rs.getInt("atttypmod");

            position = rs.getInt("attnum");
            name = rs.getString("attname");
            typeName = con.getTypeInfo().getPGType(typeOid);
            decimalDigits = con.getTypeInfo().getScale(typeOid, typeMod);
            columnSize = con.getTypeInfo().getPrecision(typeOid, typeMod);
            if (columnSize == 0) {
                columnSize = con.getTypeInfo().getDisplaySize(typeOid, typeMod);
            }
            if (columnSize == Integer.MAX_VALUE) {
                columnSize = 0;
            }
            nullable = rs.getBoolean("attnotnull") ? java.sql.DatabaseMetaData.columnNoNulls : java.sql.DatabaseMetaData.columnNullable;
            String columnDef = rs.getString("adsrc");
            if (columnDef != null) {
                defaultValue = columnDef.replace("nextval('" + schema.getName() + ".", "nextval('"); // remove schema name
                isAutoincrement = columnDef.indexOf("nextval(") != -1;
            } else {
                defaultValue = null;
                isAutoincrement = false;
            }
            if (isAutoincrement) {
                PreparedStatement stmt = null;
                try {
                    stmt = con.prepareStatement(
                            "SELECT pg_get_serial_sequence( ? , ? ) AS sequencename");
                    stmt.setString(1, getFullname());
                    stmt.setString(2, name);
                    ResultSet rs2 = stmt.executeQuery();
                    if (rs2.next() && rs2.getString("sequencename") != null) {
                        sequenceName = rs2.getString("sequencename").replace(schema.getName() + ".", "");
                    } else {
                        sequenceName = null;
                    }
                    rs2.close();
                } finally {
                    if (stmt != null) stmt.close();
                }
            } else sequenceName = null;
        }

        private StringBuilder appendSql(StringBuilder buf) {
            buf.append(name).append(" ");
            buf.append(typeName);
            if (appendSizeTo.contains(typeName) && columnSize > 0) {
                buf.append( "(").append(columnSize).append(")");
            } else if (appendPrecisionTo.contains(typeName) || typeName.startsWith("interval")) {
                buf.append("(").append(decimalDigits).append(")");
            } else if ("numeric".equals(typeName) || "decimal".equals(typeName)) {
                if (columnSize <= 1000) {
                    buf.append("(").append(columnSize).append(",").append(decimalDigits).append(")");
                }
            }
            if (defaultValue != null) {
                buf.append(" DEFAULT ").append(defaultValue);
            }
            if (nullable == java.sql.DatabaseMetaData.columnNoNulls) {
                buf.append(" NOT NULL");
            }
            return buf;
        }

        private StringBuilder appendSequenceSql(StringBuilder buf) {
            if (sequenceName == null) return buf;
            buf.append("ALTER SEQUENCE ");
            buf.append(sequenceName);
            buf.append(" OWNED BY ");
            buf.append(getName());
            buf.append(".").append(name);
            buf.append(" ;\n");
            return buf;
        }

    }

    private class Partition {
        private final String partitionName;
        private final int partitionValue;
        private final String partitionTableSpace = "pg_default";
        
        private Partition(BaseConnection con, ResultSet rs) throws SQLException {
            partitionName = rs.getString("partitionName");
            partitionValue = rs.getInt("partitionValue");
        }
        
        private StringBuilder appendPartitionSql(StringBuilder buf) {
            String partType = getPartitionType();
            if (partType.equals("p")) {
                buf.append("PARTITION BY RANGE(");
            } else if (partType.equals("v")) {
                buf.append("PARTITION BY VALUES(");
            } else {
                return buf;
            }
            return buf;
        }

    }

    private enum PartType {
        PARTTYPE_PARTITIONED_RELATION("p"),
        PARTTYPE_VALUE_PARTITIONED_RELATION("v"),
        PARTTYPE_NON_PARTITIONED_RELATION("n"),
        UNKNOWN("");
        public final String partType;
        PartType(String partType) {
            this.partType = partType;
        }
    }
}
