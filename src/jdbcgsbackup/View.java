/*
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012 Tomislav Gountchev <tomi@gountchev.net>
 */

package jdbcgsbackup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

final class View extends DbBackupObject {

    static class ViewFactory implements DBOFactory<View> {

        @Override
        public Iterable<View> getDbBackupObjects(Connection con, Schema schema) throws SQLException {
            List<View> views = new ArrayList<View>();
            PreparedStatement stmt = null;
            PreparedStatement stmt1 = null;
            try {
                stmt = con.prepareStatement(
                        "SELECT * FROM pg_views WHERE schemaname = ?");
                stmt.setString(1, schema.getName());
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    views.add(new View(rs.getString("viewname"), schema, rs.getString("viewowner"), rs.getString("definition"), false));
                }
                stmt1 = con.prepareStatement(
                        "SELECT * FROM gs_matviews WHERE schemaname = ?");
                stmt1.setString(1, schema.getName());
                rs = stmt1.executeQuery();
                while (rs.next()) {
                    views.add(new View(rs.getString("matviewname"), schema, rs.getString("matviewowner"), rs.getString("definition"), true));
                }
                rs.close();
            } finally {
                if (stmt != null) stmt.close();
                if (stmt1 != null) stmt1.close();
            }
            return views;
        }

        @Override
        public View getDbBackupObject(Connection con, String viewName, Schema schema) throws SQLException {
            View view = null;
            PreparedStatement stmt = null;
            try {
                stmt = con.prepareStatement(
                        "SELECT * FROM pg_views WHERE schemaname = ? AND viewname = ?");
                stmt.setString(1, schema.getName());
                stmt.setString(2, viewName);
                ResultSet rs = stmt.executeQuery();
                if (rs.next())
                    view = new View(viewName, schema, rs.getString("viewowner"), rs.getString("definition"), false);
                else
                    throw new RuntimeException("no such view: " + viewName);
                rs.close();
            } finally {
                if (stmt != null) stmt.close();
            }
            return view;
        }
    }

    static class CachingViewFactory extends CachingDBOFactory<View> {

        protected CachingViewFactory(Schema.CachingSchemaFactory schemaFactory) {
            super(schemaFactory);
        }

        @Override
        protected final PreparedStatement getAllStatement(Connection con) throws SQLException {
            return con.prepareStatement(
                    "SELECT c.relnamespace AS schema_oid, c.relname AS viewname, pg_get_userbyid(c.relowner) AS viewowner, " +
                            "pg_get_viewdef(c.oid) AS definition " +
                            ", c.relkind as relkind " +
                            "FROM pg_class c " +
                    "WHERE c.relkind = 'v'::\"char\" or c.relkind = 'm'::\"char\"");
        }

        @Override
        protected final View newDbBackupObject(Connection con, ResultSet rs, Schema schema) throws SQLException {
            return new View(rs.getString("viewname"), schema,
                    rs.getString("viewowner"), rs.getString("definition"), "m".equals(rs.getString("relkind")));    
        }

    }

    private final String definition;
    private final boolean isMaterView;

    private View(String name, Schema schema, String owner, String definition, Boolean isMaterView) {
        super(name, schema, owner);
        this.definition = definition;
        this.isMaterView = isMaterView;
    }

    @Override
    protected StringBuilder appendCreateSql(StringBuilder buf) {
        buf.append("CREATE ");
        if(isMaterView) {
            buf.append("MATERIALIZED ");
        }
        buf.append("VIEW ");
        buf.append(getName());
        buf.append(" AS ");
        buf.append(definition);
        buf.append("\n");
        return buf;
    }

}
