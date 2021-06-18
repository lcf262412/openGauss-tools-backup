/*
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012 Tomislav Gountchev <tomi@gountchev.net>
 */

package jdbcgsbackup;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract class DbBackupObject {

    protected final String name;
    protected final Schema schema;
    protected final String owner;

    protected DbBackupObject(String name, Schema schema, String owner) {
        this.name = name;
        this.schema = schema;
        this.owner = owner;
    }

    final String getName() {
        String regex = "^[0-9a-z_]{1,}$";
        Matcher ma = Pattern.compile(regex).matcher(name);
        if (ma.matches()) {
            return name;
        }
        return "\"" + name + "\"";
    }

    String getFullname() {
        return schema.getName() + "." + getName();
    }

    final String getOwner() {
        return owner;
    }

    String getSql(DataFilter dataFilter) {
        StringBuilder buf = new StringBuilder();
        appendCreateSql(buf, dataFilter);
        return buf.toString();
    }

    protected abstract StringBuilder appendCreateSql(StringBuilder buf);
    
    protected StringBuilder appendCreateSql(StringBuilder buf, DataFilter dataFilter) {
        return appendCreateSql(buf);
    }

}
