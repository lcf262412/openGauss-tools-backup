/*
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012 Tomislav Gountchev <tomi@gountchev.net>
 */

package jdbcgsbackup;

public interface DataFilter {
    public boolean dumpData(String schema,String tableName);

    public static final DataFilter ALL_DATA = new DataFilter() {
        public boolean dumpData(String schema,String tableName) {
            return true;
        }
    };

    public static final DataFilter NO_DATA = new DataFilter() {
        public boolean dumpData(String schema,String tableName) {
            return false;
        }
    };
}
