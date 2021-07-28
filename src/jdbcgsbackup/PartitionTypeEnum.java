package jdbcgsbackup;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public enum PartitionTypeEnum {
    BY_RANGE("r", "BY RANGE", " VALUES LESS THAN ("),
    BY_INTERVAL("i", "BY INTERVAL", " VALUES LESS THAN ("),
    BY_LIST("l", "BY LIST", " VALUES ("),
    BY_HASH("h", "BY HASH", ""),
    BY_VALUES("v", "BY VALUES", "");

    private static Map<String, String> partitionTypeNameMap = new HashMap<String, String>();
    private static Map<String, String> partitionSqlStringMap = new HashMap<String, String>();

    private String typeCode;
    private String typeName;
    private String sqlString;

    static {
        initMap();
    }

    PartitionTypeEnum(String typeCode, String typeName, String sqlString) {
        this.typeCode = typeCode;
        this.typeName = typeName;
        this.sqlString = sqlString;
    }

    /**
     * Gets type code
     *
     * @return String the type code
     */
    public String getTypeCode(){
        return typeCode;
    }

    /**
     * Gets type name
     *
     * @return String the type name
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Gets sql string
     *
     * @return String the sql string
     */
    public String getSqlString() {
        return sqlString;
    }

    /**
     * Initialize map
     */
    private static void initMap() {
        partitionTypeNameMap = Arrays.stream(PartitionTypeEnum.values())
                .collect(Collectors.toMap(PartitionTypeEnum::getTypeCode, PartitionTypeEnum::getTypeName));
        partitionSqlStringMap = Arrays.stream(PartitionTypeEnum.values())
                .collect(Collectors.toMap(PartitionTypeEnum::getTypeCode, PartitionTypeEnum::getSqlString));
    }

    /**
     * Gets the type name
     *
     * @param String the type code
     * @return String the type name
     */
    public static String getTypeName(String typeCode) {
        return partitionTypeNameMap.get(typeCode);
    }

    /**
     * Gets the sql string
     *
     * @param String the type code
     * @return String the sql string
     */
    public static String getSqlString(String typeCode) {
        return partitionSqlStringMap.get(typeCode);
    }
}
