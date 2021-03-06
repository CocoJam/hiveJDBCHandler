package org.apache.hive.storage.jdbc.conf.config;

public class Constants {
    /* Constants for Hive stats */
    public static final String HIVE_ENGINE = "hive";

    /* Constants for LLAP */
    public static final String LLAP_LOGGER_NAME_QUERY_ROUTING = "query-routing";
    public static final String LLAP_LOGGER_NAME_CONSOLE = "console";
    public static final String LLAP_LOGGER_NAME_RFA = "RFA";
    public static final String LLAP_NUM_BUCKETS = "llap.num.buckets";
    public static final String LLAP_BUCKET_ID = "llap.bucket.id";

    /* Constants for Druid storage handler */
    public static final String DRUID_HIVE_STORAGE_HANDLER_ID =
            "org.apache.hadoop.hive.druid.DruidStorageHandler";
    public static final String DRUID_HIVE_OUTPUT_FORMAT =
            "org.apache.hadoop.hive.druid.io.DruidOutputFormat";
    public static final String DRUID_DATA_SOURCE = "druid.datasource";
    public static final String DRUID_SEGMENT_GRANULARITY = "druid.segment.granularity";

    public static final String DRUID_TARGET_SHARDS_PER_GRANULARITY =
            "druid.segment.targetShardsPerGranularity";
    public static final String DRUID_TIMESTAMP_GRANULARITY_COL_NAME = "__time_granularity";
    public static final String DRUID_SHARD_KEY_COL_NAME = "__druid_extra_partition_key";
    public static final String DRUID_QUERY_JSON = "druid.query.json";
    public static final String DRUID_QUERY_FIELD_NAMES = "druid.fieldNames";
    public static final String DRUID_QUERY_FIELD_TYPES = "druid.fieldTypes";
    public static final String DRUID_QUERY_TYPE = "druid.query.type";

    public static final String JDBC_HIVE_STORAGE_HANDLER_ID =
            "org.apache.hive.storage.jdbc.JdbcStorageHandler";
    public static final String JDBC_CONFIG_PREFIX = "hive.sql";
    public static final String JDBC_CATALOG = JDBC_CONFIG_PREFIX + ".catalog";
    public static final String JDBC_SCHEMA = JDBC_CONFIG_PREFIX + ".schema";
    public static final String JDBC_TABLE = JDBC_CONFIG_PREFIX + ".table";
    public static final String JDBC_DATABASE_TYPE = JDBC_CONFIG_PREFIX + ".database.type";
    public static final String JDBC_URL = JDBC_CONFIG_PREFIX + ".jdbc.url";
    public static final String JDBC_DRIVER = JDBC_CONFIG_PREFIX + ".jdbc.driver";
    public static final String JDBC_USERNAME = JDBC_CONFIG_PREFIX + ".dbcp.username";
    public static final String JDBC_PASSWORD = JDBC_CONFIG_PREFIX + ".dbcp.password";
    public static final String JDBC_KEYSTORE = JDBC_CONFIG_PREFIX + ".dbcp.password.keystore";
    public static final String JDBC_KEY = JDBC_CONFIG_PREFIX + ".dbcp.password.key";
    public static final String JDBC_QUERY = JDBC_CONFIG_PREFIX + ".query";
    public static final String JDBC_QUERY_FIELD_NAMES = JDBC_CONFIG_PREFIX + ".query.fieldNames";
    public static final String JDBC_QUERY_FIELD_TYPES = JDBC_CONFIG_PREFIX + ".query.fieldTypes";
    public static final String JDBC_SPLIT_QUERY = JDBC_CONFIG_PREFIX + ".query.split";
    public static final String JDBC_PARTITION_COLUMN = JDBC_CONFIG_PREFIX + ".partitionColumn";
    public static final String JDBC_NUM_PARTITIONS = JDBC_CONFIG_PREFIX + ".numPartitions";
    public static final String JDBC_LOW_BOUND = JDBC_CONFIG_PREFIX + ".lowerBound";
    public static final String JDBC_UPPER_BOUND = JDBC_CONFIG_PREFIX + ".upperBound";

    public static final String HIVE_SERVER2_JOB_CREDSTORE_PASSWORD_ENVVAR = "HIVE_JOB_CREDSTORE_PASSWORD";
    public static final String HADOOP_CREDENTIAL_PASSWORD_ENVVAR = "HADOOP_CREDSTORE_PASSWORD";
    public static final String HADOOP_CREDENTIAL_PROVIDER_PATH_CONFIG = "hadoop.security.credential.provider.path";

    public static final String MATERIALIZED_VIEW_REWRITING_TIME_WINDOW = "rewriting.time.window";
    public static final String MATERIALIZED_VIEW_SORT_COLUMNS = "materializedview.sort.columns";
    public static final String MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS = "materializedview.distribute.columns";
}
