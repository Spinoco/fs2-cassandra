package spinoco.fs2.cassandra.system

import java.nio.ByteBuffer
import java.util.UUID


/**
  * V3+ Schema for tables
  */
case class TableSchema(
    keyspace_name:String 
    , table_name: String 
    , bloom_filter_fp_chance: Double
    , caching:Map[String ,String ]
    , comment:String 
    , compaction:Map[String ,String ]
    , compression:Map[String ,String ]
    , crc_check_chance:Double
    , dclocal_read_repair_chance:Double
    , default_time_to_live: Int
    , extensions:Map[String ,ByteBuffer]
    , flags:Set[String ]
    , gc_grace_seconds: Int
    , id: UUID
    , max_index_interval: Int
    , memtable_flush_period_in_ms: Int
    , min_index_interval: Int
    , read_repair_chance: Double
    , speculative_retry: String  
)

/**
  * V2 Schema for column families, aka tables
  */
case class ColumnFamilySchemaV2(
  keyspace_name :String
  , columnfamily_name :String
  , bloom_filter_fp_chance: Double
  , caching :String
  , cf_id :UUID
  , comment :String
  , compaction_strategy_class :String
  , compaction_strategy_options :String
  , comparator :String
  , compression_parameters :String
  , default_time_to_live :Int
  , default_validator :String
  , dropped_columns :Map[String, Long]
  , gc_grace_seconds :Int
  , is_dense :Boolean
  , key_validator :String
  , local_read_repair_chance :Double
  , max_compaction_threshold :Int
  , max_index_interval :Int
  , memtable_flush_period_in_ms :Int
  , min_compaction_threshold :Int
  , min_index_interval :Int
  , read_repair_chance :Double
  , speculative_retry :String
  , subcomparator :String
  , `type` :String
)