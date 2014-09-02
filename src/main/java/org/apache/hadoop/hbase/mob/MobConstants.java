/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.util.Bytes;

public class MobConstants {

  public static final byte[] IS_MOB = Bytes.toBytes("isMob");
  public static final byte[] MOB_THRESHOLD = Bytes.toBytes("mobThreshold");
  public static final long DEFAULT_MOB_THRESHOLD = 0;

  public static final String MOB_SCAN_RAW = "hbase.mob.scan.raw";
  public static final String MOB_CACHE_BLOCKS = "hbase.mob.cache.blocks";

  public static final String MOB_FILE_CACHE_SIZE_KEY = "hbase.mob.file.cache.size";
  public static final int DEFAULT_MOB_FILE_CACHE_SIZE = 1000;

  public static final String MOB_DIR_NAME = "mobdir";
  public static final String MOB_REGION_NAME = ".mob";
  public static final byte[] MOB_REGION_NAME_BYTES = Bytes.toBytes(MOB_REGION_NAME);

  public static final String MOB_CLEAN_DELAY = "hbase.mob.cleaner.delay";
  public static final String MOB_COMPACTION_START_DATE = "hbase.mob.compaction.start.date";

  public static final String MOB_COMPACTION_INVALID_FILE_RATIO = 
      "hbase.mob.compaction.invalid.file.ratio";
  public static final String MOB_COMPACTION_SMALL_FILE_THRESHOLD = 
      "hbase.mob.compaction.small.file.threshold";

  public static final float DEFAULT_MOB_COMPACTION_INVALID_FILE_RATIO = 0.7f;
  public static final long DEFAULT_MOB_COMPACTION_SMALL_FILE_THRESHOLD = 64 * 1024 * 1024;// 64M

  public static final String DEFAULT_MOB_COMPACTION_TEMP_DIR_NAME = "mobcompaction";

  public static final String MOB_COMPACTION_MEMSTORE_FLUSH_SIZE = 
      "hbase.mob.compaction.memstore.flush.size";

  public static final String MOB_CACHE_EVICT_PERIOD = "hbase.mob.cache.evict.period";
  public static final String MOB_CACHE_EVICT_REMAIN_RATIO = "hbase.mob.cache.evict.remain.ratio";
  public static final byte MOB_REFERENCE_TAG_TYPE = Byte.MAX_VALUE;
  public static final byte MOB_TABLE_NAME_TAG_TYPE = Byte.MAX_VALUE - 1;
  public static final Tag MOB_REF_TAG = new Tag(MOB_REFERENCE_TAG_TYPE,
      HConstants.EMPTY_BYTE_ARRAY);

  public static final float DEFAULT_EVICT_REMAIN_RATIO = 0.5f;
  public static final long DEFAULT_MOB_CACHE_EVICT_PERIOD = 3600l;

  public final static String TEMP_DIR_NAME = ".tmp";

  private MobConstants() {

  }
}
