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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.SweepCounter;
import org.apache.hadoop.hbase.mob.compactions.SweepJob;
import org.apache.hadoop.hbase.mob.compactions.SweepReducer.SweepPartitionId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * The wrapper of a DefaultMemStore.
 * This wrapper is used in the sweep reducer to buffer and sort the cells written from
 * the invalid and small mob files.
 * It's flushed when it's full, the mob data are writren to the mob files, and their file names
 * are written back to store files of HBase.
 */
public class MemStoreWrapper {

  private static final Log LOG = LogFactory.getLog(MemStoreWrapper.class);

  private MemStore memstore;
  private long flushSize;
  private SweepPartitionId partitionId;
  private Context context;
  private Configuration conf;
  private HTable table;
  private HColumnDescriptor hcd;
  private Path mobFamilyDir;
  private FileSystem fs;
  private CacheConfig cacheConfig;

  public MemStoreWrapper(Context context, FileSystem fs, HTable table, HColumnDescriptor hcd,
      MemStore memstore, CacheConfig cacheConfig) throws IOException {
    this.memstore = memstore;
    this.context = context;
    this.fs = fs;
    this.table = table;
    this.hcd = hcd;
    this.conf = context.getConfiguration();
    this.cacheConfig = cacheConfig;
    flushSize = this.conf.getLong(MobConstants.MOB_COMPACTION_MEMSTORE_FLUSH_SIZE,
        HTableDescriptor.DEFAULT_MEMSTORE_FLUSH_SIZE);
    mobFamilyDir = MobUtils.getMobFamilyPath(conf, table.getName(), hcd.getNameAsString());
  }

  public void setPartitionId(SweepPartitionId partitionId) {
    this.partitionId = partitionId;
  }

  /**
   * Flushes the memstore if the size is large enough.
   * @throws IOException
   */
  public void flushMemStoreIfNecessary() throws IOException {
    if (memstore.heapSize() >= flushSize) {
      flushMemStore();
    }
  }

  /**
   * Flushes the memstore anyway.
   * @throws IOException
   */
  public void flushMemStore() throws IOException {
    memstore.snapshot();
    SortedSet<KeyValue> snapshot = memstore.getSnapshot();
    internalFlushCache(snapshot);
    memstore.clearSnapshot(snapshot);
  }

  /**
   * Flushes the snapshot of the memstore.
   * Flushes the mob data to the mob files, and flushes the name of these mob files to HBase.
   * @param snapshot The snapshot of the memstore.
   * @throws IOException
   */
  private void internalFlushCache(final SortedSet<KeyValue> snapshot)
      throws IOException {
    if (snapshot.size() == 0) {
      return;
    }
    // generate the files into a temp directory.
    String tempPathString = context.getConfiguration().get(SweepJob.WORKING_FILES_DIR_KEY);
    StoreFile.Writer mobFileWriter = MobUtils.createWriterInTmp(conf, fs, hcd,
        partitionId.getDate(), new Path(tempPathString), snapshot.size(),
        hcd.getCompactionCompression(), partitionId.getStartKey(), cacheConfig);

    String relativePath = mobFileWriter.getPath().getName();
    LOG.info("Create files under a temp directory " + mobFileWriter.getPath().toString());

    byte[] referenceValue = Bytes.toBytes(relativePath);
    int keyValueCount = 0;
    KeyValueScanner scanner = new CollectionBackedScanner(snapshot, KeyValue.COMPARATOR);
    scanner.seek(KeyValue.createFirstOnRow(HConstants.EMPTY_START_ROW));
    Cell cell = null;
    while (null != (cell = scanner.next())) {
      KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
      mobFileWriter.append(kv);
      keyValueCount++;
    }
    scanner.close();
    // Write out the log sequence number that corresponds to this output
    // hfile. The hfile is current up to and including logCacheFlushId.
    mobFileWriter.appendMetadata(Long.MAX_VALUE, false);
    mobFileWriter.close();

    MobUtils.commitFile(conf, fs, mobFileWriter.getPath(), mobFamilyDir, cacheConfig);
    context.getCounter(SweepCounter.FILE_AFTER_MERGE_OR_CLEAN).increment(1);
    // write reference/fileName back to the store files of HBase.
    scanner = new CollectionBackedScanner(snapshot, KeyValue.COMPARATOR);
    scanner.seek(KeyValue.createFirstOnRow(HConstants.EMPTY_START_ROW));
    cell = null;
    while (null != (cell = scanner.next())) {
      List<Tag> existingTags = Tag.asList(cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
      if (existingTags.isEmpty()) {
        existingTags = new ArrayList<Tag>();
      }
      // the cell whose value is the name of a mob file has such a tag.
      Tag mobRefTag = new Tag(MobConstants.MOB_REFERENCE_TAG_TYPE, HConstants.EMPTY_BYTE_ARRAY);
      existingTags.add(mobRefTag);
      long valueLength = cell.getValueLength();
      byte[] newValue = Bytes.add(Bytes.toBytes(valueLength), referenceValue);

      KeyValue reference = new KeyValue(cell.getRowArray(), cell.getRowOffset(),
          cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(),
          cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
          cell.getQualifierLength(), cell.getTimestamp(), KeyValue.Type.Put, newValue, 0,
          newValue.length, existingTags);

      Put put = new Put(reference.getRow());
      put.add(reference);
      table.put(put);
      context.getCounter(SweepCounter.RECORDS_UPDATED).increment(1);
    }
    if (keyValueCount > 0) {
      table.flushCommits();
    }
    scanner.close();
  }

  /**
   * Adds a KeyValue into the memstore.
   * @param kv The KeyValue to be added.
   */
  public void addToMemstore(KeyValue kv) {
    memstore.add(kv);
  }

}
