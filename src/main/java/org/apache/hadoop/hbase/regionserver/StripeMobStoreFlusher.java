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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.util.Bytes;

public class StripeMobStoreFlusher extends StripeStoreFlusher {
  private static final Log LOG = LogFactory.getLog(StripeMobStoreFlusher.class);
  private final Object flushLock = new Object();
  private final StripeCompactionPolicy policy;
  private final StripeCompactionPolicy.StripeInformationProvider stripes;
  private boolean isMob = false;
  private int mobCellSizeThreshold = 0;
  private Path targetPath;
  private MobFileStore mobFileStore;
  private Object lock = new Object();
  private Path mobHome;

  public StripeMobStoreFlusher(Configuration conf, Store store, StripeCompactionPolicy policy,
      StripeStoreFileManager stripes) {
    super(conf, store, policy, stripes);
    this.policy = policy;
    this.stripes = stripes;
    isMob = MobUtils.isMobFamily(store.getFamily());
    mobCellSizeThreshold = conf.getInt(MobConstants.MOB_CELL_SIZE_THRESHOLD,
        MobConstants.DEFAULT_MOB_CELL_SIZE_THRESHOLD);
    mobHome = MobUtils.getMobHome(conf);
    Path mobPath = new Path(mobHome, new Path(store.getTableName().getNameAsString(),
        MobConstants.MOB_REGION_NAME));
    this.targetPath = new Path(mobPath, store.getColumnFamilyName());
  }

  @Override
  public List<Path> flushSnapshot(SortedSet<KeyValue> snapshot, long cacheFlushSeqNum,
      final TimeRangeTracker tracker, AtomicLong flushedSize, MonitoredTask status)
      throws IOException {
    List<Path> result = null;
    int kvCount = snapshot.size();
    if (kvCount == 0)
      return result; // don't flush if there are no entries

    long smallestReadPoint = store.getSmallestReadPoint();
    InternalScanner scanner = createScanner(snapshot, smallestReadPoint);
    if (scanner == null) {
      return result; // NULL scanner returned from coprocessor hooks means skip normal processing
    }

    // Let policy select flush method.
    StripeFlushRequest req = this.policy.selectFlush(this.stripes, kvCount);

    long flushedBytes = 0;
    boolean success = false;
    StripeMultiFileWriter mw = null;
    try {
      mw = req.createWriter(); // Writer according to the policy.
      StripeMultiFileWriter.WriterFactory factory = createWriterFactory(tracker, kvCount);
      StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner) scanner : null;
      mw.init(storeScanner, factory, store.getComparator());

      synchronized (flushLock) {
        if (!isMob) {
          flushedBytes = performFlush(scanner, mw, smallestReadPoint);
        } else {
          mobFileStore = currentMobFileStore();
          StoreFile.Writer mobFileWriter = null;
          int compactionKVMax = conf.getInt(HConstants.COMPACTION_KV_MAX,
              HConstants.COMPACTION_KV_MAX_DEFAULT);
          Iterator<KeyValue> iter = snapshot.iterator();
          int mobKVCount = 0;
          long time = 0;
          while (iter != null && iter.hasNext()) {
            KeyValue kv = iter.next();
            if (kv.getValueLength() >= mobCellSizeThreshold
                && !MobUtils.isMobReferenceKeyValue(kv)
                && kv.getTypeByte() == KeyValue.Type.Put.getCode()) {
              mobKVCount++;
              time = time < kv.getTimestamp() ? kv.getTimestamp() : time;
            }
          }
          mobFileWriter = mobFileStore.createWriterInTmp(new Date(time), mobKVCount, store
              .getFamily().getCompression(), store.getRegionInfo().getStartKey());
          // the target path is {tableName}/.mob/{cfName}/mobFiles
          // the relative path is mobFiles
          String relativePath = mobFileWriter.getPath().getName();
          byte[] referenceValue = Bytes.toBytes(relativePath);
          try {
            List<Cell> kvs = new ArrayList<Cell>();
            boolean hasMore;
            do {
              hasMore = scanner.next(kvs, compactionKVMax);
              if (!kvs.isEmpty()) {
                for (Cell c : kvs) {
                  // If we know that this KV is going to be included always, then let us
                  // set its memstoreTS to 0. This will help us save space when writing to
                  // disk.
                  KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                  if (kv.getMvccVersion() <= smallestReadPoint) {
                    // let us not change the original KV. It could be in the memstore
                    // changing its memstoreTS could affect other threads/scanners.
                    kv = kv.shallowCopy();
                    kv.setMvccVersion(0);
                  }

                  if (kv.getValueLength() < mobCellSizeThreshold
                      || MobUtils.isMobReferenceKeyValue(kv)
                      || kv.getTypeByte() != KeyValue.Type.Put.getCode()) {
                    mw.append(kv);
                  } else {
                    // append the original keyValue in the mob file.
                    mobFileWriter.append(kv);

                    // append the tag to the KeyValue.
                    // The key is same, the value is the filename of the mob file
                    List<Tag> existingTags = Tag
                        .asList(kv.getTagsArray(), kv.getTagsOffset(), kv.getTagsLength());
                    if (existingTags.isEmpty()) {
                      existingTags = new ArrayList<Tag>();
                    }
                    Tag mobRefTag = new Tag(MobConstants.MOB_REFERENCE_TAG_TYPE,
                        MobConstants.MOB_REFERENCE_TAG_VALUE);
                    existingTags.add(mobRefTag);
                    KeyValue reference = new KeyValue(kv.getRowArray(), kv.getRowOffset(),
                        kv.getRowLength(), kv.getFamilyArray(), kv.getFamilyOffset(),
                        kv.getFamilyLength(), kv.getQualifierArray(), kv.getQualifierOffset(),
                        kv.getQualifierLength(), kv.getTimestamp(), KeyValue.Type.Put,
                        referenceValue, 0, referenceValue.length, existingTags);
                    mw.append(reference);
                  }

                  flushedBytes += MemStore.heapSizeChange(kv, true);
                }
                kvs.clear();
              }
            } while (hasMore);
          } finally {
            status.setStatus("Flushing mob file " + store + ": appending metadata");
            mobFileWriter.appendMetadata(cacheFlushSeqNum, false);
            status.setStatus("Flushing mob file " + store + ": closing flushed file");
            mobFileWriter.close();

            // commit the mob file from temp folder to target folder.
            mobFileStore.commitFile(mobFileWriter.getPath(), targetPath);
          }
        }
        result = mw.commitWriters(cacheFlushSeqNum, false);
        success = true;
      }
    } finally {
      if (!success && (mw != null)) {
        if (result != null) {
          result.clear();
        }
        for (Path leftoverFile : mw.abortWriters()) {
          try {
            store.getFileSystem().delete(leftoverFile, false);
          } catch (Exception e) {
            LOG.error("Failed to delete a file after failed flush: " + e);
          }
        }
      }
      flushedSize.set(flushedBytes);
      try {
        scanner.close();
      } catch (IOException ex) {
        LOG.warn("Failed to close flush scanner, ignoring", ex);
      }
    }
    return result;
  }

  private StripeMultiFileWriter.WriterFactory createWriterFactory(
      final TimeRangeTracker tracker, final long kvCount) {
    return new StripeMultiFileWriter.WriterFactory() {
      public Writer createWriter() throws IOException {
        StoreFile.Writer writer = store.createWriterInTmp(
            kvCount, store.getFamily().getCompression(), false, true, true);
        writer.setTimeRangeTracker(tracker);
        return writer;
      }
    };
  }

  private MobFileStore currentMobFileStore() throws IOException {
    if (null == mobFileStore) {
      synchronized (lock) {
        if (null == mobFileStore) {
          this.store.getFileSystem().mkdirs(targetPath);
          mobFileStore = MobFileStore.create(conf, this.store.getFileSystem(), mobHome,
              this.store.getTableName(), this.store.getFamily());
        }
      }
    }
    return mobFileStore;
  }
}
