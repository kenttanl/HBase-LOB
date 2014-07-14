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
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.MobZookeeper;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.zookeeper.KeeperException;

public class HMobStore extends HStore {

  public HMobStore(final HRegion region, final HColumnDescriptor family,
      final Configuration confParam) throws IOException {
    super(region, family, confParam);
  }

  @Override
  public KeyValueScanner getScanner(Scan scan, NavigableSet<byte[]> targetCols, long readPt)
      throws IOException {
    lock.readLock().lock();
    try {
      KeyValueScanner scanner = null;
      if (this.getCoprocessorHost() != null) {
        scanner = this.getCoprocessorHost().preStoreScannerOpen(this, scan, targetCols);
      }
      if (scanner == null) {
        scanner = scan.isReversed() ? new MobReversedStoreScanner(this, getScanInfo(), scan,
            targetCols, readPt)
            : new MobStoreScanner(this, getScanInfo(), scan, targetCols, readPt);
      }
      return scanner;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public List<StoreFile> compact(CompactionContext compaction) throws IOException {
    // If it's major compaction, try to find whether there's a sweeper is running
    // If yes, change the major compaction to a minor one.
    if (compaction.getRequest().isMajor() && MobUtils.isMobFamily(getFamily())) {
      MobZookeeper zk = null;
      try {
        zk = MobZookeeper.newInstance(this.getHRegion().conf);
      } catch (KeeperException e) {
        LOG.error("Cannot connect to the zookeeper, ready to perform the minor compaction instead",
            e);
        // change the major compaction into a minor one
        compaction.getRequest().setIsMajor(false);
        return super.compact(compaction);
      }
      boolean major = false;
      String compactionName = UUID.randomUUID().toString().replaceAll("-", "");
      try {
        if (zk.lockStore(getTableName().getNameAsString(), getFamily().getNameAsString())) {
          try {
            LOG.info("Obtain the lock for the store[" + this
                + "], ready to perform the major compaction");
            // check the sweeper znode
            boolean hasSweeper = zk.isSweeperZNodeExist(getTableName().getNameAsString(),
                getFamily().getNameAsString());
            if (!hasSweeper) {
              // if not, add a child region znode to the family znode
              major = zk.addMajorCompactionZNode(getTableName().getNameAsString(), getFamily()
                  .getNameAsString(), compactionName);
            }
          } catch (Exception e) {
            LOG.error("Fail to handle the Zookeeper", e);
          } finally {
            zk.unlockStore(getTableName().getNameAsString(), getFamily().getNameAsString());
          }
        }
        try {
          if (major) {
            return super.compact(compaction);
          } else {
            LOG.info("Cannot obtain the lock or there's another major compaction for the store["
                + this + "], ready to perform the minor compaction instead");
            // change the major compaction into a minor one
            compaction.getRequest().setIsMajor(false);
            return super.compact(compaction);
          }
        } finally {
          if (major) {
            try {
              zk.deleteMajorCompactionZNode(getTableName().getNameAsString(), getFamily()
                  .getNameAsString(), compactionName);
            } catch (KeeperException e) {
              LOG.error("Fail to delete the compaction znode" + compactionName, e);
            }
          }
        }
      } finally {
        zk.close();
      }
    } else {
      return super.compact(compaction);
    }
  }
}
