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
package org.apache.hadoop.hbase.mob.compactions;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.protobuf.ServiceException;

/**
 * The cleaner to delete the expired MOB files.
 */
@InterfaceAudience.Private
public class ExpiredMobFileCleaner extends Configured implements Tool {

  /**
   * Cleans the MOB files when they're expired and their min versions are 0.
   * If the latest timestamp of Cells in a MOB file is older than the TTL in the column family,
   * it's regarded as expired. This cleaner deletes them.
   * Users are allowed to configure a clean delay (hbase.mob.cleaner.delay) in the configuration
   * which allows the file is deleted when (latestTimeStamp < Now - TTL - delay).
   * @param tableName The current table name.
   * @param familyName The current family name.
   * @throws ServiceException
   * @throws IOException
   */
  void cleanExpiredMobFiles(String tableName, String familyName) throws ServiceException,
      IOException {
    Configuration conf = getConf();
    HBaseAdmin.checkHBaseAvailable(conf);
    TableName tn = TableName.valueOf(tableName);
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      FileSystem fs = FileSystem.get(conf);
      if (!admin.tableExists(tn)) {
        throw new IOException("Table " + tableName + " not exist");
      }
      HTableDescriptor htd = admin.getTableDescriptor(tn);
      HColumnDescriptor family = htd.getFamily(Bytes.toBytes(familyName));
      if (!MobUtils.isMobFamily(family)) {
        throw new IOException("It's not a MOB column family");
      }
      if(family.getMinVersions() > 0) {
        throw new IOException(
            "The minVersions of the column family is not 0, could not be handled by this cleaner");
      }
      System.out.println("Cleaning the expired MOB files...");
      // disable the block cache.
      Configuration copyOfConf = new Configuration(conf);
      copyOfConf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.00001f);
      CacheConfig cacheConfig = new CacheConfig(copyOfConf);
      MobUtils.cleanExpiredMobFiles(fs, conf, tn, family, cacheConfig,
          EnvironmentEdgeManager.currentTimeMillis());
    } finally {
      try {
        admin.close();
      } catch (IOException e) {
        System.out.println("Fail to close the HBaseAdmin: " + e.getMessage());
      }
    }
  }

  public static void main(String[] args) throws Exception {

    System.out.print("Usage:\n" + "--------------------------\n"
        + ExpiredMobFileCleaner.class.getName() + "[tableName] [familyName]");

    Configuration conf = HBaseConfiguration.create();
    ToolRunner.run(conf, new ExpiredMobFileCleaner(), args);
  }

  public int run(String[] args) throws Exception {
    if (args.length >= 2) {
      String tableName = args[0];
      String familyName = args[1];
      cleanExpiredMobFiles(tableName, familyName);
    }
    return 0;
  }

}
