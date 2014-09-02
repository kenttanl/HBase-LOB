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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ServiceException;

/**
 * The sweep tool. It deletes the mob files that are not used and merges the small mob files to
 * bigger ones. Each running of this sweep tool is only handle one column family. Each running on
 * the same column family are mutually exclusive. And the major compaction and sweep tool on the
 * same column family are mutually exclusive too.
 */
@InterfaceAudience.Public
public class Sweeper extends Configured implements Tool {

  /**
   * Sweeps the mob files on one column family. It deletes the unused mob files and merges
   * the small mob files into bigger ones.
   * @param tableName The current table name in string format.
   * @param familyName The column family name.
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws KeeperException
   * @throws ServiceException
   */
  void sweepFamily(String tableName, String familyName) throws IOException, InterruptedException,
      ClassNotFoundException, KeeperException, ServiceException {
    Configuration conf = getConf();
    // make sure the target HBase exists.
    HBaseAdmin.checkHBaseAvailable(conf);
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      FileSystem fs = FileSystem.get(conf);
      if (!admin.tableExists(tableName)) {
        throw new IOException("Table " + tableName + " not exist");
      }
      HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(tableName));
      HColumnDescriptor family = htd.getFamily(Bytes.toBytes(familyName));
      if (!MobUtils.isMobFamily(family)) {
        throw new IOException("It's not a MOB column family");
      }
      SweepJob job = new SweepJob(conf, fs);
      // Run the sweeping
      job.sweep(TableName.valueOf(tableName), family);
    } finally {
      try {
        admin.close();
      } catch (IOException e) {
        System.out.println("Fail to close the HBaseAdmin: " + e.getMessage());
      }
    }
  }

  public static void main(String[] args) throws Exception {

    System.out.print("Usage:\n" + "--------------------------\n" + Sweeper.class.getName()
        + "[tableName] [familyName]");

    Configuration conf = HBaseConfiguration.create();
    ToolRunner.run(conf, new Sweeper(), args);
  }

  public int run(String[] args) throws Exception {
    if (args.length >= 2) {
      String table = args[0];
      String family = args[1];
      sweepFamily(table, family);
    }
    return 0;
  }
}