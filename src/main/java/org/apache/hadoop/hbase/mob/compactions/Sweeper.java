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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.MobFileStore;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ServiceException;

public class Sweeper extends Configured implements Tool {

  public void sweepFamily(String table, String familyName) throws IOException, InterruptedException,
      ClassNotFoundException, KeeperException, ServiceException {
    Configuration conf = getConf();
    HBaseAdmin.checkHBaseAvailable(conf);
    HBaseAdmin admin = new HBaseAdmin(conf);
    try {
      FileSystem fs = FileSystem.get(conf);
      if (!admin.tableExists(table)) {
        throw new IOException("Table " + table + " not exist");
      }
      HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(table));
      HColumnDescriptor family = htd.getFamily(Bytes.toBytes(familyName));
      if (!MobUtils.isMobFamily(family)) {
        throw new IOException("It's not a MOB column family");
      }
      MobFileStore store = MobFileStore.create(conf, fs, MobUtils.getMobHome(conf),
          TableName.valueOf(table), family);
      SweepJob job = new SweepJob(fs);
      job.sweep(store);
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