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
import java.util.Date;
import java.util.UUID;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFile;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class MobFileStore {

  private static final Log LOG = LogFactory.getLog(MobFileStore.class);
  private Configuration conf;
  private FileSystem fs;
  private Path homePath;
  private CacheConfig cacheConf;
  private String tableName;
  private HColumnDescriptor family;
  private Path mobFamilyPath;
  private static final int MIN_BLOCK_SIZE = 1024;
  private final static String TMP = ".tmp";

  private MobFileStore(Configuration conf, FileSystem fs, Path homedPath, String tableName,
      HColumnDescriptor family) {
    this.fs = fs;
    this.homePath = homedPath;
    this.conf = conf;
    this.cacheConf = new CacheConfig(conf, family);
    this.tableName = tableName;
    this.family = family;
    Path mobPath = new Path(homePath, new Path(tableName, MobConstants.MOB_REGION_NAME));
    this.mobFamilyPath = new Path(mobPath, family.getNameAsString());
  }

  public static MobFileStore create(Configuration conf, FileSystem fs, Path familyPath,
      String tableName, HColumnDescriptor family) throws IOException {
    if (null == family) {
      LOG.warn("fail to create the MobFileStore because the family is null in table [" + tableName
          + "]!");
      return null;
    }
    String familyName = family.getNameAsString();
    if (!MobUtils.isMobFamily(family)) {
      LOG.warn("failed to create the MobFileStore because the family [" + familyName
          + "] in table [" + tableName + "] is not a mob one!");
      return null;
    }
    return new MobFileStore(conf, fs, familyPath, tableName, family);
  }

  public static MobFileStore create(Configuration conf, FileSystem fs, Path mobHome,
      TableName tableName, HColumnDescriptor family) throws IOException {
    return create(conf, fs, mobHome, tableName.getNameAsString(), family);
  }

  public HColumnDescriptor getColumnDescriptor() {
    return this.family;
  }

  public Path getPath() {
    return mobFamilyPath;
  }

  public Path getTmpDir() {
    return new Path(homePath, TMP);
  }

  public String getTableName() {
    return tableName;
  }

  public String getFamilyName() {
    return family.getNameAsString();
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  public StoreFile.Writer createWriterInTmp(Date date, int maxKeyCount,
      Compression.Algorithm compression, byte[] startKey) throws IOException {
    if (null == startKey || startKey.length == 0) {
      startKey = new byte[1];
      startKey[0] = 0x00;
    }

    CRC32 crc = new CRC32();
    crc.update(startKey);
    int checksum = (int) crc.getValue();
    return createWriterInTmp(date, maxKeyCount, compression, MobFileName.int2HexString(checksum));
  }

  public StoreFile.Writer createWriterInTmp(Date date, int maxKeyCount,
      Compression.Algorithm compression, String startKey) throws IOException {
    Path path = getTmpDir();
    return createWriterInTmp(MobUtils.formatDate(date), path, maxKeyCount, compression, startKey);
  }

  public StoreFile.Writer createWriterInTmp(String date, Path basePath, int maxKeyCount,
      Compression.Algorithm compression, String startKey) throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, maxKeyCount, date, UUID.randomUUID()
        .toString().replaceAll("-", ""));
    Path mobFilePath = new Path(mobFamilyPath, mobFileName.getFileName());
    final CacheConfig writerCacheConf = cacheConf;
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withChecksumType(HFile.DEFAULT_CHECKSUM_TYPE)
        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM).withBlockSize(MIN_BLOCK_SIZE)
        .withHBaseCheckSum(true).withDataBlockEncoding(DataBlockEncoding.NONE).build();

    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf, fs)
        .withFilePath(mobFilePath).withComparator(KeyValue.COMPARATOR)
        .withBloomType(BloomType.NONE).withMaxKeyCount(maxKeyCount).withFileContext(hFileContext)
        .build();
    return w;
  }

  public void commitFile(final Path sourceFile, Path targetPath) throws IOException {
    if (null == sourceFile) {
      throw new NullPointerException();
    }

    Path dstPath = new Path(targetPath, sourceFile.getName());
    validateStoreFile(sourceFile);
    String msg = "Renaming flushed file from " + sourceFile + " to " + dstPath;
    LOG.info(msg);

    Path parent = dstPath.getParent();
    if (!fs.exists(parent)) {
      fs.mkdirs(parent);
    }

    if (!fs.rename(sourceFile, dstPath)) {
      LOG.warn("Unable to rename " + sourceFile + " to " + dstPath);
    }
  }

  private void validateStoreFile(Path path) throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = new StoreFile(this.fs, path, conf, this.cacheConf, BloomType.NONE);

      storeFile.createReader();
    } catch (IOException e) {
      LOG.error("Failed to open lob store file[" + path + "], keeping it in tmp location["
          + getTmpDir() + "].", e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeReader(false);
      }
    }
  }

  public KeyValue resolve(KeyValue reference, boolean cacheBlocks) throws IOException {
    byte[] referenceValue = reference.getValue();
    String fileName = Bytes.toString(referenceValue);
    Path targetPath = new Path(mobFamilyPath, fileName);
    MobFile file = MobFile.create(fs, targetPath, conf, cacheConf);
    KeyValue result = file.readKeyValue(reference, cacheBlocks);

    if (result == null) {
      LOG.warn("The KeyValue result is null, assemble a new KeyValue with the same row,family,"
          + "qualifier,timestamp,type and tags but with an empty value to return.");
      result = new KeyValue(reference.getRowArray(), reference.getRowOffset(),
          reference.getRowLength(), reference.getFamilyArray(), reference.getFamilyOffset(),
          reference.getFamilyLength(), reference.getQualifierArray(),
          reference.getQualifierOffset(), reference.getQualifierLength(), reference.getTimestamp(),
          Type.codeToType(reference.getTypeByte()), HConstants.EMPTY_BYTE_ARRAY,
          reference.getValueOffset(), 0, reference.getTags());
    }
    return result;
  }
}
