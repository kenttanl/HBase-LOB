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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * The mob utilities
 */
@InterfaceAudience.Private
public class MobUtils {

  private static final Log LOG = LogFactory.getLog(MobUtils.class);
  private static final long ONE_HOUR = 60 * 60 * 1000; // 1 hour

  private static final ThreadLocal<SimpleDateFormat> LOCAL_FORMAT =
      new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyyMMdd");
    }
  };

  /**
   * Indicates whether the column family is a mob one.
   * @param hcd The descriptor of a column family.
   * @return True if this column family is a mob one, false if it's not.
   */
  public static boolean isMobFamily(HColumnDescriptor hcd) {
    byte[] isMob = hcd.getValue(MobConstants.IS_MOB);
    return isMob != null && isMob.length == 1 && Bytes.toBoolean(isMob);
  }

  /**
   * Gets the mob threshold.
   * If the size of a cell value is larger than this threshold, it's regarded as a mob.
   * The default threshold is 1024*100(100K)B.
   * @param hcd The descriptor of a column family.
   * @return The threshold.
   */
  public static long getMobThreshold(HColumnDescriptor hcd) {
    byte[] threshold = hcd.getValue(MobConstants.MOB_THRESHOLD);
    return threshold != null && threshold.length == Bytes.SIZEOF_LONG ? Bytes.toLong(threshold)
        : MobConstants.DEFAULT_MOB_THRESHOLD;
  }

  /**
   * Gets the clean delay for the expired mob files. The default is 1 hour, and unit
   * is milliseconds.
   * When the creationTime(of a mob file) < current - TTL - delay, it's expired,
   * @param conf The current configuration.
   * @return The clean delay. The default is 1 hour.
   */
  public static long getCleanDelayOfExpiredMobFiles(Configuration conf) {
    return conf.getLong(MobConstants.MOB_CLEAN_DELAY, ONE_HOUR);
  }

  /**
   * Formats a date to a string.
   * @param date The date.
   * @return The string format of the date, it's yyyymmdd.
   */
  public static String formatDate(Date date) {
    return LOCAL_FORMAT.get().format(date);
  }

  /**
   * Parses the string to a date.
   * @param dateString The string format of a date, it's yyyymmdd.
   * @return A date.
   * @throws ParseException
   */
  public static Date parseDate(String dateString) throws ParseException {
    return LOCAL_FORMAT.get().parse(dateString);
  }

  /**
   * Whether the current cell is a mob reference cell.
   * @param cell The current cell.
   * @return True if the cell has a mob reference tag, false if it doesn't.
   */
  public static boolean isMobReferenceCell(Cell cell) {
    if (cell.getTagsLength() > 0) {
      Tag tag = Tag.getTag(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength(),
          MobConstants.MOB_REFERENCE_TAG_TYPE);
      return tag != null;
    }
    return false;
  }

  /**
   * Whether the tag list has a mob reference tag.
   * @param tags The tag list.
   * @return True if the list has a mob reference tag, false if it doesn't.
   */
  public static boolean hasMobReferenceTag(List<Tag> tags) {
    if (!tags.isEmpty()) {
      for (Tag tag : tags) {
        if (tag.getType() == MobConstants.MOB_REFERENCE_TAG_TYPE) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Indicates whether it's a raw scan.
   * The information is set in the attribute "hbase.mob.scan.raw" of scan.
   * For a mob cell, in a normal scan the scanners retrieves the mob cell from the mob file.
   * In a raw scan, the scanner directly returns cell in HBase without retrieve the one in
   * the mob file.
   * @param scan The current scan.
   * @return True if it's a raw scan.
   */
  public static boolean isRawMobScan(Scan scan) {
    byte[] raw = scan.getAttribute(MobConstants.MOB_SCAN_RAW);
    try {
      return raw != null && Bytes.toBoolean(raw);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Indicates whether the scan contains the information of caching blocks.
   * The information is set in the attribute "hbase.mob.cache.blocks" of scan.
   * @param scan The current scan.
   * @return True when the Scan attribute specifies to cache the MOB blocks.
   */
  public static boolean isCacheMobBlocks(Scan scan) {
    byte[] cache = scan.getAttribute(MobConstants.MOB_CACHE_BLOCKS);
    try {
      return cache != null && Bytes.toBoolean(cache);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Sets the attribute of caching blocks in the scan.
   *
   * @param scan
   *          The current scan.
   * @param cacheBlocks
   *          True, set the attribute of caching blocks into the scan, the scanner with this scan
   *          caches blocks.
   *          False, the scanner doesn't cache blocks for this scan.
   */
  public static void setCacheMobBlocks(Scan scan, boolean cacheBlocks) {
    scan.setAttribute(MobConstants.MOB_CACHE_BLOCKS, Bytes.toBytes(cacheBlocks));
  }

  /**
   * Cleans the expired mob files.
   * Cleans the files whose creation date is older than (current - columnFamily.ttl), and
   * the minVersions of that column family is 0.
   * @param fs The current file system.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param columnDescriptor The descriptor of the current column family.
   * @param cacheConfig The cacheConfig that disables the block cache.
   * @param current The current time.
   * @throws IOException
   */
  public static void cleanExpiredMobFiles(FileSystem fs, Configuration conf, TableName tableName,
      HColumnDescriptor columnDescriptor, CacheConfig cacheConfig, long current)
      throws IOException {
    long timeToLive = columnDescriptor.getTimeToLive();
    if (Integer.MAX_VALUE == timeToLive) {
      // no need to clean, because the TTL is not set.
      return;
    }

    long cleanDelay = getCleanDelayOfExpiredMobFiles(conf);
    Date expireDate = new Date(current - timeToLive * 1000 - cleanDelay);
    expireDate = new Date(expireDate.getYear(), expireDate.getMonth(), expireDate.getDate());
    LOG.info("MOB HFiles older than " + expireDate.toGMTString() + " will be deleted!");

    FileStatus[] stats = null;
    Path path = MobUtils.getMobFamilyPath(conf, tableName, columnDescriptor.getNameAsString());
    if (fs.exists(path)) {
      try {
        stats = fs.listStatus(path);
      } catch (FileNotFoundException e) {
      }
    }
    if (null == stats) {
      // no file found
      return;
    }
    List<StoreFile> storeFiles = new ArrayList<StoreFile>();
    int deletedFileCount = 0;
    for (FileStatus file : stats) {
      String fileName = file.getPath().getName();
      try {
        MobFileName mobFileName = null;
        if (!HFileLink.isHFileLink(file.getPath())) {
          mobFileName = MobFileName.create(fileName);
        } else {
          HFileLink hfileLink = new HFileLink(conf, file.getPath());
          mobFileName = MobFileName.create(hfileLink.getOriginPath().getName());
        }
        Date fileDate = parseDate(mobFileName.getDate());
        LOG.info("Checking file " + fileName);
        if (fileDate.getTime() < expireDate.getTime()) {
          LOG.info("It is an expired file " + fileName);
          storeFiles.add(new StoreFile(fs, file.getPath(), conf, cacheConfig, BloomType.NONE));
        }
      } catch (Exception e) {
        LOG.error("Cannot parse the fileName " + fileName, e);
      }
      if (!storeFiles.isEmpty() && storeFiles.size() % 10 == 0) {
        try {
          MobUtils.removeMobFiles(conf, fs, tableName, columnDescriptor.getName(), storeFiles);
          deletedFileCount += storeFiles.size();
        } catch (IOException e) {
          LOG.error("Fail to delete the mob files " + storeFiles, e);
        }
        storeFiles.clear();
      }
    }
    if (!storeFiles.isEmpty()) {
      try {
        MobUtils.removeMobFiles(conf, fs, tableName, columnDescriptor.getName(), storeFiles);
        deletedFileCount += storeFiles.size();
      } catch (IOException e) {
        LOG.error("Fail to delete the mob files " + storeFiles, e);
      }
      storeFiles.clear();
    }
    LOG.info(deletedFileCount + " expired mob files are deleted");
  }

  /**
   * Gets the root dir of the mob files.
   * It's {HBASE_DIR}/mobdir.
   * @param conf The current configuration.
   * @return the root dir of the mob file.
   */
  public static Path getMobHome(Configuration conf) {
    Path hbaseDir = new Path(conf.get(HConstants.HBASE_DIR));
    return new Path(hbaseDir, MobConstants.MOB_DIR_NAME);
  }

  /**
   * Gets the region dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @return The region dir of the mob files.
   */
  public static Path getMobRegionPath(Configuration conf, TableName tableName) {
    Path tablePath = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    HRegionInfo regionInfo = getMobRegionInfo(tableName);
    return new Path(tablePath, regionInfo.getEncodedName());
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param conf The current configuration.
   * @param tableName The current table name.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Configuration conf, TableName tableName, String familyName) {
    return new Path(getMobRegionPath(conf, tableName), familyName);
  }

  /**
   * Gets the family dir of the mob files.
   * It's {HBASE_DIR}/mobdir/{namespace}/{tableName}/{regionEncodedName}/{columnFamilyName}.
   * @param regionPath The path of mob region which is a dummy one.
   * @param familyName The current family name.
   * @return The family dir of the mob files.
   */
  public static Path getMobFamilyPath(Path regionPath, String familyName) {
    return new Path(regionPath, familyName);
  }

  /**
   * Gets the HRegionInfo of the mob files.
   * This is a dummy region. The mob files are not saved in a region in HBase.
   * This is only used in mob snapshot. It's internally used only.
   * @param tableName
   * @return A dummy mob region info.
   */
  public static HRegionInfo getMobRegionInfo(TableName tableName) {
    HRegionInfo info = new HRegionInfo(tableName, MobConstants.MOB_REGION_NAME_BYTES,
        HConstants.EMPTY_END_ROW, false, 0);
    return info;
  }

  /**
   * Gets the working directory of the mob compaction.
   * @param root The root directory of the mob compaction.
   * @param jobName The current job name.
   * @return The directory of the mob compaction for the current job.
   */
  public static Path getCompactionWorkingPath(Path root, String jobName) {
    Path parent = new Path(root, jobName);
    return new Path(parent, "working");
  }

  /**
   * Archives the mob files.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param tableName The table name.
   * @param family The name of the column family.
   * @param storeFiles The files to be deleted.
   * @throws IOException
   */
  public static void removeMobFiles(Configuration conf, FileSystem fs, TableName tableName,
      byte[] family, Collection<StoreFile> storeFiles) throws IOException {
    HFileArchiver.archiveStoreFiles(conf, fs, getMobRegionInfo(tableName),
        FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName), family, storeFiles);
  }

  /**
   * Creates a mob reference KeyValue.
   * The value of the mob reference KeyValue is mobCellValueSize + mobFileName.
   * @param kv The original KeyValue.
   * @param fileName The mob file name where the mob reference KeyValue is written.
   * @param tableNameTag The tag of the current table name. It's very important in
   *                        cloning the snapshot.
   * @return The mob reference KeyValue.
   */
  public static KeyValue createMobRefKeyValue(KeyValue kv, byte[] fileName, Tag tableNameTag) {
    // Append the tags to the KeyValue.
    // The key is same, the value is the filename of the mob file
    List<Tag> existingTags = Tag.asList(kv.getTagsArray(), kv.getTagsOffset(), kv.getTagsLength());
    existingTags.add(MobConstants.MOB_REF_TAG);
    // Add the tag of the source table name, this table is where this mob file is flushed
    // from.
    // It's very useful in cloning the snapshot. When reading from the cloning table, we need to
    // find the original mob files by this table name. For details please see cloning
    // snapshot for mob files.
    existingTags.add(tableNameTag);
    long valueLength = kv.getValueLength();
    byte[] refValue = Bytes.add(Bytes.toBytes(valueLength), fileName);
    KeyValue reference = new KeyValue(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
        kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength(), kv.getQualifierArray(),
        kv.getQualifierOffset(), kv.getQualifierLength(), kv.getTimestamp(), KeyValue.Type.Put,
        refValue, 0, refValue.length, existingTags);
    reference.setMvccVersion(kv.getMvccVersion());
    return reference;
  }

  /**
   * Creates the temp directory of mob files for flushing.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param family The descriptor of the current column family.
   * @param date The date string, its format is yyyymmmdd.
   * @param basePath The basic path for a temp directory.
   * @param maxKeyCount The key count.
   * @param compression The compression algorithm.
   * @param startKey The hex string of the start key.
   * @param cacheConfig The current cache config.
   * @return The writer for the mob file.
   * @throws IOException
   */
  public static StoreFile.Writer createWriterInTmp(Configuration conf, FileSystem fs,
      HColumnDescriptor family, String date, Path basePath, long maxKeyCount,
      Compression.Algorithm compression, String startKey, CacheConfig cacheConfig)
      throws IOException {
    MobFileName mobFileName = MobFileName.create(startKey, date, UUID.randomUUID().toString()
        .replaceAll("-", ""));
    HFileContext hFileContext = new HFileContextBuilder().withCompression(compression)
        .withIncludesMvcc(false).withIncludesTags(true)
        .withChecksumType(HFile.DEFAULT_CHECKSUM_TYPE)
        .withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM)
        .withBlockSize(family.getBlocksize()).withHBaseCheckSum(true)
        .withDataBlockEncoding(family.getDataBlockEncoding()).build();

    StoreFile.Writer w = new StoreFile.WriterBuilder(conf, cacheConfig, fs)
        .withFilePath(new Path(basePath, mobFileName.getFileName()))
        .withComparator(KeyValue.COMPARATOR).withBloomType(BloomType.NONE)
        .withMaxKeyCount(maxKeyCount).withFileContext(hFileContext).build();
    return w;
  }

  /**
   * Commits the mob file.
   * @param @param conf The current configuration.
   * @param fs The current file system.
   * @param path The path where the mob file is saved.
   * @param targetPath The directory path where the source file is renamed to.
   * @param cacheConfig The current cache config.
   * @throws IOException
   */
  public static void commitFile(Configuration conf, FileSystem fs, final Path sourceFile,
      Path targetPath, CacheConfig cacheConfig) throws IOException {
    if (sourceFile == null) {
      return;
    }
    Path dstPath = new Path(targetPath, sourceFile.getName());
    validateMobFile(conf, fs, sourceFile, cacheConfig);
    String msg = "Renaming flushed file from " + sourceFile + " to " + dstPath;
    LOG.info(msg);
    Path parent = dstPath.getParent();
    if (!fs.exists(parent)) {
      fs.mkdirs(parent);
    }
    if (!fs.rename(sourceFile, dstPath)) {
      throw new IOException("Failed rename of " + sourceFile + " to " + dstPath);
    }
  }

  /**
   * Validates a mob file by opening and closing it.
   * @param conf The current configuration.
   * @param fs The current file system.
   * @param path The path where the mob file is saved.
   * @param cacheConfig The current cache config.
   */
  private static void validateMobFile(Configuration conf, FileSystem fs, Path path,
      CacheConfig cacheConfig) throws IOException {
    StoreFile storeFile = null;
    try {
      storeFile = new StoreFile(fs, path, conf, cacheConfig, BloomType.NONE);
      storeFile.createReader();
    } catch (IOException e) {
      LOG.error("Fail to open mob file[" + path + "], keep it in temp directory.", e);
      throw e;
    } finally {
      if (storeFile != null) {
        storeFile.closeReader(false);
      }
    }
  }
}
