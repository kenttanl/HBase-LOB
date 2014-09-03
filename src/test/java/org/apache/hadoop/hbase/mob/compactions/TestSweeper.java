package org.apache.hadoop.hbase.mob.compactions;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.DefaultMobStoreFlusher;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFlusher;
import org.apache.hadoop.hbase.regionserver.HMobRegion;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSweeper {


  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private String tableName;
  private final static String row = "row_";
  private final static String family = "family";
  private final static String column = "column";
  private static HTable table;
  private static HBaseAdmin admin;

  private Random random = new Random();
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    TEST_UTIL.getConfiguration().setClass("hbase.hregion.impl", HMobRegion.class,
        HRegion.class);
    TEST_UTIL.getConfiguration().setClass(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DefaultMobStoreFlusher.class, DefaultStoreFlusher.class);

    TEST_UTIL.startMiniCluster();

    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    long tid = System.currentTimeMillis();
    tableName = "testSweeper" + tid;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, Bytes.toBytes(Boolean.TRUE));
    hcd.setValue(MobConstants.MOB_THRESHOLD, Bytes.toBytes(3L));
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);

    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    table.setAutoFlush(false);

  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(TableName.valueOf(tableName));
    admin.deleteTable(TableName.valueOf(tableName));
    admin.close();
  }

  private Path getMobFamilyPath(Configuration conf, String tableNameStr,
                                String familyName) {
    Path p = new Path(MobUtils.getMobRegionPath(conf, TableName.valueOf(tableNameStr)),
            familyName);
    return p;
  }


  private String mergeString(Set<String> set) {
    StringBuilder sb = new StringBuilder();
    for (String s : set)
      sb.append(s);
    return sb.toString();
  }


  private void generateMobTable(int count, int flushStep)
          throws IOException, InterruptedException {
    if (count <= 0 || flushStep <= 0)
      return;
    int index = 0;
    for (int i = 0; i < count; i++) {
      byte[] mobVal = new byte[101*1024];
      random.nextBytes(mobVal);

      Put put = new Put(Bytes.toBytes(row + i));
      put.add(Bytes.toBytes(family), Bytes.toBytes(column), mobVal);
      table.put(put);
      if (index++ % flushStep == 0) {
        table.flushCommits();
        admin.flush(tableName);
      }


    }
    table.flushCommits();
    admin.flush(tableName);
  }

  @Test
  public void testSweeper() throws Exception {

    int count = 10;
    //create table and generate 10 mob files
    generateMobTable(count, 1);

    //get mob files
    Path mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    FileStatus[] fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    // mobFileSet0 stores the orignal mob files
    TreeSet<String> mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    //scan the table, retreive the references
    Scan scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setFilter(new ReferenceOnlyFilter());
    ResultScanner rs = table.getScanner(scan);
    TreeSet<String> mobFilesScanned = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family),
          Bytes.toBytes(column));
      mobFilesScanned.add(Bytes.toString(valueBytes, 8, valueBytes.length - 8));
    }

    //there should be 10 mob files
    assertEquals(10, mobFilesScanned.size());
    //check if we store the correct reference of mob files
    assertEquals(mergeString(mobFilesSet), mergeString(mobFilesScanned));


    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(SweepJob.MOB_COMPACTION_DELAY, 24 * 60 * 60 * 1000);

    String[] args = new String[2];
    args[0] = tableName;
    args[1] = family;
    ToolRunner.run(conf, new Sweeper(), args);


    mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    assertEquals(10, mobFilesSet.size());


    scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setFilter(new ReferenceOnlyFilter());
    rs = table.getScanner(scan);
    TreeSet<String> mobFilesScannedAfterJob = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family), Bytes.toBytes(
          column));
      mobFilesScannedAfterJob.add(Bytes.toString(valueBytes, 8, valueBytes.length - 8));
    }

    assertEquals(10, mobFilesScannedAfterJob.size());

    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    assertEquals(10, mobFilesSet.size());
    assertEquals(true, mobFilesScannedAfterJob.iterator().next()
            .equalsIgnoreCase(mobFilesSet.iterator().next()));

  }

  @Test
  public void testCompactionDelaySweeper() throws Exception {

    int count = 10;
    //create table and generate 10 mob files
    generateMobTable(count, 1);

    //get mob files
    Path mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    FileStatus[] fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    // mobFileSet0 stores the orignal mob files
    TreeSet<String> mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    //scan the table, retreive the references
    Scan scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setFilter(new ReferenceOnlyFilter());
    ResultScanner rs = table.getScanner(scan);
    TreeSet<String> mobFilesScanned = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family),
              Bytes.toBytes(column));
      mobFilesScanned.add(Bytes.toString(valueBytes, 8, valueBytes.length - 8));
    }

    //there should be 10 mob files
    assertEquals(10, mobFilesScanned.size());
    //check if we store the correct reference of mob files
    assertEquals(mergeString(mobFilesSet), mergeString(mobFilesScanned));


    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(SweepJob.MOB_COMPACTION_DELAY, 0);

    String[] args = new String[2];
    args[0] = tableName;
    args[1] = family;
    ToolRunner.run(conf, new Sweeper(), args);


    mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    assertEquals(1, mobFilesSet.size());


    scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setFilter(new ReferenceOnlyFilter());
    rs = table.getScanner(scan);
    TreeSet<String> mobFilesScannedAfterJob = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family), Bytes.toBytes(
              column));
      mobFilesScannedAfterJob.add(Bytes.toString(valueBytes, 8, valueBytes.length - 8));
    }

    assertEquals(1, mobFilesScannedAfterJob.size());

    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    assertEquals(1, mobFilesSet.size());
    assertEquals(true, mobFilesScannedAfterJob.iterator().next()
            .equalsIgnoreCase(mobFilesSet.iterator().next()));

  }

}
