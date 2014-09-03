package org.apache.hadoop.hbase.mob.compactions;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestExpiredMobFileCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static TableName tableName = TableName.valueOf("TestExpiredMobFileCleaner");
  private final static String family = "family";
  private final static byte[] row1 = Bytes.toBytes("row1");
  private final static byte[] row2 = Bytes.toBytes("row2");
  private final static byte[] row3 = Bytes.toBytes("row3");
  private final static byte[] qf = Bytes.toBytes("qf");

  private static HTable table;
  private static HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {

  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getTestFileSystem().delete(TEST_UTIL.getDataTestDir(), true);
  }

  private void init(int cleanDelayDays) throws Exception {
    int cleanDelay = cleanDelayDays * secondsOfDay() * 1000;
    TEST_UTIL.getConfiguration().setLong(MobConstants.MOB_CLEAN_DELAY, cleanDelay);

    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, Bytes.toBytes(Boolean.TRUE));
    hcd.setValue(MobConstants.MOB_THRESHOLD, Bytes.toBytes(3L));
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);

    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    table.setAutoFlush(false, false);
  }

  private void modifyColumn(int expireDays) throws Exception {
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, Bytes.toBytes(Boolean.TRUE));
    hcd.setValue(MobConstants.MOB_THRESHOLD, Bytes.toBytes(3L));
    // change ttl as expire days to make some row expired
    int timeToLive = expireDays * secondsOfDay();
    hcd.setTimeToLive(timeToLive);

    admin.modifyColumn(tableName, hcd);
  }

  private void putKVAndFlush(HTable table, byte[] row, byte[] value, long ts)
      throws Exception {

    Put put = new Put(row, ts);
    put.add(Bytes.toBytes(family), qf, value);
    table.put(put);

    table.flushCommits();
    admin.flush(tableName.getName());
  }

  @Test
  public void testCleanerNoDelay() throws Exception {
    init(0); // no clean delay

    Path mobDirPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);

    byte[] dummyData = makeDummyData(600);
    long ts = System.currentTimeMillis() - 3 * secondsOfDay() * 1000; // 3 days before
    putKVAndFlush(table, row1, dummyData, ts);
    FileStatus[] firstFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    //the first mob file
    assertEquals("Before cleanup without delay 1", 1, firstFiles.length);
    String firstFile = firstFiles[0].getPath().getName();

    ts = System.currentTimeMillis() - 1 * secondsOfDay() * 1000; // 1 day before
    putKVAndFlush(table, row2, dummyData, ts);
    FileStatus[] secondFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    //now there are 2 mob files
    assertEquals("Before cleanup without delay 2", 2, secondFiles.length);
    String f1 = secondFiles[0].getPath().getName();
    String f2 = secondFiles[1].getPath().getName();
    String secondFile = f1.equals(firstFile) ? f2 : f1;

    modifyColumn(2); // ttl = 2, make the first row expired

    //run the cleaner
    String[] args = new String[2];
    args[0] = tableName.getNameAsString();
    args[1] = family;
    ToolRunner.run(TEST_UTIL.getConfiguration(), new ExpiredMobFileCleaner(), args);

    FileStatus[] filesAfterClean = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    String lastFile = filesAfterClean[0].getPath().getName();
    //the first mob fie is removed
    assertEquals("After cleanup without delay 1", 1, filesAfterClean.length);
    assertEquals("After cleanup without delay 2", secondFile, lastFile);
  }

  @Test
  public void testCleanerWithDelay() throws Exception {

    init(2); // 2 days clean delay

    Path mobDirPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);

    byte[] dummyData = makeDummyData(600);
    long ts = System.currentTimeMillis() - 4 * secondsOfDay() * 1000; // 4 days before
    putKVAndFlush(table, row1, dummyData, ts);
    FileStatus[] firstFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    assertEquals("Before cleanup with delay 1", 1, firstFiles.length);
    String mob1 = firstFiles[0].getPath().getName();

    ts = System.currentTimeMillis() - 2 * secondsOfDay() * 1000; // 2 days before
    putKVAndFlush(table, row2, dummyData, ts);
    putKVAndFlush(table, row3, dummyData, ts);
    FileStatus[] secondFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    assertEquals("Before cleanup with delay 2", 3, secondFiles.length);

    //set ttl = 1 day, so row2 and row3 won't be cleaned, row1 will be.
    modifyColumn(1);

    //run the cleaner
    String[] args = new String[2];
    args[0] = tableName.getNameAsString();
    args[1] = family;
    ToolRunner.run(TEST_UTIL.getConfiguration(), new ExpiredMobFileCleaner(), args);

    FileStatus[] thirdFiles = TEST_UTIL.getTestFileSystem().listStatus(mobDirPath);
    assertEquals("After cleanup with delay 1", 2, thirdFiles.length);

    Set<String> files3 = new HashSet<String>();
    for (int i = 0; i < thirdFiles.length; i++)
      files3.add(thirdFiles[i].getPath().getName());

    //only the first mob file is deleted due to the setting of  MobConstants.MOB_CLEAN_DELAY
    assertEquals("After cleanup with delay 2", false, files3.contains(mob1));
  }

  private Path getMobFamilyPath(Configuration conf, TableName tableName, String familyName) {
    Path p = new Path(MobUtils.getMobRegionPath(conf, tableName), familyName);
    return p;
  }

  private int secondsOfDay() {
    return 24 * 3600;
  }

  private byte[] makeDummyData(int size) {
    byte [] dummyData = new byte[size];
    new Random().nextBytes(dummyData);
    return dummyData;
  }
}
