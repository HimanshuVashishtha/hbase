/**
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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestHLogSwitch {
  private static final Log LOG = LogFactory.getLog(TestHLogSwitch.class);
  private static Configuration conf;
  private static FileSystem fs;
  private static Path dir;
  private static MiniDFSCluster cluster;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Path hbaseDir;

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // The below stuff is copied from TestHLog.
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // needed for testAppendClose()
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.broken.append", true);
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("heartbeat.recheck.interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.socket.timeout", 5000);
    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration().setInt("ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt("ipc.client.connection.maxidletime", 500);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
      SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);

    conf = TEST_UTIL.getConfiguration();
    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();

    hbaseDir = TEST_UTIL.createRootDir();
    dir = new Path(hbaseDir, getName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static String getName() {
    return "TestHLogSwitch";
  }

  /**
   * Tests the WAL Switch functionality correctness.
   * Write to a log file with twenty concurrent threads, with switching aggressively.
   * Verify that there is no "out-of-order" edits, and the data is in the expected range.
   * @throws Exception
   */
  @Test
  public void testConcurrentWritesWithSwitching() throws Exception {
    // Run the HPE tool with twenty threads writing 5000 edits each concurrently.
    // When done, verify that all edits were written.
    LOG.debug("testConcurrentWritesWithSwitching");
    Configuration conf1 = HBaseConfiguration.create(conf);
    conf1.setBoolean("hbase.regionserver.wal.switch.enabled", true);
    conf1.setLong("hbase.regionserver.wal.switch.threshold", 10);

    int errCode = HLogPerformanceEvaluation.innerMain(conf1, new String[] { "-threads", "20",
        "-verify", "-noclosefs", "-iterations", "5000" });
    assertEquals(0, errCode);
  }

  /**
   * Methods to test the switch functionality.
   * @throws IOException
   */
  @Test
  public void testSwitchOccurred() throws Exception {
    LOG.debug("testSwitchOccurred");
    Configuration conf1 = HBaseConfiguration.create(conf);
    conf1.setBoolean("hbase.regionserver.wal.switch.enabled", true);
    conf1.setClass("hbase.regionserver.hlog.writer.impl", SlowSyncOpWriter.class,
      HLog.Writer.class);
    TableName table1 = TableName.valueOf("t1");
    HLogFactory.resetLogWriterClass();
    HLog hlog = HLogFactory.createHLog(fs, FSUtils.getRootDir(conf1), dir.toString(), conf1);
    try {
      assertEquals(0, ((FSHLog) hlog).getNumRolledLogFiles());
      HRegionInfo hri1 = new HRegionInfo(table1, HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      // variables to mock region sequenceIds.
      final AtomicLong sequenceId1 = new AtomicLong(1);
      // start with the testing logic: insert a waledit, and roll writer
      int curNumLogFiles = hlog.getNumLogFiles();
      addEdits(hlog, hri1, table1, 1, sequenceId1);
      // invoke sync before calling roll...
      hlog.sync();
      addEdits(hlog, hri1, table1, 1, sequenceId1);
      assertTrue("Old log count is same as current log count",
        hlog.getNumLogFiles() > curNumLogFiles);
      curNumLogFiles = hlog.getNumLogFiles();
      hlog.sync();
      // ensure empty sync also works
      hlog.sync();
      assertTrue("Old log count is same as current log count",
        hlog.getNumLogFiles() > curNumLogFiles);
      LOG.warn("Before calling the roll writer.. switch");
      // test concurrent log roll.
      hlog.rollWriter();
      // assert that the wal is rolled
      // add edits in the second wal file, and roll writer.
      addEdits(hlog, hri1, table1, 1, sequenceId1);
      hlog.sync();
      hlog.rollWriter();
    } finally {
      HLogFactory.resetLogWriterClass();
      if (hlog != null) hlog.close();
    }
  }

  private void addEdits(HLog log, HRegionInfo hri, TableName tableName, int times,
      AtomicLong sequenceId) throws IOException {
    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor("row"));

    final byte[] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      log.append(hri, tableName, cols, timestamp, htd, sequenceId);
    }
  }

}
