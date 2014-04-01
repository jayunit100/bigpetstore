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

package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestDistributedFileSystem {
  private static final Random RAN = new Random();

  {
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  private boolean dualPortTesting = false;
  
  private Configuration getTestConfiguration() {
    Configuration conf = new Configuration();
    if (dualPortTesting) {
      conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_ADDRESS_KEY,
              "localhost:0");
    }
    return conf;
  }

  @Test
  public void testFileSystemCloseAll() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);
    URI address = FileSystem.getDefaultUri(conf);

    try {
      FileSystem.closeAll();

      conf = getTestConfiguration();
      FileSystem.setDefaultUri(conf, address);
      FileSystem.get(conf);
      FileSystem.get(conf);
      FileSystem.closeAll();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Tests DFSClient.close throws no ConcurrentModificationException if 
   * multiple files are open.
   */
  @Test
  public void testDFSClose() throws Exception {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fileSys = cluster.getFileSystem();

    try {
      // create two files
      fileSys.create(new Path("/test/dfsclose/file-0"));
      fileSys.create(new Path("/test/dfsclose/file-1"));

      fileSys.close();
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testDFSClient() throws Exception {
    Configuration conf = getTestConfiguration();
    final long grace = 1000L;
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final String filepathstring = "/test/LeaseChecker/foo";
      final Path[] filepaths = new Path[4];
      for(int i = 0; i < filepaths.length; i++) {
        filepaths[i] = new Path(filepathstring + i);
      }
      final long millis = System.currentTimeMillis();

      {
        DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
        dfs.dfs.getLeaseRenewer().setGraceSleepPeriod(grace);
        assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
  
        {
          //create a file
          final FSDataOutputStream out = dfs.create(filepaths[0]);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          //write something
          out.writeLong(millis);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          //close
          out.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          for(int i = 0; i < 3; i++) {
            if (dfs.dfs.getLeaseRenewer().isRunning()) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
        }

        {
          //create file1
          final FSDataOutputStream out1 = dfs.create(filepaths[1]);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          //create file2
          final FSDataOutputStream out2 = dfs.create(filepaths[2]);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());

          //write something to file1
          out1.writeLong(millis);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          //close file1
          out1.close();
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());

          //write something to file2
          out2.writeLong(millis);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          //close file2
          out2.close();
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
        }

        {
          //create file3
          final FSDataOutputStream out3 = dfs.create(filepaths[3]);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          Thread.sleep(grace/4*3);
          //passed previous grace period, should still running
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          //write something to file3
          out3.writeLong(millis);
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          //close file3
          out3.close();
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          Thread.sleep(grace/4*3);
          //within grace period
          assertTrue(dfs.dfs.getLeaseRenewer().isRunning());
          for(int i = 0; i < 3; i++) {
            if (dfs.dfs.getLeaseRenewer().isRunning()) {
              Thread.sleep(grace/2);
            }
          }
          //passed grace period
          assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
        }

        dfs.close();
      }

      {
        DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
        assertFalse(dfs.dfs.getLeaseRenewer().isRunning());

        //open and check the file
        FSDataInputStream in = dfs.open(filepaths[0]);
        assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
        assertEquals(millis, in.readLong());
        assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
        in.close();
        assertFalse(dfs.dfs.getLeaseRenewer().isRunning());
        dfs.close();
      }
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  @Test
  public void testStatistics() throws Exception {
    int lsLimit = 2;
    final Configuration conf = getTestConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, lsLimit);
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    try {
      final FileSystem fs = cluster.getFileSystem();
      Path dir = new Path("/test");
      Path file = new Path(dir, "file");
      
      int readOps = DFSTestUtil.getStatistics(fs).getReadOps();
      int writeOps = DFSTestUtil.getStatistics(fs).getWriteOps();
      int largeReadOps = DFSTestUtil.getStatistics(fs).getLargeReadOps();
      fs.mkdirs(dir);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      FSDataOutputStream out = fs.create(file, (short)1);
      out.close();
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      FileStatus status = fs.getFileStatus(file);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.getFileBlockLocations(status, 0, 0);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      FSDataInputStream in = fs.open(file);
      in.close();
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.setReplication(file, (short)2);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      Path file1 = new Path(dir, "file1");
      fs.rename(file, file1);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      fs.getContentSummary(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      
      // Iterative ls test
      for (int i = 0; i < 10; i++) {
        Path p = new Path(dir, Integer.toString(i));
        fs.mkdirs(p);
        FileStatus[] list = fs.listStatus(dir);
        if (list.length > lsLimit) {
          // if large directory, then count readOps and largeReadOps by 
          // number times listStatus iterates
          int iterations = (int)Math.ceil((double)list.length/lsLimit);
          largeReadOps += iterations;
          readOps += iterations;
        } else {
          // Single iteration in listStatus - no large read operation done
          readOps++;
        }
        
        // writeOps incremented by 1 for mkdirs
        // readOps and largeReadOps incremented by 1 or more
        checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      }
      
      fs.getFileChecksum(file1);
      checkStatistics(fs, ++readOps, writeOps, largeReadOps);
      
      fs.setPermission(file1, new FsPermission((short)0777));
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      fs.setTimes(file1, 0L, 0L);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      fs.setOwner(file1, ugi.getUserName(), ugi.getGroupNames()[0]);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
      fs.delete(dir, true);
      checkStatistics(fs, readOps, ++writeOps, largeReadOps);
      
    } finally {
      if (cluster != null) cluster.shutdown();
    }
    
  }
  
  /** Checks statistics. -1 indicates do not check for the operations */
  private void checkStatistics(FileSystem fs, int readOps, int writeOps, int largeReadOps) {
    assertEquals(readOps, DFSTestUtil.getStatistics(fs).getReadOps());
    assertEquals(writeOps, DFSTestUtil.getStatistics(fs).getWriteOps());
    assertEquals(largeReadOps, DFSTestUtil.getStatistics(fs).getLargeReadOps());
  }

  @Test
  public void testFileChecksum() throws Exception {
    ((Log4JLogger)HftpFileSystem.LOG).getLogger().setLevel(Level.ALL);

    final long seed = RAN.nextLong();
    System.out.println("seed=" + seed);
    RAN.setSeed(seed);

    final Configuration conf = getTestConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_WEBHDFS_ENABLED_KEY, true);
    conf.set("slave.host.name", "localhost");

    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    final FileSystem hdfs = cluster.getFileSystem();

    final String nnAddr = conf.get("dfs.http.address");
    final UserGroupInformation current = UserGroupInformation.getCurrentUser();
    final UserGroupInformation ugi = UserGroupInformation.createUserForTesting(
        current.getShortUserName() + "x", new String[]{"user"});

    try {
      ((DistributedFileSystem) hdfs).getFileChecksum(new Path(
          "/test/TestNonExistingFile"));
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("File does not exist: /test/TestNonExistingFile"));
    }

    try {
      Path path = new Path("/test/TestExistingDir/");
      hdfs.mkdirs(path);
      ((DistributedFileSystem) hdfs).getFileChecksum(path);
      fail("Expecting FileNotFoundException");
    } catch (FileNotFoundException e) {
      assertTrue("Not throwing the intended exception message", e.getMessage()
          .contains("File does not exist: /test/TestExistingDir"));
    }

    //hftp
    final String hftpuri = "hftp://" + nnAddr;
    System.out.println("hftpuri=" + hftpuri);
    final FileSystem hftp = ugi.doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return new Path(hftpuri).getFileSystem(conf);
      }
    });

    //webhdfs
    final String webhdfsuri = WebHdfsFileSystem.SCHEME  + "://" + nnAddr;
    System.out.println("webhdfsuri=" + webhdfsuri);
    final FileSystem webhdfs = ugi.doAs(
        new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return new Path(webhdfsuri).getFileSystem(conf);
      }
    });

    final Path dir = new Path("/filechecksum");
    final int block_size = 1024;
    final int buffer_size = conf.getInt("io.file.buffer.size", 4096);
    conf.setInt("io.bytes.per.checksum", 512);

    //try different number of blocks
    for(int n = 0; n < 5; n++) {
      //generate random data
      final byte[] data = new byte[RAN.nextInt(block_size/2-1)+n*block_size+1];
      RAN.nextBytes(data);
      System.out.println("data.length=" + data.length);
  
      //write data to a file
      final Path foo = new Path(dir, "foo" + n);
      {
        final FSDataOutputStream out = hdfs.create(foo, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
      
      //compute checksum
      final FileChecksum hdfsfoocs = hdfs.getFileChecksum(foo);
      System.out.println("hdfsfoocs=" + hdfsfoocs);

      //hftp
      final FileChecksum hftpfoocs = hftp.getFileChecksum(foo);
      System.out.println("hftpfoocs=" + hftpfoocs);

      final Path qualified = new Path(hftpuri + dir, "foo" + n);
      final FileChecksum qfoocs = hftp.getFileChecksum(qualified);
      System.out.println("qfoocs=" + qfoocs);

      //webhdfs
      final FileChecksum webhdfsfoocs = webhdfs.getFileChecksum(foo);
      System.out.println("webhdfsfoocs=" + webhdfsfoocs);

      final Path webhdfsqualified = new Path(webhdfsuri + dir, "foo" + n);
      final FileChecksum webhdfs_qfoocs = webhdfs.getFileChecksum(webhdfsqualified);
      System.out.println("webhdfs_qfoocs=" + webhdfs_qfoocs);
      
      //write another file
      final Path bar = new Path(dir, "bar" + n);
      {
        final FSDataOutputStream out = hdfs.create(bar, false, buffer_size,
            (short)2, block_size);
        out.write(data);
        out.close();
      }
  
      { //verify checksum
        final FileChecksum barcs = hdfs.getFileChecksum(bar);
        final int barhashcode = barcs.hashCode();
        assertEquals(hdfsfoocs.hashCode(), barhashcode);
        assertEquals(hdfsfoocs, barcs);

        //hftp
        assertEquals(hftpfoocs.hashCode(), barhashcode);
        assertEquals(hftpfoocs, barcs);

        assertEquals(qfoocs.hashCode(), barhashcode);
        assertEquals(qfoocs, barcs);

        //webhdfs
        assertEquals(webhdfsfoocs.hashCode(), barhashcode);
        assertEquals(webhdfsfoocs, barcs);

        assertEquals(webhdfs_qfoocs.hashCode(), barhashcode);
        assertEquals(webhdfs_qfoocs, barcs);
      }

      hdfs.setPermission(dir, new FsPermission((short)0));
      { //test permission error on hftp 
        try {
          hftp.getFileChecksum(qualified);
          fail();
        } catch(IOException ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }
      }

      { //test permission error on webhdfs 
        try {
          webhdfs.getFileChecksum(webhdfsqualified);
          fail();
        } catch(IOException ioe) {
          FileSystem.LOG.info("GOOD: getting an exception", ioe);
        }
      }
      hdfs.setPermission(dir, new FsPermission((short)0777));
    }
    cluster.shutdown();
  }
  
  @Test
  public void testAllWithDualPort() throws Exception {
    dualPortTesting = true;

    testFileSystemCloseAll();
    testDFSClose();
    testDFSClient();
    testFileChecksum();
  }

  @Test(timeout=60000)
  public void testFileCloseStatus() throws IOException {
    Configuration conf = getTestConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
    try {
      // create a new file.
      Path file = new Path("/simpleFlush.dat");
      FSDataOutputStream output = fs.create(file);
      // write to file
      output.writeBytes("Some test data");
      output.flush();
      assertFalse("File status should be open", fs.isFileClosed(file));
      output.close();
      assertTrue("File status should be closed", fs.isFileClosed(file));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
