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

package org.apache.hadoop.ipc;

import org.apache.commons.logging.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.net.NetUtils;

import java.util.Random;
import java.io.DataInput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;

import javax.net.SocketFactory;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import static org.mockito.Mockito.*;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Unit tests for IPC. */
public class TestIPC extends TestCase {
  public static final Log LOG =
    LogFactory.getLog(TestIPC.class);
  
  final private static Configuration conf = new Configuration();
  final static private int PING_INTERVAL = 1000;
  final static private int MIN_SLEEP_TIME = 1000;
 
  static {
    Client.setPingInterval(conf, PING_INTERVAL);
  }
  public TestIPC(String name) { super(name); }

  private static final Random RANDOM = new Random();

  private static final String ADDRESS = "0.0.0.0";

  private static class TestServer extends Server {
    private boolean sleep;

    public TestServer(int handlerCount, boolean sleep) 
      throws IOException {
      super(ADDRESS, 0, LongWritable.class, handlerCount, conf);
      this.sleep = sleep;
    }

    @Override
    public Writable call(Class<?> protocol, Writable param, long receiveTime)
        throws IOException {
      if (sleep) {
        // sleep a bit
        try {
          Thread.sleep(RANDOM.nextInt(PING_INTERVAL) + MIN_SLEEP_TIME);
        } catch (InterruptedException e) {}
      }
      return param;                               // echo param as result
    }
  }

  private static class SerialCaller extends Thread {
    private Client client;
    private InetSocketAddress server;
    private int count;
    private boolean failed;

    public SerialCaller(Client client, InetSocketAddress server, int count) {
      this.client = client;
      this.server = server;
      this.count = count;
    }

    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          LongWritable param = new LongWritable(RANDOM.nextLong());
          LongWritable value =
            (LongWritable)client.call(param, server, null, null, 0, conf);
          if (!param.equals(value)) {
            LOG.fatal("Call failed!");
            failed = true;
            break;
          }
        } catch (Exception e) {
          LOG.fatal("Caught: " + StringUtils.stringifyException(e));
          failed = true;
        }
      }
    }
  }

  private static class ParallelCaller extends Thread {
    private Client client;
    private int count;
    private InetSocketAddress[] addresses;
    private boolean failed;
    
    public ParallelCaller(Client client, InetSocketAddress[] addresses,
                          int count) {
      this.client = client;
      this.addresses = addresses;
      this.count = count;
    }

    public void run() {
      for (int i = 0; i < count; i++) {
        try {
          Writable[] params = new Writable[addresses.length];
          for (int j = 0; j < addresses.length; j++)
            params[j] = new LongWritable(RANDOM.nextLong());
          Writable[] values = client.call(params, addresses, null, null, conf);
          for (int j = 0; j < addresses.length; j++) {
            if (!params[j].equals(values[j])) {
              LOG.fatal("Call failed!");
              failed = true;
              break;
            }
          }
        } catch (Exception e) {
          LOG.fatal("Caught: " + StringUtils.stringifyException(e));
          failed = true;
        }
      }
    }
  }

  public void testSerial() throws Exception {
    testSerial(3, false, 2, 5, 10);
  }

  public void testSerial(int handlerCount, boolean handlerSleep, 
                         int clientCount, int callerCount, int callCount)
    throws Exception {
    Server server = new TestServer(handlerCount, handlerSleep);
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    server.start();

    Client[] clients = new Client[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new Client(LongWritable.class, conf);
    }
    
    SerialCaller[] callers = new SerialCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] = new SerialCaller(clients[i%clientCount], addr, callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      assertFalse(callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    server.stop();
  }
	
  public void testParallel() throws Exception {
    testParallel(10, false, 2, 4, 2, 4, 100);
  }

  public void testParallel(int handlerCount, boolean handlerSleep,
                           int serverCount, int addressCount,
                           int clientCount, int callerCount, int callCount)
    throws Exception {
    Server[] servers = new Server[serverCount];
    for (int i = 0; i < serverCount; i++) {
      servers[i] = new TestServer(handlerCount, handlerSleep);
      servers[i].start();
    }

    InetSocketAddress[] addresses = new InetSocketAddress[addressCount];
    for (int i = 0; i < addressCount; i++) {
      addresses[i] = NetUtils.getConnectAddress(servers[i%serverCount]);
    }

    Client[] clients = new Client[clientCount];
    for (int i = 0; i < clientCount; i++) {
      clients[i] = new Client(LongWritable.class, conf);
    }
    
    ParallelCaller[] callers = new ParallelCaller[callerCount];
    for (int i = 0; i < callerCount; i++) {
      callers[i] =
        new ParallelCaller(clients[i%clientCount], addresses, callCount);
      callers[i].start();
    }
    for (int i = 0; i < callerCount; i++) {
      callers[i].join();
      assertFalse(callers[i].failed);
    }
    for (int i = 0; i < clientCount; i++) {
      clients[i].stop();
    }
    for (int i = 0; i < serverCount; i++) {
      servers[i].stop();
    }
  }
	
  public void testStandAloneClient() throws Exception {
    testParallel(10, false, 2, 4, 2, 4, 100);
    Client client = new Client(LongWritable.class, conf);
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", 10);
    try {
      client.call(new LongWritable(RANDOM.nextLong()),
              address, null, null, 0, conf);
      fail("Expected an exception to have been thrown");
    } catch (IOException e) {
      String message = e.getMessage();
      String addressText = address.toString();
      assertTrue("Did not find "+addressText+" in "+message,
              message.contains(addressText));
      Throwable cause=e.getCause();
      assertNotNull("No nested exception in "+e,cause);
      String causeText=cause.getMessage();
      assertTrue("Did not find " + causeText + " in " + message,
              message.contains(causeText));
    }
  }

  /**
  * Test that, if a RuntimeException is thrown after creating a socket
  * but before successfully connecting to the IPC server, that the
  * failure is handled properly. This is a regression test for
  * HADOOP-7428 (HADOOP-8294).
  */
  public void testRTEDuringConnectionSetup() throws Exception {
    // Set up a socket factory which returns sockets which
    // throw an RTE when setSoTimeout is called.
    SocketFactory spyFactory = spy(NetUtils.getDefaultSocketFactory(conf));
    Mockito.doAnswer(new Answer<Socket>() {
      @Override
      public Socket answer(InvocationOnMock invocation) throws Throwable {
        Socket s = spy((Socket)invocation.callRealMethod());
        doThrow(new RuntimeException("Injected fault")).when(s)
          .setSoTimeout(anyInt());
        return s;
      }
    }).when(spyFactory).createSocket();
 
    Server server = new TestServer(1, true);
    server.start();
    try {
      // Call should fail due to injected exception.
      InetSocketAddress address = NetUtils.getConnectAddress(server);
      Client client = new Client(LongWritable.class, conf, spyFactory);
      try {
        client.call(new LongWritable(RANDOM.nextLong()),
                address, null, null, 0, conf);
        fail("Expected an exception to have been thrown");
      } catch (Exception e) {
        LOG.info("caught expected exception", e);
        assertTrue(StringUtils.stringifyException(e).contains(
            "Injected fault"));
      }
      // Resetting to the normal socket behavior should succeed
      // (i.e. it should not have cached a half-constructed connection)
  
      Mockito.reset(spyFactory);
      client.call(new LongWritable(RANDOM.nextLong()),
          address, null, null, 0, conf);
    } finally {
      server.stop();
    }
  }

  public void testIpcTimeout() throws Exception {
    // start server
    Server server = new TestServer(1, true);
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    server.start();

    // start client
    Client client = new Client(LongWritable.class, conf);
    // set timeout to be less than MIN_SLEEP_TIME
    try {
      client.call(new LongWritable(RANDOM.nextLong()),
              addr, null, null, MIN_SLEEP_TIME/2);
      fail("Expected an exception to have been thrown");
    } catch (SocketTimeoutException e) {
     LOG.info("Get a SocketTimeoutException ", e);
    }
    // set timeout to be bigger than 3*ping interval
    client.call(new LongWritable(RANDOM.nextLong()),
        addr, null, null, 3*PING_INTERVAL+MIN_SLEEP_TIME);
  }

  private static class LongErrorWritable extends LongWritable {
    private final static String ERR_MSG =
      "Come across an exception while reading";

    LongErrorWritable() {}

    LongErrorWritable(long longValue) {
      super(longValue);
    }

    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      throw new IOException(ERR_MSG);
    }
  }

  public void testErrorClient() throws Exception {
    // start server
    Server server = new TestServer(1, false);
    InetSocketAddress addr = NetUtils.getConnectAddress(server);
    server.start();

    // start client
    Client client = new Client(LongErrorWritable.class, conf);
    try {
      client.call(new LongErrorWritable(RANDOM.nextLong()),
          addr, null, null, 0, conf);
      fail("Expected an exception to have been thrown");
    } catch (IOException e) {
      // check error
      Throwable cause = e.getCause();
      assertTrue(cause instanceof IOException);
      assertEquals(LongErrorWritable.ERR_MSG, cause.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {

    //new TestIPC("test").testSerial(5, false, 2, 10, 1000);

    new TestIPC("test").testParallel(10, false, 2, 4, 2, 4, 1000);

  }

}
