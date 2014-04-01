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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERSIST_BLOCKS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_PERSIST_BLOCKS_KEY;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicatedBlocks.BlockIterator;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.KeyUpdateCommand;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.QueueProcessingStatistics;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

/***************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 *
 * It tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
public class FSNamesystem implements FSConstants, FSNamesystemMBean, FSClusterStats, 
    NameNodeMXBean, MetricsSource {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);
  public static final String AUDIT_FORMAT =
    "ugi=%s\t" +  // ugi
    "ip=%s\t" +   // remote IP
    "cmd=%s\t" +  // command
    "src=%s\t" +  // src path
    "dst=%s\t" +  // dst path (optional)
    "perm=%s";    // permissions (optional)

  private static final ThreadLocal<Formatter> auditFormatter =
    new ThreadLocal<Formatter>() {
      protected Formatter initialValue() {
        return new Formatter(new StringBuilder(AUDIT_FORMAT.length() * 4));
      }
  };

  private static final void logAuditEvent(UserGroupInformation ugi,
      InetAddress addr, String cmd, String src, String dst,
      HdfsFileStatus stat) {
    final Formatter fmt = auditFormatter.get();
    ((StringBuilder)fmt.out()).setLength(0);
    auditLog.info(fmt.format(AUDIT_FORMAT, ugi, addr, cmd, src, dst,
                  (stat == null)
                    ? null
                    : stat.getOwner() + ':' + stat.getGroup() + ':' +
                      stat.getPermission()
          ).toString());

  }

  public static final Log auditLog = LogFactory.getLog(
      FSNamesystem.class.getName() + ".audit");

  // Default initial capacity and load factor of map
  public static final int DEFAULT_INITIAL_MAP_CAPACITY = 16;
  public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;
  static int BLOCK_DELETION_INCREMENT = 1000;
  
  private float blocksInvalidateWorkPct;
  private int blocksReplWorkMultiplier;

  private boolean isPermissionEnabled;
  private boolean persistBlocks;
  private UserGroupInformation fsOwner;
  private String fsOwnerShortUserName;
  private String supergroup;
  private PermissionStatus defaultPermission;
  // FSNamesystemMetrics counter variables
  private long capacityTotal = 0L, capacityUsed = 0L, capacityRemaining = 0L;
  private int totalLoad = 0;
  boolean isAccessTokenEnabled;
  BlockTokenSecretManager accessTokenHandler;
  private long accessKeyUpdateInterval;
  private long accessTokenLifetime;
  
  // Scan interval is not configurable.
  private static final long DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL =
    TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
  private DelegationTokenSecretManager dtSecretManager;

  volatile long pendingReplicationBlocksCount = 0L;
  volatile long corruptReplicaBlocksCount = 0L;
  volatile long underReplicatedBlocksCount = 0L;
  volatile long scheduledReplicationBlocksCount = 0L;
  volatile long excessBlocksCount = 0L;
  volatile long pendingDeletionBlocksCount = 0L;
  //
  // Stores the correct file name hierarchy
  //
  public FSDirectory dir;

  //
  // Mapping: Block -> { INode, datanodes, self ref } 
  // Updated only in response to client-sent information.
  //
  final BlocksMap blocksMap = new BlocksMap(DEFAULT_INITIAL_MAP_CAPACITY, 
                                            DEFAULT_MAP_LOAD_FACTOR);

  //
  // Store blocks-->datanodedescriptor(s) map of corrupt replicas
  //
  public CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();
    
  /**
   * Stores the datanode -> block map.  
   * <p>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by 
   * storage id. In order to keep the storage map consistent it tracks 
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul> 
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one 
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br>
   * The list of the {@link DatanodeDescriptor}s in the map is checkpointed
   * in the namespace image file. Only the {@link DatanodeInfo} part is 
   * persistent, the list of blocks is restored from the datanode block
   * reports. 
   * <p>
   * Mapping: StorageID -> DatanodeDescriptor
   */
  NavigableMap<String, DatanodeDescriptor> datanodeMap = 
    new TreeMap<String, DatanodeDescriptor>();

  //
  // Keeps a Collection for every named machine containing
  // blocks that have recently been invalidated and are thought to live
  // on the machine in question.
  // Mapping: StorageID -> ArrayList<Block>
  //
  private Map<String, Collection<Block>> recentInvalidateSets = 
    new TreeMap<String, Collection<Block>>();

  //
  // Keeps a TreeSet for every named node.  Each treeset contains
  // a list of the blocks that are "extra" at that location.  We'll
  // eventually remove these extras.
  // Mapping: StorageID -> TreeSet<Block>
  //
  Map<String, Collection<Block>> excessReplicateMap = 
    new TreeMap<String, Collection<Block>>();

  Random r = new Random();

  /**
   * Stores a set of DatanodeDescriptor objects.
   * This is a subset of {@link #datanodeMap}, containing nodes that are 
   * considered alive.
   * The {@link HeartbeatMonitor} periodically checks for outdated entries,
   * and removes them from the list.
   */
  ArrayList<DatanodeDescriptor> heartbeats = new ArrayList<DatanodeDescriptor>();

/**
 * Store set of Blocks that need to be replicated 1 or more times.
 * Set of: Block
 */
  private UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
  // We also store pending replication-orders.
  PendingReplicationBlocks pendingReplications;

  public LeaseManager leaseManager = new LeaseManager(this); 

  //
  // Threaded object that checks to see if we have been
  // getting heartbeats from all clients. 
  //
  Daemon hbthread = null;   // HeartbeatMonitor thread
  public Daemon lmthread = null;   // LeaseMonitor thread
  Daemon smmthread = null;  // SafeModeMonitor thread
  public Daemon replthread = null;  // Replication thread
  private ReplicationMonitor replmon = null; // Replication metrics
  
  private volatile boolean fsRunning = true;
  long systemStart = 0;

  //  The maximum number of replicates we should allow for a single block
  private int maxReplication;
  //  How many outgoing replication streams a given node should have at one time
  private int maxReplicationStreams;
  // MIN_REPLICATION is how many copies we need in place or else we disallow the write
  private int minReplication;
  // Default replication
  private int defaultReplication;
  // Variable to stall new replication checks for testing purposes
  private volatile boolean stallReplicationWork = false;
  // heartbeatRecheckInterval is how often namenode checks for expired datanodes
  private long heartbeatRecheckInterval;
  // heartbeatExpireInterval is how long namenode waits for datanode to report
  // heartbeat
  private long heartbeatExpireInterval;
  //replicationRecheckInterval is how often namenode checks for new replication work
  private long replicationRecheckInterval;
  // default block size of a file
  private long defaultBlockSize = 0;
  // allow file appending (for test coverage)
  private boolean allowBrokenAppend = false;
  // enable durable sync
  private boolean durableSync = true;
  // How many entries are returned by getCorruptInodes()
  int maxCorruptFilesReturned;

  /**
   * Last block index used for replication work.
   */
  private int replIndex = 0;
  private long missingBlocksInCurIter = 0;
  private long missingBlocksInPrevIter = 0; 

  public static FSNamesystem fsNamesystemObject;
  /** NameNode RPC address */
  private InetSocketAddress nameNodeAddress = null; // TODO: name-node has this field, it should be removed here
  private SafeModeInfo safeMode;  // safe mode information
  private Host2NodesMap host2DataNodeMap = new Host2NodesMap();
    
  // datanode networktoplogy
  NetworkTopology clusterMap;
  private DNSToSwitchMapping dnsToSwitchMapping;
  
  // for block replicas placement
  BlockPlacementPolicy replicator;

  private HostsFileReader hostsReader; 
  private Daemon dnthread = null;

  private long maxFsObjects = 0;          // maximum number of fs objects

  /**
   * The global generation stamp for this file system. 
   */
  private final GenerationStamp generationStamp = new GenerationStamp();

  // Ask Datanode only up to this many blocks to delete.
  int blockInvalidateLimit = DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT;

  // precision of access times.
  private long accessTimePrecision = 0;
  private String nameNodeHostName;
  /** The interval for judging stale DataNodes for read/write */
  private long staleInterval;
  /** Whether or not to avoid using stale DataNodes for reading */
  private boolean avoidStaleDataNodesForRead;

  /**
   * Whether or not to avoid using stale DataNodes for writing.
   * Note that, even if this is configured, the policy may be
   * temporarily disabled when a high percentage of the nodes
   * are marked as stale.
   */
  private boolean avoidStaleDataNodesForWrite;

  /**
   * When the ratio of stale datanodes reaches this number, stop avoiding
   * writing to stale datanodes, i.e., continue using stale nodes for writing.
   */
  private float ratioUseStaleDataNodesForWrite;
  /** The number of stale DataNodes */
  private volatile int numStaleNodes = 0;

  /**
   * FSNamesystem constructor.
   */
  FSNamesystem(NameNode nn, Configuration conf) throws IOException {
    try {
      initialize(nn, conf);
    } catch (IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      shutdown();
      throw e;
    } catch (RuntimeException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      shutdown();
      throw e;
    }
  }

  void activateSecretManager() throws IOException {
    if (dtSecretManager != null) {
      dtSecretManager.startThreads();
    }
  }

  /**
   * Initialize FSNamesystem.
   */
  private void initialize(NameNode nn, Configuration conf) throws IOException {
    this.systemStart = now();
    setConfigurationParameters(conf);
    dtSecretManager = createDelegationTokenSecretManager(conf);

    this.nameNodeAddress = nn.getNameNodeAddress();
    this.registerMBean(conf); // register the MBean for the FSNamesystemStutus
    this.dir = new FSDirectory(this, conf);
    StartupOption startOpt = NameNode.getStartupOption(conf);
    this.dir.loadFSImage(getNamespaceDirs(conf),
                         getNamespaceEditsDirs(conf), startOpt);
    long timeTakenToLoadFSImage = now() - systemStart;
    LOG.info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
    NameNode.getNameNodeMetrics().setFsImageLoadTime(timeTakenToLoadFSImage);
    this.safeMode = new SafeModeInfo(conf);
    setBlockTotal();
    pendingReplications = new PendingReplicationBlocks(
                            conf.getInt("dfs.replication.pending.timeout.sec", 
                                        -1) * 1000L);
    if (isAccessTokenEnabled) {
      accessTokenHandler = new BlockTokenSecretManager(true,
          accessKeyUpdateInterval, accessTokenLifetime);
    }
    this.hbthread = new Daemon(new HeartbeatMonitor());
    this.lmthread = new Daemon(leaseManager.new Monitor());
    this.replmon = new ReplicationMonitor();
    this.replthread = new Daemon(replmon);
    hbthread.start();
    lmthread.start();
    replthread.start();

    this.hostsReader = new HostsFileReader(conf.get("dfs.hosts",""),
                                           conf.get("dfs.hosts.exclude",""));
    this.dnthread = new Daemon(new DecommissionManager(this).new Monitor(
        conf.getInt("dfs.namenode.decommission.interval", 30),
        conf.getInt("dfs.namenode.decommission.nodes.per.interval", 5)));
    dnthread.start();

    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);
    
    /* If the dns to swith mapping supports cache, resolve network 
     * locations of those hosts in the include list, 
     * and store the mapping in the cache; so future calls to resolve
     * will be fast.
     */
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<String>(hostsReader.getHosts()));
    }
    
    InetSocketAddress socAddr = NameNode.getAddress(conf);
    this.nameNodeHostName = socAddr.getHostName();
    
    registerWith(DefaultMetricsSystem.INSTANCE);
  }

  public static Collection<File> getNamespaceDirs(Configuration conf) {
    Collection<String> dirNames = conf.getStringCollection("dfs.name.dir");
    if (dirNames.isEmpty())
      dirNames.add("/tmp/hadoop/dfs/name");
    Collection<File> dirs = new ArrayList<File>(dirNames.size());
    for(String name : dirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }
  
  public static Collection<File> getNamespaceEditsDirs(Configuration conf) {
    Collection<String> editsDirNames = 
            conf.getStringCollection("dfs.name.edits.dir");
    if (editsDirNames.isEmpty())
      editsDirNames.add("/tmp/hadoop/dfs/name");
    Collection<File> dirs = new ArrayList<File>(editsDirNames.size());
    for(String name : editsDirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }

  /**
   * dirs is a list of directories where the filesystem directory state 
   * is stored
   */
  FSNamesystem(FSImage fsImage, Configuration conf) throws IOException {
    setConfigurationParameters(conf);
    this.dir = new FSDirectory(fsImage, this, conf);
    dtSecretManager = createDelegationTokenSecretManager(conf);
  }

  /**
   * Initializes some of the members from configuration
   */
  private void setConfigurationParameters(Configuration conf) 
                                          throws IOException {
    fsNamesystemObject = this;
    fsOwner = UserGroupInformation.getCurrentUser();
    this.fsOwnerShortUserName = fsOwner.getShortUserName();
    LOG.info("fsOwner=" + fsOwner);

    this.supergroup = conf.get("dfs.permissions.supergroup", "supergroup");
    this.isPermissionEnabled = conf.getBoolean("dfs.permissions", true);
    LOG.info("supergroup=" + supergroup);
    LOG.info("isPermissionEnabled=" + isPermissionEnabled);
    
    this.persistBlocks = conf.getBoolean(DFS_PERSIST_BLOCKS_KEY,
                                                  DFS_PERSIST_BLOCKS_DEFAULT);
    
    short filePermission = (short)conf.getInt("dfs.upgrade.permission", 0777);
    this.defaultPermission = PermissionStatus.createImmutable(
        fsOwnerShortUserName, supergroup, new FsPermission(filePermission));

    this.blocksInvalidateWorkPct = 
              DFSUtil.getInvalidateWorkPctPerIteration(conf);

    this.blocksReplWorkMultiplier = DFSUtil.getReplWorkMultiplier(conf);
    this.clusterMap = NetworkTopology.getInstance(conf);

    this.maxCorruptFilesReturned = conf.getInt(
        DFSConfigKeys.DFS_MAX_CORRUPT_FILES_RETURNED_KEY,
        DFSConfigKeys.DFS_MAX_CORRUPT_FILES_RETURNED_DEFAULT);
    this.replicator = BlockPlacementPolicy.getInstance(conf, this, clusterMap);
    this.defaultReplication = conf.getInt("dfs.replication", 3);
    this.maxReplication = conf.getInt("dfs.replication.max", 512);
    this.minReplication = conf.getInt("dfs.replication.min", 1);
    if (minReplication <= 0)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.min = " 
                            + minReplication
                            + " must be greater than 0");
    if (maxReplication >= (int)Short.MAX_VALUE)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.max = " 
                            + maxReplication + " must be less than " + (Short.MAX_VALUE));
    if (maxReplication < minReplication)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.min = " 
                            + minReplication
                            + " must be less than dfs.replication.max = " 
                            + maxReplication);
    this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
    this.heartbeatRecheckInterval = conf.getInt(
        "heartbeat.recheck.interval", 5 * 60 * 1000); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval +
      10 * heartbeatInterval;
    this.replicationRecheckInterval = 
      conf.getInt("dfs.replication.interval", 3) * 1000L;
    this.defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.maxFsObjects = conf.getLong("dfs.max.objects", 0);

    //default limit
    this.blockInvalidateLimit = Math.max(this.blockInvalidateLimit, 
                                         20*(int)(heartbeatInterval/1000));
    //use conf value if it is set.
    this.blockInvalidateLimit = conf.getInt(
        DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY, this.blockInvalidateLimit);
    LOG.info(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY + "=" + this.blockInvalidateLimit);

    this.accessTimePrecision = conf.getLong("dfs.access.time.precision", 0);
    this.allowBrokenAppend = conf.getBoolean("dfs.support.broken.append", false);
    if (conf.getBoolean("dfs.support.append", false)) {
      LOG.warn("The dfs.support.append option is in your configuration, " +
               "however append is not supported. This configuration option " +
               "is no longer required to enable sync");
    }
    this.durableSync = conf.getBoolean("dfs.durable.sync", true);
    if (!durableSync) {
      LOG.warn("Durable sync disabled. Beware data loss when running " +
               "programs like HBase that require durable sync!");
    }
    this.isAccessTokenEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);
    if (isAccessTokenEnabled) {
      this.accessKeyUpdateInterval = conf.getLong(
          DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 600) * 60 * 1000L; // 10 hrs
      this.accessTokenLifetime = conf.getLong(
          DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 600) * 60 * 1000L; // 10 hrs
    }
    LOG.info("isAccessTokenEnabled=" + isAccessTokenEnabled
        + " accessKeyUpdateInterval=" + accessKeyUpdateInterval / (60 * 1000)
        + " min(s), accessTokenLifetime=" + accessTokenLifetime / (60 * 1000)
        + " min(s)");
    
    // set the value of stale interval based on configuration
    this.avoidStaleDataNodesForRead = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_READ_DEFAULT);
    this.avoidStaleDataNodesForWrite = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT);
    this.staleInterval = getStaleIntervalFromConf(conf, heartbeatExpireInterval);
    this.ratioUseStaleDataNodesForWrite = 
        getRatioUseStaleNodesForWriteFromConf(conf);

    if (avoidStaleDataNodesForWrite && staleInterval < heartbeatRecheckInterval) {
      this.heartbeatRecheckInterval = staleInterval;
      LOG.info("Setting heartbeat recheck interval to " + staleInterval
          + " since " + DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY
          + " is less than "
          + DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY);
    }
  }
  
  private static float getRatioUseStaleNodesForWriteFromConf(Configuration conf) {
    float ratioUseStaleDataNodesForWrite = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY,
        DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_DEFAULT);
    if (ratioUseStaleDataNodesForWrite > 0
        && ratioUseStaleDataNodesForWrite <= 1.0f) {
      return ratioUseStaleDataNodesForWrite;
    } else {
      throw new IllegalArgumentException(
          DFSConfigKeys.DFS_NAMENODE_USE_STALE_DATANODE_FOR_WRITE_RATIO_KEY
              + " = '" + ratioUseStaleDataNodesForWrite
              + "' is invalid. It should be a positive non-zero float value,"
              + " not greater than 1.0f");
    }
  }
  
  private static long getStaleIntervalFromConf(Configuration conf,
      long heartbeatExpireInterval) {
    long staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    if (staleInterval <= 0) {
      throw new IllegalArgumentException(
          DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY + " = '"
              + staleInterval
              + "' is invalid. It should be a positive non-zero value");
    }
    final long heartbeatIntervalSeconds = conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT);
    // The stale interval value cannot be smaller than
    // 3 times of heartbeat interval
    final long minStaleInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT)
        * heartbeatIntervalSeconds * 1000;
    if (staleInterval < minStaleInterval) {
      LOG.warn("The given interval for marking stale datanode = "
          + staleInterval + ", which is less than "
          + DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_MINIMUM_INTERVAL_DEFAULT
          + " heartbeat intervals. This may cause too frequent changes of "
          + "stale states of DataNodes since a heartbeat msg may be missing "
          + "due to temporary short-term failures. Reset stale interval to "
          + minStaleInterval);
      staleInterval = minStaleInterval;
    }
    if (staleInterval > heartbeatExpireInterval) {
      LOG.warn("The given interval for marking stale datanode = "
          + staleInterval + ", which is larger than heartbeat expire interval "
          + heartbeatExpireInterval);
    }
    return staleInterval;
  }

  /**
   * Return the default path permission when upgrading from releases with no
   * permissions (<=0.15) to releases with permissions (>=0.16)
   */
  protected PermissionStatus getUpgradePermission() {
    return defaultPermission;
  }
  
  /** Return the FSNamesystem object
   * 
   */
  public static FSNamesystem getFSNamesystem() {
    return fsNamesystemObject;
  } 

  synchronized NamespaceInfo getNamespaceInfo() {
    return new NamespaceInfo(dir.fsImage.getNamespaceID(),
                             dir.fsImage.getCTime(),
                             getDistributedUpgradeVersion());
  }

  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  public void close() {
    fsRunning = false;
    try {
      if (pendingReplications != null) pendingReplications.stop();
      if (hbthread != null) hbthread.interrupt();
      if (replthread != null) replthread.interrupt();
      if (dnthread != null) dnthread.interrupt();
      if (smmthread != null) smmthread.interrupt();
      if (dtSecretManager != null) dtSecretManager.stopThreads();
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
        dir.close();
        blocksMap.close();
      } catch (InterruptedException ie) {
      } catch (IOException ie) {
        LOG.error("Error closing FSDirectory", ie);
        IOUtils.cleanup(LOG, dir);
      }
    }
  }

  /** Is this name system running? */
  boolean isRunning() {
    return fsRunning;
  }

  /**
   * Dump all metadata into specified file
   */
  void metaSave(String filename) throws IOException {
    checkSuperuserPrivilege();
    synchronized (this) {
      File file = new File(System.getProperty("hadoop.log.dir"), filename);
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file,
          true)));

      long totalInodes = this.dir.totalInodes();
      long totalBlocks = this.getBlocksTotal();

      ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      this.DFSNodesStatus(live, dead);

      String str = totalInodes + " files and directories, " + totalBlocks
          + " blocks = " + (totalInodes + totalBlocks) + " total";
      out.println(str);
      out.println("Live Datanodes: " + live.size());
      out.println("Dead Datanodes: " + dead.size());

      //
      // Dump contents of neededReplication
      //
      synchronized (neededReplications) {
        out.println("Metasave: Blocks waiting for replication: "
            + neededReplications.size());
        for (Block block : neededReplications) {
          List<DatanodeDescriptor> containingNodes = new ArrayList<DatanodeDescriptor>();
          NumberReplicas numReplicas = new NumberReplicas();
          // source node returned is not used
          chooseSourceDatanode(block, containingNodes, numReplicas);
          int usableReplicas = numReplicas.liveReplicas()
              + numReplicas.decommissionedReplicas();

          if (block instanceof BlockInfo) {
            String fileName = FSDirectory.getFullPathName(((BlockInfo) block)
                .getINode());
            out.print(fileName + ": ");
          }

          // l: == live:, d: == decommissioned c: == corrupt e: == excess
          out.print(block + ((usableReplicas > 0) ? "" : " MISSING")
              + " (replicas:" + " l: " + numReplicas.liveReplicas() + " d: "
              + numReplicas.decommissionedReplicas() + " c: "
              + numReplicas.corruptReplicas() + " e: "
              + numReplicas.excessReplicas() + ") ");

          Collection<DatanodeDescriptor> corruptNodes = corruptReplicas
              .getNodes(block);

          for (Iterator<DatanodeDescriptor> jt = blocksMap.nodeIterator(block); jt
              .hasNext();) {
            DatanodeDescriptor node = jt.next();
            String state = "";
            if (corruptNodes != null && corruptNodes.contains(node)) {
              state = "(corrupt)";
            } else if (node.isDecommissioned()
                || node.isDecommissionInProgress()) {
              state = "(decommissioned)";
            }
            out.print(" " + node + state + " : ");
          }
          out.println("");
        }
      }

      //
      // Dump blocks from pendingReplication
      //
      pendingReplications.metaSave(out);

      //
      // Dump blocks that are waiting to be deleted
      //
      dumpRecentInvalidateSets(out);

      //
      // Dump all datanodes
      //
      datanodeDump(out);

      out.flush();
      out.close();
    }
  }

  /**
   * Tell all datanodes to use a new, non-persistent bandwidth value for
   * dfs.balance.bandwidthPerSec.
   *
   * A system administrator can tune the balancer bandwidth parameter
   * (dfs.balance.bandwidthPerSec) dynamically by calling
   * "dfsadmin -setBalanacerBandwidth newbandwidth", at which point the
   * following 'bandwidth' variable gets updated with the new value for each
   * node. Once the heartbeat command is issued to update the value on the
   * specified datanode, this value will be set back to 0.
   *
   * @param bandwidth Blanacer bandwidth in bytes per second for all datanodes.
   * @throws IOException
   */
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    checkSuperuserPrivilege();
    synchronized(datanodeMap) {
      for (DatanodeDescriptor nodeInfo : datanodeMap.values()) {
        nodeInfo.setBalancerBandwidth(bandwidth);
      }
    }
  }

  long getDefaultBlockSize() {
    return defaultBlockSize;
  }

  long getAccessTimePrecision() {
    return accessTimePrecision;
  }

  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }
    
  /* get replication factor of a block */
  private int getReplication(Block block) {
    INodeFile fileINode = blocksMap.getINode(block);
    if (fileINode == null) { // block does not belong to any file
      return 0;
    }
    assert !fileINode.isDirectory() : "Block cannot belong to a directory";
    return fileINode.getReplication();
  }

  /* updates a block in under replication queue */
  synchronized void updateNeededReplications(Block block,
                        int curReplicasDelta, int expectedReplicasDelta) {
    NumberReplicas repl = countNodes(block);
    int curExpectedReplicas = getReplication(block);
    neededReplications.update(block, 
                              repl.liveReplicas(), 
                              repl.decommissionedReplicas(),
                              curExpectedReplicas,
                              curReplicasDelta, expectedReplicasDelta);
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by secondary namenodes
  //
  /////////////////////////////////////////////////////////
  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode on which blocks are located
   * @param size total size of blocks
   */
  BlocksWithLocations getBlocks(DatanodeID datanode, long size)
      throws IOException {
    checkSuperuserPrivilege();

    synchronized (this) {
      DatanodeDescriptor node = getDatanode(datanode);
      if (node == null) {
        NameNode.stateChangeLog
            .warn("BLOCK* NameSystem.getBlocks: "
                + "Asking for blocks from an unrecorded node "
                + datanode.getName());
        throw new IllegalArgumentException(
            "Unexpected exception.  Got getBlocks message for datanode "
                + datanode.getName() + ", but there is no info for it");
      }

      int numBlocks = node.numBlocks();
      if (numBlocks == 0) {
        return new BlocksWithLocations(new BlockWithLocations[0]);
      }
      Iterator<Block> iter = node.getBlockIterator();
      int startBlock = r.nextInt(numBlocks); // starting from a random block
      // skip blocks
      for (int i = 0; i < startBlock; i++) {
        iter.next();
      }
      List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
      long totalSize = 0;
      while (totalSize < size && iter.hasNext()) {
        totalSize += addBlock(iter.next(), results);
      }
      if (totalSize < size) {
        iter = node.getBlockIterator(); // start from the beginning
        for (int i = 0; i < startBlock && totalSize < size; i++) {
          totalSize += addBlock(iter.next(), results);
        }
      }

      return new BlocksWithLocations(
          results.toArray(new BlockWithLocations[results.size()]));
    }
  }
  
  /**
   * Get access keys
   * 
   * @return current access keys
   */
  ExportedBlockKeys getBlockKeys() {
    return isAccessTokenEnabled ? accessTokenHandler.exportKeys()
        : ExportedBlockKeys.DUMMY_KEYS;
  }

  /**
   * Get all valid locations of the block & add the block to results
   * return the length of the added block; 0 if the block is not added
   */
  private long addBlock(Block block, List<BlockWithLocations> results) {
    ArrayList<String> machineSet =
      new ArrayList<String>(blocksMap.numNodes(block));
    for(Iterator<DatanodeDescriptor> it = 
      blocksMap.nodeIterator(block); it.hasNext();) {
      String storageID = it.next().getStorageID();
      // filter invalidate replicas
      Collection<Block> blocks = recentInvalidateSets.get(storageID); 
      if(blocks==null || !blocks.contains(block)) {
        machineSet.add(storageID);
      }
    }
    if(machineSet.size() == 0) {
      return 0;
    } else {
      results.add(new BlockWithLocations(block, 
          machineSet.toArray(new String[machineSet.size()])));
      return block.getNumBytes();
    }
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   * @throws IOException
   */
  public void setPermission(String src, FsPermission permission
      ) throws IOException {
    FSPermissionChecker pc = new FSPermissionChecker(fsOwnerShortUserName,
        supergroup);
    synchronized (this) {
      if (isInSafeMode())
         throw new SafeModeException("Cannot set permission for " + src, safeMode);
      checkOwner(pc, src);
      dir.setPermission(src, permission);
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setPermission", src, null, stat);
    }
  }

  /**
   * Set owner for an existing file.
   * @throws IOException
   */
  public void setOwner(String src, String username, String group
      ) throws IOException {
    FSPermissionChecker pc = new FSPermissionChecker(fsOwnerShortUserName,
        supergroup);
    synchronized (this) {
      if (isInSafeMode())
         throw new SafeModeException("Cannot set owner for " + src, safeMode);
      checkOwner(pc, src);
      if (!pc.isSuperUser()) {
        if (username != null && !pc.getUser().equals(username)) {
          throw new AccessControlException("Non-super user cannot change owner");
        }
        if (group != null && !pc.containsGroup(group)) {
          throw new AccessControlException("User does not belong to " + group);
        }
      }
      dir.setOwner(src, username, group);
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setOwner", src, null, stat);
    }
  }

  /**
   * Get block locations within the specified range.
   * 
   * @see #getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src,
      long offset, long length) throws IOException {
    LocatedBlocks blocks = getBlockLocations(src, offset, length, true, true,
        true);
    if (blocks != null) {
      //sort the blocks
      // In some deployment cases, cluster is with separation of task tracker 
      // and datanode which means client machines will not always be recognized 
      // as known data nodes, so here we should try to get node (but not 
      // datanode only) for locality based sort.
      Node client = host2DataNodeMap.getDatanodeByHost(
          clientMachine);
      if (client == null) {
        List<String> hosts = new ArrayList<String> (1);
        hosts.add(clientMachine);
        String rName = dnsToSwitchMapping.resolve(hosts).get(0);
        if (rName != null)
          client = new NodeBase(clientMachine, rName);
      }   

      DFSUtil.StaleComparator comparator = null;
      if (avoidStaleDataNodesForRead) {
        comparator = new DFSUtil.StaleComparator(staleInterval);
      }
      // Note: the last block is also included and sorted
      for (LocatedBlock b : blocks.getLocatedBlocks()) {
        clusterMap.pseudoSortByDistance(client, b.getLocations());
        if (avoidStaleDataNodesForRead) {
          Arrays.sort(b.getLocations(), comparator);
        }
      }
    }
    return blocks;
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(String src, long offset, long length
      ) throws IOException {
    return getBlockLocations(src, offset, length, false, true, true);
  }

  /**
   * Get block locations within the specified range.
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(String src, long offset, long length,
      boolean doAccessTime, boolean needBlockToken, boolean checkSafeMode)
      throws IOException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      checkPathAccess(pc, src, FsAction.READ);
    }

    if (offset < 0) {
      throw new IOException("Negative offset is not supported. File: " + src );
    }
    if (length < 0) {
      throw new IOException("Negative length is not supported. File: " + src );
    }
    final LocatedBlocks ret = getBlockLocationsInternal(src, 
        offset, length, Integer.MAX_VALUE, doAccessTime, needBlockToken);  
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "open", src, null, null);
    }
    if (checkSafeMode && isInSafeMode()) {
      for (LocatedBlock b : ret.getLocatedBlocks()) {
        // if safemode & no block locations yet then throw safemodeException
        if ((b.getLocations() == null) || (b.getLocations().length == 0)) {
          throw new SafeModeException("Zero blocklocations for " + src,
              safeMode);
        }
      }
    }
    return ret;
  }

  private synchronized LocatedBlocks getBlockLocationsInternal(String src,
                                                       long offset, 
                                                       long length,
                                                       int nrBlocksToReturn,
                                                       boolean doAccessTime, 
                                                       boolean needBlockToken)
                                                       throws IOException {
    INodeFile inode = dir.getFileINode(src);
    if(inode == null) {
      return null;
    }
    if (doAccessTime && isAccessTimeSupported()) {
      dir.setTimes(src, inode, -1, now(), false);
    }
    Block[] blocks = inode.getBlocks();
    if (blocks == null) {
      return null;
    }
    if (blocks.length == 0) {
      return inode.createLocatedBlocks(new ArrayList<LocatedBlock>(blocks.length));
    }
    List<LocatedBlock> results;
    results = new ArrayList<LocatedBlock>(blocks.length);

    int curBlk = 0;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }
    
    if (nrBlocks > 0 && curBlk == nrBlocks)   // offset >= end of file
      return null;
    
    long endOff = offset + length;
    
    do {
      // get block locations
      int numNodes = blocksMap.numNodes(blocks[curBlk]);
      int numCorruptNodes = countNodes(blocks[curBlk]).corruptReplicas();
      int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blocks[curBlk]); 
      if (numCorruptNodes != numCorruptReplicas) {
        LOG.warn("Inconsistent number of corrupt replicas for " + 
            blocks[curBlk] + "blockMap has " + numCorruptNodes + 
            " but corrupt replicas map has " + numCorruptReplicas);
      }
      DatanodeDescriptor[] machineSet = null;
      boolean blockCorrupt = false;
      if (inode.isUnderConstruction() && curBlk == blocks.length - 1
          && blocksMap.numNodes(blocks[curBlk]) == 0) {
        // get unfinished block locations
        INodeFileUnderConstruction cons = (INodeFileUnderConstruction)inode;
        machineSet = cons.getTargets();
        blockCorrupt = false;
      } else {
        blockCorrupt = (numCorruptNodes == numNodes);
        int numMachineSet = blockCorrupt ? numNodes : 
                            (numNodes - numCorruptNodes);
        machineSet = new DatanodeDescriptor[numMachineSet];
        if (numMachineSet > 0) {
          numNodes = 0;
          for(Iterator<DatanodeDescriptor> it = 
              blocksMap.nodeIterator(blocks[curBlk]); it.hasNext();) {
            DatanodeDescriptor dn = it.next();
            boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blocks[curBlk], dn);
            if (blockCorrupt || (!blockCorrupt && !replicaCorrupt))
              machineSet[numNodes++] = dn;
          }
        }
      }
      LocatedBlock b = new LocatedBlock(blocks[curBlk], machineSet, curPos,
          blockCorrupt);
      if(isAccessTokenEnabled && needBlockToken) {
        b.setBlockToken(accessTokenHandler.generateToken(b.getBlock(), 
            EnumSet.of(BlockTokenSecretManager.AccessMode.READ)));
      }
      
      results.add(b); 
      curPos += blocks[curBlk].getNumBytes();
      curBlk++;
    } while (curPos < endOff 
          && curBlk < blocks.length 
          && results.size() < nrBlocksToReturn);
    
    return inode.createLocatedBlocks(results);
  }
  

  /**
   * Moves all the blocks from {@code srcs} in the order of {@code srcs} array
   * and appends them to {@code target}.
   * The {@code srcs} files are deleted.
   * To avoid rollbacks we will verify validitity of ALL of the args
   * before we start actual move.
   * @param target file to move the blocks to
   * @param srcs list of file to move the blocks from
   * @throws IOException
   */
  void concat(String target, String [] srcs) 
      throws IOException {
    if(FSNamesystem.LOG.isDebugEnabled()) {
      FSNamesystem.LOG.debug("concat " + Arrays.toString(srcs) +
          " to " + target);
    }
    
    // verify args
    if(target.isEmpty()) {
      throw new IllegalArgumentException("Target file name is empty");
    }
    if(srcs == null || srcs.length == 0) {
      throw new IllegalArgumentException("No sources given");
    }
    
    // We require all files be in the same directory
    String trgParent = 
      target.substring(0, target.lastIndexOf(Path.SEPARATOR_CHAR));
    for (String s : srcs) {
      String srcParent = s.substring(0, s.lastIndexOf(Path.SEPARATOR_CHAR));
      if (!srcParent.equals(trgParent)) {
        throw new IllegalArgumentException(
           "Sources and target are not in the same directory");
      }
    }

    FSPermissionChecker pc = getPermissionChecker();
    HdfsFileStatus resultingStat = null;
    synchronized(this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot concat " + target, safeMode);
      }
      concatInternal(pc, target, srcs);
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        resultingStat = dir.getFileInfo(target);
      }
    }
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getLoginUser(),
                    Server.getRemoteIp(),
                    "concat", Arrays.toString(srcs), target, resultingStat);
    }
  }

  /** See {@link #concat(String, String[])} */
  private void concatInternal(FSPermissionChecker pc, String target, String [] srcs) 
      throws IOException {

    // write permission for the target
    if (isPermissionEnabled) {
      checkPathAccess(pc, target, FsAction.WRITE);

      // and srcs
      for(String aSrc: srcs) {
        checkPathAccess(pc, aSrc, FsAction.READ); // read the file
        checkParentAccess(pc, aSrc, FsAction.WRITE); // for delete 
      }
    }

    // to make sure no two files are the same
    Set<INode> si = new HashSet<INode>();

    // we put the following prerequisite for the operation
    // replication and blocks sizes should be the same for ALL the blocks

    // check the target
    final INodeFile trgInode = dir.getFileINode(target);
    if(trgInode.isUnderConstruction()) {
      throw new IllegalArgumentException("concat: target file "
          + target + " is under construction");
    }
    // per design target shouldn't be empty and all the blocks same size
    if(trgInode.blocks.length  == 0) {
      throw new IllegalArgumentException("concat: target file "
          + target + " is empty");
    }

    long blockSize = trgInode.getPreferredBlockSize();

    // check the end block to be full
    final BlockInfo last =  trgInode.blocks[trgInode.blocks.length-1];
    if(blockSize != last.getNumBytes()) {
      throw new IllegalArgumentException("The last block in " + target
          + " is not full; last block size = " + last.getNumBytes()
          + " but file block size = " + blockSize);
    }

    si.add(trgInode);
    short repl = trgInode.getReplication();

    // now check the srcs
    boolean endSrc = false; // final src file doesn't have to have full end block
    for(int i=0; i<srcs.length; i++) {
      String src = srcs[i];
      if(i==srcs.length-1)
        endSrc=true;

      final INodeFile srcInode = dir.getFileINode(src);
      if(src.isEmpty() 
          || srcInode.isUnderConstruction()
          || srcInode.blocks.length == 0) {
        throw new IllegalArgumentException("concat: source file " + src
            + " is invalid or empty or underConstruction");
      }

      // check replication and blocks size
      if(repl != srcInode.getReplication()) {
        throw new IllegalArgumentException("concat: the source file "
            + src + " and the target file " + target
            + " should have the same replication: source replication is "
            + srcInode.getReplication()
            + " but target replication is " + repl);
      }

      //boolean endBlock=false;
      // verify that all the blocks are of the same length as target
      // should be enough to check the end blocks
      final BlockInfo[] srcBlocks = srcInode.getBlocks();
      int idx = srcBlocks.length-1;
      if(endSrc)
        idx = srcBlocks.length-2; // end block of endSrc is OK not to be full
      if(idx >= 0 && srcBlocks[idx].getNumBytes() != blockSize) {
        throw new IllegalArgumentException("concat: the soruce file "
            + src + " and the target file " + target
            + " should have the same blocks sizes: target block size is "
            + blockSize + " but the size of source block " + idx + " is "
            + srcBlocks[idx].getNumBytes());
      }

      si.add(srcInode);
    }

    // make sure no two files are the same
    if(si.size() < srcs.length+1) { // trg + srcs
      // it means at least two files are the same
      throw new IllegalArgumentException(
          "concat: at least two of the source files are the same");
    }

    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.concat: " + 
          Arrays.toString(srcs) + " to " + target);
    }

    dir.concat(target,srcs);
  }

  /**
   * stores the modification and access time for this inode. 
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
                            " Please set dfs.access.time.precision configuration parameter");
    }
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot set accesstimes  for " + src,
            safeMode);
      }
      //
      // The caller needs to have write access to set access & modification
      // times.
      if (isPermissionEnabled) {
        checkPathAccess(pc, src, FsAction.WRITE);
      }
      INodeFile inode = dir.getFileINode(src);
      if (inode != null) {
        dir.setTimes(src, inode, mtime, atime, true);
        if (auditLog.isInfoEnabled() && isExternalInvocation()) {
          final HdfsFileStatus stat = dir.getFileInfo(src);
          logAuditEvent(UserGroupInformation.getCurrentUser(),
              Server.getRemoteIp(), "setTimes", src, null, stat);
        }
      } else {
        throw new FileNotFoundException("File " + src + " does not exist");
      }
    }
  }

  /**
   * Set replication for an existing file.
   * 
   * The NameNode sets new replication and schedules either replication of 
   * under-replicated data blocks or removal of the eccessive block copies 
   * if the blocks are over-replicated.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param src file name
   * @param replication new replication
   * @return true if successful; 
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(String src, short replication) 
                                throws IOException {
    boolean status = setReplicationInternal(src, replication);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "setReplication", src, null, null);
    }
    return status;
  }

  private boolean setReplicationInternal(String src,
                                             short replication
                                             ) throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isInSafeMode())
        throw new SafeModeException("Cannot set replication for " + src,
            safeMode);
      verifyReplication(src, replication, null);
      if (isPermissionEnabled) {
        checkPathAccess(pc, src, FsAction.WRITE);
      }

      int[] oldReplication = new int[1];
      Block[] fileBlocks;
      fileBlocks = dir.setReplication(src, replication, oldReplication);
      if (fileBlocks == null) // file not found or is a directory
        return false;
      int oldRepl = oldReplication[0];
      if (oldRepl == replication) // the same replication
        return true;

      // update needReplication priority queues
      for(int idx = 0; idx < fileBlocks.length; idx++)
        updateNeededReplications(fileBlocks[idx], 0, replication - oldRepl);

      if (oldRepl > replication) {
        // old replication > the new one; need to remove copies
        LOG.info("Reducing replication for " + src 
            + ". New replication is " + replication);
        for (int idx = 0; idx < fileBlocks.length; idx++)
          processOverReplicatedBlock(fileBlocks[idx], replication, null, null);
      } else { // replication factor is increased
        LOG.info("Increasing replication for " + src
            + ". New replication is " + replication);
      }
    }
    return true;
  }
    
  long getPreferredBlockSize(String filename) throws IOException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      checkTraverse(pc, filename);
    }
    return dir.getPreferredBlockSize(filename);
  }
    
  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
  private void verifyReplication(String src, 
                                 short replication, 
                                 String clientName 
                                 ) throws IOException {
    String text = "file " + src 
      + ((clientName != null) ? " on client " + clientName : "")
      + ".\n"
      + "Requested replication " + replication;

    if (replication > maxReplication)
      throw new IOException(text + " exceeds maximum " + maxReplication);
      
    if (replication < minReplication)
      throw new IOException( 
                            text + " is less than the required minimum " + minReplication);
  }

  /*
   * Verify that parent dir exists
   */
  private void verifyParentDir(String src) throws FileAlreadyExistsException,
      FileNotFoundException {
    Path parent = new Path(src).getParent();
    if (parent != null) {
      INode[] pathINodes = dir.getExistingPathINodes(parent.toString());
      if (pathINodes[pathINodes.length - 1] == null) {
        throw new FileNotFoundException("Parent directory doesn't exist: "
            + parent.toString());
      } else if (!pathINodes[pathINodes.length - 1].isDirectory()) {
        throw new FileAlreadyExistsException("Parent path is not a directory: "
            + parent.toString());
      }
    }
  }

  /**
   * Create a new file entry in the namespace.
   * 
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   * 
   * @throws IOException if file name is invalid
   *         {@link FSDirectory#isValidToCreate(String)}.
   */
  void startFile(String src, PermissionStatus permissions,
                 String holder, String clientMachine,
                 boolean overwrite, boolean createParent, short replication, long blockSize
                ) throws IOException {
    startFileInternal(src, permissions, holder, clientMachine, overwrite, false,
                      createParent, replication, blockSize);
    getEditLog().logSync();
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "create", src, null, stat);
    }
  }

  private void startFileInternal(String src,
                                              PermissionStatus permissions,
                                              String holder, 
                                              String clientMachine, 
                                              boolean overwrite,
                                              boolean append,
                                              boolean createParent,
                                              short replication,
                                              long blockSize
                                              ) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* startFile: src=" + src
          + ", holder=" + holder
          + ", clientMachine=" + clientMachine
          + ", createParent=" + createParent
          + ", replication=" + replication
          + ", overwrite=" + overwrite
          + ", append=" + append);
    }

    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isInSafeMode())
        throw new SafeModeException("Cannot create " + src, safeMode);
      if (!DFSUtil.isValidName(src)) {
        throw new IOException("Invalid name: " + src);
      }

      // Verify that the destination does not exist as a directory already.
      boolean pathExists = dir.exists(src);
      if (pathExists && dir.isDir(src)) {
        throw new IOException("Cannot create "+ src + "; already exists as a directory");
      }

      if (isPermissionEnabled) {
        if (append || (overwrite && pathExists)) {
          checkPathAccess(pc, src, FsAction.WRITE);
        } else {
          checkAncestorAccess(pc, src, FsAction.WRITE);
        }
      }

      if (!createParent) {
        verifyParentDir(src);
      }

      try {
        INode myFile = dir.getFileINode(src);
        recoverLeaseInternal(myFile, src, holder, clientMachine, false);

        try {
          verifyReplication(src, replication, clientMachine);
        } catch (IOException e) {
          throw new IOException("failed to create " + e.getMessage());
        }
        if (append) {
          if (myFile == null) {
            throw new FileNotFoundException("failed to append to non-existent "
                + src + " on client " + clientMachine);
          } else if (myFile.isDirectory()) {
            throw new IOException("failed to append to directory " + src
                + " on client " + clientMachine);
          }
        } else if (!dir.isValidToCreate(src)) {
          if (overwrite) {
            delete(src, true);
          } else {
            throw new IOException("failed to create file " + src
                + " on client " + clientMachine
                + " either because the filename is invalid or the file exists");
          }
        }

        DatanodeDescriptor clientNode = host2DataNodeMap
            .getDatanodeByHost(clientMachine);

        if (append) {
          //
          // Replace current node with a INodeUnderConstruction.
          // Recreate in-memory lease record.
          //
          INodeFile node = (INodeFile) myFile;
          INodeFileUnderConstruction cons = new INodeFileUnderConstruction(
              node.getLocalNameBytes(), node.getReplication(),
              node.getModificationTime(), node.getPreferredBlockSize(),
              node.getBlocks(), node.getPermissionStatus(), holder,
              clientMachine, clientNode);
          dir.replaceNode(src, node, cons);
          leaseManager.addLease(cons.clientName, src);

        } else {
          // Now we can add the name to the filesystem. This file has no
          // blocks associated with it.
          //
          checkFsObjectLimit();

          // increment global generation stamp
          long genstamp = nextGenerationStamp();
          INodeFileUnderConstruction newNode = dir.addFile(src, permissions,
              replication, blockSize, holder, clientMachine, clientNode,
              genstamp);
          if (newNode == null) {
            throw new IOException("DIR* startFile: Unable to add to namespace");
          }
          leaseManager.addLease(newNode.clientName, src);
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* startFile: "
                                       +"add "+src+" to namespace for "+holder);
          }
        }
      } catch (IOException ie) {
        NameNode.stateChangeLog.warn("DIR* startFile: "
                                     +ie.getMessage());
        throw ie;
      }
    }
  }

  /**
   * Returns true if the file is closed
   */
  boolean isFileClosed(String src)
      throws AccessControlException, IOException {
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isPermissionEnabled) {
        checkTraverse(pc, src);
      }
      INode inode = dir.getFileINode(src);
      if (inode == null) {
        throw new FileNotFoundException("File not found " + src);
      }

      return !inode.isUnderConstruction();
    }
  }
  
  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   * 
   * @param src the path of the file to start lease recovery
   * @param holder the lease holder's name
   * @param clientMachine the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(String src, String holder, String clientMachine)
  throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot recover the lease of " + src,
            safeMode);
      }
      if (!DFSUtil.isValidName(src)) {
        throw new IOException("Invalid name: " + src);
      }

      INode inode = dir.getFileINode(src);
      if (inode == null) {
        throw new FileNotFoundException("File not found " + src);
      }

      if (!inode.isUnderConstruction()) {
        return true;
      }
      if (isPermissionEnabled) {
        checkPathAccess(pc, src, FsAction.WRITE);
      }

      recoverLeaseInternal(inode, src, holder, clientMachine, true);
    }
    return false;
  }
  
  private void recoverLeaseInternal(INode fileInode, 
      String src, String holder, String clientMachine, boolean force)
  throws IOException {
    if (fileInode != null && fileInode.isUnderConstruction()) {
      INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) fileInode;
      //
      // If the file is under construction , then it must be in our
      // leases. Find the appropriate lease record.
      //
      Lease lease = leaseManager.getLease(holder);
      //
      // We found the lease for this file. And surprisingly the original
      // holder is trying to recreate this file. This should never occur.
      //
      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLeaseByPath(src);
        if (leaseFile != null && leaseFile.equals(lease)) { 
          throw new AlreadyBeingCreatedException(
                    "failed to create file " + src + " for " + holder +
                    " on client " + clientMachine + 
                    " because current leaseholder is trying to recreate file");
        }
      }
      //
      // Find the original holder.
      //
      lease = leaseManager.getLease(pendingFile.clientName);
      if (lease == null) {
        throw new AlreadyBeingCreatedException(
                    "failed to create file " + src + " for " + holder +
                    " on client " + clientMachine + 
                    " because pendingCreates is non-null but no leases found");
      }
      if (force) {
        // close now: no need to wait for soft lease expiration and 
        // close only the file src
        LOG.info("recoverLease: recover lease " + lease + ", src=" + src +
                 " from client " + pendingFile.clientName);
        internalReleaseLeaseOne(lease, src);
      } else {
        //
        // If the original holder has not renewed in the last SOFTLIMIT 
        // period, then start lease recovery.
        //
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover lease " + lease + ", src=" + src +
              " from client " + pendingFile.clientName);
          internalReleaseLease(lease, src);
        }
        throw new AlreadyBeingCreatedException(
            "failed to create file " + src + " for " + holder +
            " on client " + clientMachine + 
            ", because this file is already being created by " +
            pendingFile.getClientName() + 
            " on " + pendingFile.getClientMachine());
      }
    }

  }
  
  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(String src, String holder, String clientMachine
      ) throws IOException {
    if (!allowBrokenAppend) {
      throw new IOException("Append is not supported. " +
          "Please see the dfs.support.append configuration parameter");
    }
    startFileInternal(src, null, holder, clientMachine, false, true, 
                      false, (short)maxReplication, (long)0);
    getEditLog().logSync();

    //
    // Create a LocatedBlock object for the last block of the file
    // to be returned to the client. Return null if the file does not
    // have a partial block at the end.
    //
    LocatedBlock lb = null;
    synchronized (this) {
      // Need to re-check existence here, since the file may have been deleted
      // in between the synchronized blocks
      INodeFileUnderConstruction file = checkLease(src, holder);

      Block[] blocks = file.getBlocks();
      if (blocks != null && blocks.length > 0) {
        Block last = blocks[blocks.length-1];
        BlockInfo storedBlock = blocksMap.getStoredBlock(last);
        if (file.getPreferredBlockSize() > storedBlock.getNumBytes()) {
          long fileLength = file.computeContentSummary().getLength();
          DatanodeDescriptor[] targets = new DatanodeDescriptor[blocksMap.numNodes(last)];
          Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(last);
          for (int i = 0; it != null && it.hasNext(); i++) {
            targets[i] = it.next();
          }
          // remove the replica locations of this block from the blocksMap
          for (int i = 0; i < targets.length; i++) {
            targets[i].removeBlock(storedBlock);
          }
          // set the locations of the last block in the lease record
          file.setLastBlock(storedBlock, targets);

          lb = new LocatedBlock(last, targets, 
                                fileLength-storedBlock.getNumBytes());
          if (isAccessTokenEnabled) {
            lb.setBlockToken(accessTokenHandler.generateToken(lb.getBlock(), 
                EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
          }

          // Remove block from replication queue.
          updateNeededReplications(last, 0, 0);

          // remove this block from the list of pending blocks to be deleted. 
          // This reduces the possibility of triggering HADOOP-1349.
          //
          for (DatanodeDescriptor dd : targets) {
            String datanodeId = dd.getStorageID();
            Collection<Block> v = recentInvalidateSets.get(datanodeId);
            if (v != null && v.remove(last)) {
              if (v.isEmpty()) {
                recentInvalidateSets.remove(datanodeId);
              }
              pendingDeletionBlocksCount--;
            }
          }
        }
      }
    }
    if (lb != null) {
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* appendFile: "
            +src+" for "+holder+" at "+clientMachine
            +" block " + lb.getBlock()
            +" block size " + lb.getBlock().getNumBytes());
      }
    }

    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "append", src, null, null);
    }
    return lb;
  }

  /**
   * Stub for old callers pre-HDFS-630
   */
  public LocatedBlock getAdditionalBlock(String src, 
                                         String clientName
                                         ) throws IOException {
    return getAdditionalBlock(src, clientName, null);
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to).  Return an array that consists
   * of the block, plus a set of machines.  The first on this list should
   * be where the client writes data.  Subsequent items in the list must
   * be provided in the connection to the first datanode.
   *
   * Make sure the previous blocks have been reported by datanodes and
   * are replicated.  Will return an empty 2-elt array if we want the
   * client to "try again later".
   */
  public LocatedBlock getAdditionalBlock(String src, 
                                         String clientName,
                                         HashMap<Node, Node> excludedNodes
                                         ) throws IOException {
    long fileLength, blockSize;
    int replication;
    DatanodeDescriptor clientNode = null;
    Block newBlock = null;

    NameNode.stateChangeLog.debug("BLOCK* getAdditionalBlock: "
                                  +src+" for "+clientName);

    synchronized (this) {
      if (isInSafeMode()) {//check safemode first for failing-fast
        throw new SafeModeException("Cannot add block to " + src, safeMode);
      }
      // have we exceeded the configured limit of fs objects.
      checkFsObjectLimit();

      INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);

      //
      // If we fail this, bad things happen!
      //
      if (!checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }
      fileLength = pendingFile.computeContentSummary().getLength();
      blockSize = pendingFile.getPreferredBlockSize();
      clientNode = pendingFile.getClientNode();
      replication = (int)pendingFile.getReplication();
    }

    // choose targets for the new block to be allocated.
    DatanodeDescriptor targets[] = replicator.chooseTarget(src, 
                                                           replication,
                                                           clientNode,
                                                           excludedNodes,
                                                           blockSize);
    if (targets.length < this.minReplication) {
      throw new IOException("File " + src + " could only be replicated to " +
                            targets.length + " nodes, instead of " +
                            minReplication);
    }

    // Allocate a new block and record it in the INode. 
    synchronized (this) {
      if (isInSafeMode()) { //make sure it is not in safemode again.
        throw new SafeModeException("Cannot add block to " + src, safeMode);
      }
      INode[] pathINodes = dir.getExistingPathINodes(src);
      int inodesLen = pathINodes.length;
      checkLease(src, clientName, pathINodes[inodesLen-1]);
      INodeFileUnderConstruction pendingFile  = (INodeFileUnderConstruction) 
                                                pathINodes[inodesLen - 1];
                                                           
      if (!checkFileProgress(pendingFile, false)) {
        throw new NotReplicatedYetException("Not replicated yet:" + src);
      }

      // allocate new block record block locations in INode.
      newBlock = allocateBlock(src, pathINodes);
      pendingFile.setTargets(targets);
      
      for (DatanodeDescriptor dn : targets) {
        dn.incBlocksScheduled();
      }
      dir.persistBlocks(src, pendingFile);
    }
    if (persistBlocks) {
      getEditLog().logSync();
    }
        
    // Create next block
    LocatedBlock b = new LocatedBlock(newBlock, targets, fileLength);
    if (isAccessTokenEnabled) {
      b.setBlockToken(accessTokenHandler.generateToken(b.getBlock(), 
          EnumSet.of(BlockTokenSecretManager.AccessMode.WRITE)));
    }
    return b;
  }

  /**
   * The client would like to let go of the given block
   */
  public synchronized boolean abandonBlock(Block b, String src, String holder
      ) throws IOException {
    //
    // Remove the block from the pending creates list
    //
    NameNode.stateChangeLog.debug("BLOCK* abandonBlock: " + b + "of " + src);
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot abandon " + b +
                                  " for " + src, safeMode);
    }
    INodeFileUnderConstruction file = checkLease(src, holder);
    dir.removeBlock(src, file, b);
    NameNode.stateChangeLog.debug("BLOCK* abandonBlock: " + b
                                    + " is removed from pendingCreates");
    dir.persistBlocks(src, file);
    if (persistBlocks) {
      getEditLog().logSync();
    }
    return true;
  }
  
  // make sure that we still have the lease on this file.
  private INodeFileUnderConstruction checkLease(String src, String holder) 
                                                      throws IOException {
    INodeFile file = dir.getFileINode(src);
    checkLease(src, holder, file);
    return (INodeFileUnderConstruction)file;
  }

  private void checkLease(String src, String holder, INode file) 
                                                     throws IOException {

    if (file == null || file.isDirectory()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src +
                                      " File does not exist. " +
                                      (lease != null ? lease.toString() :
                                       "Holder " + holder + 
                                       " does not have any open files"));
    }
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder);
      throw new LeaseExpiredException("No lease on " + src + 
                                      " File is not open for writing. " +
                                      (lease != null ? lease.toString() :
                                       "Holder " + holder + 
                                       " does not have any open files"));
    }
    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)file;
    if (holder != null && !pendingFile.getClientName().equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " + src + " owned by "
          + pendingFile.getClientName() + " but is accessed by " + holder);
    }
  }

  /**
   * The FSNamesystem will already know the blocks that make up the file.
   * Before we return, we make sure that all the file's blocks have 
   * been reported by datanodes and are replicated correctly.
   */
  
  enum CompleteFileStatus {
    OPERATION_FAILED,
    STILL_WAITING,
    COMPLETE_SUCCESS
  }
  
  public CompleteFileStatus completeFile(String src, String holder) throws IOException {
    CompleteFileStatus status = completeFileInternal(src, holder);
    getEditLog().logSync();
    return status;
  }


  private synchronized CompleteFileStatus completeFileInternal(String src, 
                                                String holder) throws IOException {
    NameNode.stateChangeLog.debug("DIR* completeFile: " + src + " for " + holder);
    if (isInSafeMode())
      throw new SafeModeException("Cannot complete " + src, safeMode);

    INodeFileUnderConstruction pendingFile  = checkLease(src, holder);
    Block[] fileBlocks =  dir.getFileBlocks(src);

    if (fileBlocks == null ) {
      NameNode.stateChangeLog.warn("DIR* completeFile: "
                                   + "failed to complete " + src
                                   + " because dir.getFileBlocks() is null,"
                                   + " pending from " + pendingFile.getClientMachine());
      return CompleteFileStatus.OPERATION_FAILED;
    }

    if (!checkFileProgress(pendingFile, true)) {
      return CompleteFileStatus.STILL_WAITING;
    }

    finalizeINodeFileUnderConstruction(src, pendingFile);

    NameNode.stateChangeLog.info("DIR* completeFile: " + src
                                  + " is closed by " + holder);
    return CompleteFileStatus.COMPLETE_SUCCESS;
  }

  /** 
   * Check all blocks of a file. If any blocks are lower than their intended
   * replication factor, then insert them into neededReplication
   */
  private void checkReplicationFactor(INodeFile file) {
    int numExpectedReplicas = file.getReplication();
    Block[] pendingBlocks = file.getBlocks();
    int nrBlocks = pendingBlocks.length;
    for (int i = 0; i < nrBlocks; i++) {
      // filter out containingNodes that are marked for decommission.
      NumberReplicas number = countNodes(pendingBlocks[i]);
      if (number.liveReplicas() < numExpectedReplicas) {
        neededReplications.add(pendingBlocks[i], 
                               number.liveReplicas(), 
                               number.decommissionedReplicas,
                               numExpectedReplicas);
      }
    }
  }

  static Random randBlockId = new Random();
    
  /**
   * Allocate a block at the given pending filename
   * 
   * @param src path to the file
   * @param inodes INode representing each of the components of src. 
   *        <code>inodes[inodes.length-1]</code> is the INode for the file.
   */
  private Block allocateBlock(String src, INode[] inodes) throws IOException {
    Block b = new Block(FSNamesystem.randBlockId.nextLong(), 0, 0); 
    while(isValidBlock(b)) {
      b.setBlockId(FSNamesystem.randBlockId.nextLong());
    }
    b.setGenerationStamp(getGenerationStamp());
    b = dir.addBlock(src, inodes, b);
    NameNode.stateChangeLog.info("BLOCK* allocateBlock: "
                                 +src+ ". "+b);
    return b;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   */
  synchronized boolean checkFileProgress(INodeFile v, boolean checkall) {
    if (checkall) {
      //
      // check all blocks of the file.
      //
      for (Block block: v.getBlocks()) {
        if (blocksMap.numNodes(block) < this.minReplication) {
          return false;
        }
      }
    } else {
      //
      // check the penultimate block of this file
      //
      Block b = v.getPenultimateBlock();
      if (b != null) {
        if (blocksMap.numNodes(b) < this.minReplication) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Remove a datanode from the invalidatesSet
   * @param n datanode
   */
  void removeFromInvalidates(String storageID) {
    Collection<Block> blocks = recentInvalidateSets.remove(storageID);
    if (blocks != null) {
      pendingDeletionBlocksCount -= blocks.size();
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on 
   * specified datanode
   * @param b block
   * @param n datanode
   * @param log true to create an entry in the log
   */
  void addToInvalidates(Block b, DatanodeInfo dn, boolean log) {
    addToInvalidatesNoLog(b, dn);
    if (log) {
      NameNode.stateChangeLog.info("BLOCK* addToInvalidates: "
          + b.getBlockName() + " to " + dn.getName());
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode and log the operation
   * 
   * @param b
   *          block
   * @param dn
   *          datanode
   */
  void addToInvalidates(Block b, DatanodeInfo dn) {
    addToInvalidates(b, dn, true);
  }
  
  /**
   * Adds block to list of blocks which will be invalidated on 
   * specified datanode
   * @param b block
   * @param n datanode
   */
  void addToInvalidatesNoLog(Block b, DatanodeInfo n) {
    Collection<Block> invalidateSet = recentInvalidateSets.get(n.getStorageID());
    if (invalidateSet == null) {
      invalidateSet = new HashSet<Block>();
      recentInvalidateSets.put(n.getStorageID(), invalidateSet);
    }
    if (invalidateSet.add(b)) {
      pendingDeletionBlocksCount++;
    }
  }
  
  /**
   * Adds block to list of blocks which will be invalidated on 
   * all its datanodes.
   */
  private void addToInvalidates(Block b) {
    StringBuilder datanodes = new StringBuilder();
    for (Iterator<DatanodeDescriptor> it = 
                                blocksMap.nodeIterator(b); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      addToInvalidates(b, node, false);
      datanodes.append(node.getName()).append(" ");
    }
    if (datanodes.length() != 0) {
      NameNode.stateChangeLog.info("BLOCK* addToInvalidates: "
          + b.getBlockName() + " to " + datanodes.toString());
    }
  }

  /**
   * dumps the contents of recentInvalidateSets
   */
  private synchronized void dumpRecentInvalidateSets(PrintWriter out) {
    int size = recentInvalidateSets.values().size();
    out.println("Metasave: Blocks " + pendingDeletionBlocksCount 
        + " waiting deletion from " + size + " datanodes");
    if (size == 0) {
      return;
    }
    for(Map.Entry<String,Collection<Block>> entry : recentInvalidateSets.entrySet()) {
      Collection<Block> blocks = entry.getValue();
      if (blocks.size() > 0) {
        out.println(datanodeMap.get(entry.getKey()).getName() + blocks);
      }
    }
  }

  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   */
  public synchronized void markBlockAsCorrupt(Block blk, DatanodeInfo dn)
    throws IOException {
    DatanodeDescriptor node = getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot mark block" + blk.getBlockName() +
                            " as corrupt because datanode " + dn.getName() +
                            " does not exist. ");
    }
    
    final BlockInfo storedBlockInfo = blocksMap.getStoredBlock(blk);
    if (storedBlockInfo == null) {
      // Check if the replica is in the blockMap, if not 
      // ignore the request for now. This could happen when BlockScanner
      // thread of Datanode reports bad block before Block reports are sent
      // by the Datanode on startup
      NameNode.stateChangeLog.info("BLOCK markBlockAsCorrupt: " +
                                   blk + " could not be marked " +
                                   "as corrupt as it does not exists in " +
                                   "blocksMap");
    } else {
      INodeFile inode = storedBlockInfo.getINode();
      if (inode == null) {
        NameNode.stateChangeLog.info("BLOCK markBlockAsCorrupt: " +
                                     blk + " could not be marked " +
                                     "as corrupt as it does not belong to " +
                                     "any file");
        addToInvalidates(storedBlockInfo, node);
        return;
      } 
      // Add this replica to corruptReplicas Map 
      corruptReplicas.addToCorruptReplicasMap(storedBlockInfo, node);
      if (countNodes(storedBlockInfo).liveReplicas()>inode.getReplication()) {
        // the block is over-replicated so invalidate the replicas immediately
        invalidateBlock(storedBlockInfo, node);
      } else {
        // add the block to neededReplication 
        updateNeededReplications(storedBlockInfo, -1, 0);
      }
    }
  }

  /**
   * Invalidates the given block on the given datanode.
   */
  private synchronized void invalidateBlock(Block blk, DatanodeInfo dn)
    throws IOException {
    NameNode.stateChangeLog.info("DIR* invalidateBlock: " 
                                 + blk + " on " 
                                 + dn.getName());
    DatanodeDescriptor node = getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot invalidate " + blk +
                            " because datanode " + dn.getName() +
                            " does not exist");
    }

    // Check how many copies we have of the block.  If we have at least one
    // copy on a live node, then we can delete it. 
    int count = countNodes(blk).liveReplicas();
    if (count > 1) {
      addToInvalidates(blk, dn);
      removeStoredBlock(blk, node);
      NameNode.stateChangeLog.debug("BLOCK* invalidateBlocks: "
                                   + blk + " on " 
                                   + dn.getName() + " listed for deletion");
    } else {
      NameNode.stateChangeLog.info("BLOCK* invalidateBlocks: "
                                   + blk + " on " 
                                   + dn.getName() + " is the only copy and was not deleted");
    }
  }

  ////////////////////////////////////////////////////////////////
  // Here's how to handle block-copy failure during client write:
  // -- As usual, the client's write should result in a streaming
  // backup write to a k-machine sequence.
  // -- If one of the backup machines fails, no worries.  Fail silently.
  // -- Before client is allowed to close and finalize file, make sure
  // that the blocks are backed up.  Namenode may have to issue specific backup
  // commands to make up for earlier datanode failures.  Once all copies
  // are made, edit namespace and return to client.
  ////////////////////////////////////////////////////////////////

  /** Change the indicated filename. */
  public boolean renameTo(String src, String dst) throws IOException {
    boolean status = renameToInternal(src, dst);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(dst);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "rename", src, dst, stat);
    }
    return status;
  }

  private boolean renameToInternal(String src, String dst
      ) throws IOException {
    NameNode.stateChangeLog.debug("DIR* renameTo: " + src + " to " + dst);
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isInSafeMode())
        throw new SafeModeException("Cannot rename " + src, safeMode);
      if (!DFSUtil.isValidName(dst)) {
        throw new IOException("Invalid name: " + dst);
      }
  
      if (isPermissionEnabled) {
        //We should not be doing this.  This is move() not renameTo().
        //but for now,
        String actualdst = dir.isDir(dst)?
            dst + Path.SEPARATOR + new Path(src).getName(): dst;
        checkParentAccess(pc, src, FsAction.WRITE);
        checkAncestorAccess(pc, actualdst, FsAction.WRITE);
      }
  
      HdfsFileStatus dinfo = dir.getFileInfo(dst);
      if (dir.renameTo(src, dst)) {
        changeLease(src, dst, dinfo);     // update lease with new filename
        return true;
      }
    }
    return false;
  }

  /**
   * Remove the indicated filename from namespace. If the filename 
   * is a directory (non empty) and recursive is set to false then throw exception.
   */
    public boolean delete(String src, boolean recursive) throws IOException {
      if ((!recursive) && (!dir.isDirEmpty(src))) {
        throw new IOException(src + " is non empty");
      }
      if (NameNode.stateChangeLog.isDebugEnabled()) {
        NameNode.stateChangeLog.debug("DIR* delete: " + src);
      }
      boolean status = deleteInternal(src, true);
      if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
        logAuditEvent(UserGroupInformation.getCurrentUser(),
                      Server.getRemoteIp(),
                      "delete", src, null, null);
      }
      return status;
    }
    
  /**
   * Remove a file/directory from the namespace.
   * <p>
   * For large directories, deletion is incremental. The blocks under the
   * directory are collected and deleted a small number at a time holding the
   * {@link FSNamesystem} lock.
   * <p>
   * For small directory or file the deletion is done in one shot.
   */
  private boolean deleteInternal(String src,
      boolean enforcePermission) throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    ArrayList<Block> collectedBlocks = new ArrayList<Block>();
    synchronized (this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot delete " + src, safeMode);
      }
      if (enforcePermission && isPermissionEnabled) {
        checkPermission(pc, src, false, null, FsAction.WRITE, null,
            FsAction.ALL);
      }
      // Unlink the target directory from directory tree
      if (!dir.delete(src, collectedBlocks)) {
        return false;
      }
    }
    
    // Log directory deletion to editlog
    getEditLog().logSync();
    removeBlocks(collectedBlocks); // Incremental deletion of blocks
    collectedBlocks.clear();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* delete: " + src
          + " is removed");
    }
    return true;
  }
  
  /** From the given list, incrementally remove the blocks from blockManager */
  private void removeBlocks(List<Block> blocks) {
    int start = 0;
    int end = 0;
    while (start < blocks.size()) {
      end = BLOCK_DELETION_INCREMENT + start;
      end = end > blocks.size() ? blocks.size() : end;
      synchronized (this) {
        for (int i = start; i < end; i++) {
          Block b = blocks.get(i);
          blocksMap.removeINode(b);
          corruptReplicas.removeFromCorruptReplicasMap(b);
          // Remove the block from pendingReplications
          if (pendingReplications != null) {
            pendingReplications.remove(b);
          }
          addToInvalidates(b);
        }
      }
      start = end;
    }
  }

  void removePathAndBlocks(String src, List<Block> blocks) {
    leaseManager.removeLeaseWithPrefixPath(src);
    if (blocks == null) {
      return;
    }
    removeBlocks(blocks);
  }

  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system 
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getFileInfo(String src) throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isPermissionEnabled) {
        checkTraverse(pc, src);
      }
      return dir.getFileInfo(src);
    }
  }

  /**
   * Create all the necessary directories
   */
  public boolean mkdirs(String src, PermissionStatus permissions
      ) throws IOException {
    boolean status = mkdirsInternal(src, permissions);
    getEditLog().logSync();
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = dir.getFileInfo(src);
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    Server.getRemoteIp(),
                    "mkdirs", src, null, stat);
    }
    return status;
  }
    
  /**
   * Create all the necessary directories
   */
  private boolean mkdirsInternal(String src,
      PermissionStatus permissions) throws IOException {
    NameNode.stateChangeLog.debug("DIR* mkdirs: " + src);
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isPermissionEnabled) {
        checkTraverse(pc, src);
      }
      if (dir.isDir(src)) {
        // all the users of mkdirs() are used to expect 'true' even if
        // a new directory is not created.
        return true;
      }
      if (isInSafeMode())
        throw new SafeModeException("Cannot create directory " + src, safeMode);
      if (!DFSUtil.isValidName(src)) {
        throw new IOException("Invalid directory name: " + src);
      }
      if (isPermissionEnabled) {
        checkAncestorAccess(pc, src, FsAction.WRITE);
      }

      // validate that we have enough inodes. This is, at best, a
      // heuristic because the mkdirs() operation migth need to
      // create multiple inodes.
      checkFsObjectLimit();

      if (!dir.mkdirs(src, permissions, false, now())) {
        throw new IOException("Invalid directory name: " + src);
      }
      return true;
    }
  }

  ContentSummary getContentSummary(String src) throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isPermissionEnabled) {
        checkPermission(pc, src, false, null, null, null, FsAction.READ_EXECUTE);
      }
      return dir.getContentSummary(src);
    }
  }

  /**
   * Set the namespace quota and diskspace quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long)} for the 
   * contract.
   */
  void setQuota(String path, long nsQuota, long dsQuota) throws IOException {
    checkSuperuserPrivilege();
    synchronized (this) {
      if (isInSafeMode())
        throw new SafeModeException("Cannot set quota on " + path, safeMode); 
    
      dir.setQuota(path, nsQuota, dsQuota);
    }
    getEditLog().logSync();
  }
  
  /** Persist all metadata about this file.
   * @param src The string representation of the path
   * @param clientName The string representation of the client
   * @throws IOException if path does not exist
   */
  void fsync(String src, String clientName) throws IOException {

    NameNode.stateChangeLog.info("BLOCK* fsync: "
                                  + src + " for " + clientName);
    synchronized (this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Cannot fsync " + src, safeMode);
      }
      INodeFileUnderConstruction pendingFile  = checkLease(src, clientName);
      dir.persistBlocks(src, pendingFile);
    }
    getEditLog().logSync();
  }

  /**
   * This is invoked when a lease expires. On lease expiry, 
   * all the files that were written from that dfsclient should be
   * recovered.
   */
  void internalReleaseLease(Lease lease, String src) throws IOException {
    if (lease.hasPath()) {
      // make a copy of the paths because internalReleaseLeaseOne removes
      // pathnames from the lease record.
      String[] leasePaths = new String[lease.getPaths().size()];
      lease.getPaths().toArray(leasePaths);
      for (String p: leasePaths) {
        internalReleaseLeaseOne(lease, p);
      }
    } else {
      internalReleaseLeaseOne(lease, src);
    }
  }

  /**
   * Move a file that is being written to be immutable.
   * @param src The filename
   * @param lease The lease for the client creating the file
   */
  void internalReleaseLeaseOne(Lease lease, String src) throws IOException {
    assert Thread.holdsLock(this);

    LOG.info("Recovering lease=" + lease + ", src=" + src);

    INodeFile iFile = dir.getFileINode(src);
    if (iFile == null) {
      final String message = "DIR* internalReleaseCreate: "
        + "attempt to release a create lock on "
        + src + " file does not exist";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    if (!iFile.isUnderConstruction()) {
      final String message = "DIR* internalReleaseCreate: "
        + "attempt to release a create lock on "
        + src + " but file is already closed";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) iFile;

    // Initialize lease recovery for pendingFile. If there are no blocks 
    // associated with this file, then reap lease immediately. Otherwise 
    // renew the lease and trigger lease recovery.
    if (pendingFile.getTargets() == null ||
        pendingFile.getTargets().length == 0) {
      if (pendingFile.getBlocks().length == 0) {
        finalizeINodeFileUnderConstruction(src, pendingFile);
        NameNode.stateChangeLog.warn("BLOCK*"
          + " internalReleaseLease: No blocks found, lease removed for " +  src);
        return;
      }
      // setup the Inode.targets for the last block from the blocksMap
      //
      Block[] blocks = pendingFile.getBlocks();
      Block last = blocks[blocks.length-1];
      DatanodeDescriptor[] targets = 
         new DatanodeDescriptor[blocksMap.numNodes(last)];
      Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(last);
      for (int i = 0; it != null && it.hasNext(); i++) {
        targets[i] = it.next();
      }
      pendingFile.setTargets(targets);
    }
    // start lease recovery of the last block for this file.
    pendingFile.assignPrimaryDatanode();
    Lease reassignedLease = reassignLease(
      lease, src, HdfsConstants.NN_RECOVERY_LEASEHOLDER, pendingFile);
    leaseManager.renewLease(reassignedLease);
  }

  private Lease reassignLease(Lease lease, String src, String newHolder,
                      INodeFileUnderConstruction pendingFile) {
    if(newHolder == null)
      return lease;
    pendingFile.setClientName(newHolder);
    return leaseManager.reassignLease(lease, src, newHolder);
  }


  private void finalizeINodeFileUnderConstruction(String src,
      INodeFileUnderConstruction pendingFile) throws IOException {
    NameNode.stateChangeLog.info("Removing lease on  " + src + 
                                 " from client " + pendingFile.clientName);
    leaseManager.removeLease(pendingFile.clientName, src);

    // The file is no longer pending.
    // Create permanent INode, update blockmap
    INodeFile newFile = pendingFile.convertToInodeFile();
    dir.replaceNode(src, pendingFile, newFile);

    // close file and persist block allocations for this file
    dir.closeFile(src, newFile);

    checkReplicationFactor(newFile);
  }

  public void commitBlockSynchronization(Block lastblock,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException {
    LOG.info("commitBlockSynchronization(lastblock=" + lastblock
          + ", newgenerationstamp=" + newgenerationstamp
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets)
          + ", closeFile=" + closeFile
          + ", deleteBlock=" + deleteblock
          + ")");
    String src = null;
    try {
      synchronized (this) {
        if (isInSafeMode()) {
          throw new SafeModeException("Cannot commitBlockSynchronization " 
                                      + lastblock, safeMode);
        }
        final BlockInfo oldblockinfo = blocksMap.getStoredBlock(lastblock);
        if (oldblockinfo == null) {
          throw new IOException("Block (=" + lastblock + ") not found");
        }
        INodeFile iFile = oldblockinfo.getINode();
        if (!iFile.isUnderConstruction()) {
          throw new IOException("Unexpected block (=" + lastblock
              + ") since the file (=" + iFile.getLocalName()
              + ") is not under construction");
        }
        INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction)iFile;
    
    
        // Remove old block from blocks map. This always have to be done
        // because the generation stamp of this block is changing.
        blocksMap.removeBlock(oldblockinfo);
    
        if (deleteblock) {
          pendingFile.removeBlock(lastblock);
        } else {
          // update last block, construct newblockinfo and add it to the blocks map
          lastblock.set(lastblock.getBlockId(), newlength, newgenerationstamp);
          final BlockInfo newblockinfo = blocksMap.addINode(lastblock, pendingFile);
    
          // find the DatanodeDescriptor objects
          // There should be no locations in the blocksMap till now because the
          // file is underConstruction
          DatanodeDescriptor[] descriptors = null;
          List<DatanodeDescriptor> descriptorsList =
            new ArrayList<DatanodeDescriptor>(newtargets.length);
          for(int i = 0; i < newtargets.length; i++) {
            DatanodeDescriptor node =
              datanodeMap.get(newtargets[i].getStorageID());
            if (node != null) {
              if (closeFile) {
                // If we aren't closing the file, we shouldn't add it to the
                // block list for the node, since the block is still under
                // construction there. (in getAdditionalBlock, for example
                // we don't add to the block map for the targets)
                node.addBlock(newblockinfo);
              }
              descriptorsList.add(node);
            } else {
              LOG.error("commitBlockSynchronization included a target DN " +
                newtargets[i] + " which is not known to NN. Ignoring");
            }
          }
          if (!descriptorsList.isEmpty()) {
            descriptors = descriptorsList.toArray(new DatanodeDescriptor[0]);
          }
          // add locations into the INodeUnderConstruction
          pendingFile.setLastBlock(newblockinfo, descriptors);
        }
    
        // If this commit does not want to close the file, persist
        // blocks (if durable sync is enabled) and return
        src = leaseManager.findPath(pendingFile);
        if (!closeFile) {
          if (durableSync) {
            dir.persistBlocks(src, pendingFile);
          }
          LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
          return;
        }
        
        //remove lease, close file
        finalizeINodeFileUnderConstruction(src, pendingFile);
      } // end of synchronized section
    } finally {
      if (closeFile || durableSync) {
        getEditLog().logSync();
      }
    }
    if (closeFile) {
      LOG.info("commitBlockSynchronization(newblock=" + lastblock
          + ", file=" + src
          + ", newgenerationstamp=" + newgenerationstamp
          + ", newlength=" + newlength
          + ", newtargets=" + Arrays.asList(newtargets) + ") successful");
    }
  }


  /**
   * Renew the lease(s) held by the given client
   */
  synchronized void renewLease(String holder) throws IOException {
    if (isInSafeMode())
      throw new SafeModeException("Cannot renew lease for " + holder, safeMode);
    leaseManager.renewLease(holder);
  }

  /**
   * Get a partial listing of the indicated directory
   * 
   * @param src the directory name
   * @param startAfter the name to start after
   * @return a partial listing starting after startAfter 
   */
  public DirectoryListing getListing(String src, byte[] startAfter)
  throws IOException {
    FSPermissionChecker pc = getPermissionChecker();
    synchronized (this) {
      if (isPermissionEnabled) {
        if (dir.isDir(src)) {
          checkPathAccess(pc, src, FsAction.READ_EXECUTE);
        } else {
          checkTraverse(pc, src);
        }
      }
      if (auditLog.isInfoEnabled() && isExternalInvocation()) {
        logAuditEvent(UserGroupInformation.getCurrentUser(),
            Server.getRemoteIp(), "listStatus", src, null, null);
      }
      return dir.getListing(src, startAfter);
    }
  }

  /////////////////////////////////////////////////////////
  //
  // These methods are called by datanodes
  //
  /////////////////////////////////////////////////////////
  /**
   * Register Datanode.
   * <p>
   * The purpose of registration is to identify whether the new datanode
   * serves a new data storage, and will report new data block copies,
   * which the namenode was not aware of; or the datanode is a replacement
   * node for the data storage that was previously served by a different
   * or the same (in terms of host:port) datanode.
   * The data storages are distinguished by their storageIDs. When a new
   * data storage is reported the namenode issues a new unique storageID.
   * <p>
   * Finally, the namenode returns its namespaceID as the registrationID
   * for the datanodes. 
   * namespaceID is a persistent attribute of the name space.
   * The registrationID is checked every time the datanode is communicating
   * with the namenode. 
   * Datanodes with inappropriate registrationID are rejected.
   * If the namenode stops, and then restarts it can restore its 
   * namespaceID and will continue serving the datanodes that has previously
   * registered with the namenode without restarting the whole cluster.
   * 
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode#register()
   */
  public synchronized void registerDatanode(DatanodeRegistration nodeReg
                                            ) throws IOException {
    String dnAddress = Server.getRemoteAddress();
    if (dnAddress == null) {
      // Mostly called inside an RPC.
      // But if not, use address passed by the data-node.
      dnAddress = nodeReg.getHost();
    }      

    // check if the datanode is allowed to be connect to the namenode
    if (!verifyNodeRegistration(nodeReg, dnAddress)) {
      throw new DisallowedDatanodeException(nodeReg);
    }

    String hostName = nodeReg.getHost();
      
    // update the datanode's name with ip:port
    DatanodeID dnReg = new DatanodeID(dnAddress + ":" + nodeReg.getPort(),
                                      nodeReg.getStorageID(),
                                      nodeReg.getInfoPort(),
                                      nodeReg.getIpcPort());
    nodeReg.updateRegInfo(dnReg);
    nodeReg.exportedKeys = getBlockKeys();
      
    NameNode.stateChangeLog.info(
                                 "BLOCK* registerDatanode: "
                                 + "node registration from " + nodeReg.getName()
                                 + " storage " + nodeReg.getStorageID());

    DatanodeDescriptor nodeS = datanodeMap.get(nodeReg.getStorageID());
    DatanodeDescriptor nodeN = host2DataNodeMap.getDatanodeByName(nodeReg.getName());
      
    if (nodeN != null && nodeN != nodeS) {
      NameNode.LOG.info("BLOCK* registerDatanode: "
                        + "node from name: " + nodeN.getName());
      // nodeN previously served a different data storage, 
      // which is not served by anybody anymore.
      removeDatanode(nodeN);
      // physically remove node from datanodeMap
      wipeDatanode(nodeN);
      nodeN = null;
    }

    if (nodeS != null) {
      if (nodeN == nodeS) {
        // The same datanode has been just restarted to serve the same data 
        // storage. We do not need to remove old data blocks, the delta will
        // be calculated on the next block report from the datanode
        NameNode.stateChangeLog.debug("BLOCK* registerDatanode: "
                                      + "node restarted");
      } else {
        // nodeS is found
        /* The registering datanode is a replacement node for the existing 
          data storage, which from now on will be served by a new node.
          If this message repeats, both nodes might have same storageID 
          by (insanely rare) random chance. User needs to restart one of the
          nodes with its data cleared (or user can just remove the StorageID
          value in "VERSION" file under the data directory of the datanode,
          but this is might not work if VERSION file format has changed 
       */        
        NameNode.stateChangeLog.info( "BLOCK* registerDatanode: "
                                      + "node " + nodeS.getName()
                                      + " is replaced by " + nodeReg.getName() + 
                                      " with the same storageID " +
                                      nodeReg.getStorageID());
      }
      // update cluster map
      clusterMap.remove(nodeS);
      nodeS.updateRegInfo(nodeReg);
      nodeS.setHostName(hostName);
      
      // resolve network location
      resolveNetworkLocation(nodeS);
      clusterMap.add(nodeS);
        
      // also treat the registration message as a heartbeat
      synchronized(heartbeats) {
        if( !heartbeats.contains(nodeS)) {
          heartbeats.add(nodeS);
          //update its timestamp
          nodeS.updateHeartbeat(0L, 0L, 0L, 0);
          nodeS.isAlive = true;
        }
      }
      return;
    } 

    // this is a new datanode serving a new data storage
    if (nodeReg.getStorageID().equals("")) {
      // this data storage has never been registered
      // it is either empty or was created by pre-storageID version of DFS
      nodeReg.storageID = newStorageID();
      NameNode.stateChangeLog.debug(
                                    "BLOCK* registerDatanode: "
                                    + "new storageID " + nodeReg.getStorageID() + " assigned");
    }
    // register new datanode
    DatanodeDescriptor nodeDescr 
      = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK, hostName);
    resolveNetworkLocation(nodeDescr);
    unprotectedAddDatanode(nodeDescr);
    clusterMap.add(nodeDescr);
      
    // also treat the registration message as a heartbeat
    synchronized(heartbeats) {
      heartbeats.add(nodeDescr);
      nodeDescr.isAlive = true;
      // no need to update its timestamp
      // because its is done when the descriptor is created
    }

    if (safeMode != null) {
      safeMode.checkMode();
    }
    return;
  }
    
  /* Resolve a node's network location */
  private void resolveNetworkLocation (DatanodeDescriptor node) {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      // get the node's IP address
      names.add(node.getHost());
    } else {
      // get the node's host name
      String hostName = node.getHostName();
      int colon = hostName.indexOf(":");
      hostName = (colon==-1)?hostName:hostName.substring(0,colon);
      names.add(hostName);
    }
    
    // resolve its network location
    List<String> rName = dnsToSwitchMapping.resolve(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null! Using " + 
          NetworkTopology.DEFAULT_RACK + " for host " + names);
      networkLocation = NetworkTopology.DEFAULT_RACK;
    } else {
      networkLocation = rName.get(0);
    }
    node.setNetworkLocation(networkLocation);
  }
  
  /**
   * Get registrationID for datanodes based on the namespaceID.
   * 
   * @see #registerDatanode(DatanodeRegistration)
   * @see FSImage#newNamespaceID()
   * @return registration ID
   */
  public String getRegistrationID() {
    return Storage.getRegistrationID(dir.fsImage);
  }
    
  /**
   * Generate new storage ID.
   * 
   * @return unique storage ID
   * 
   * Note: that collisions are still possible if somebody will try 
   * to bring in a data storage from a different cluster.
   */
  private String newStorageID() {
    String newID = null;
    while(newID == null) {
      newID = "DS" + Integer.toString(r.nextInt());
      if (datanodeMap.get(newID) != null)
        newID = null;
    }
    return newID;
  }
    
  private boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdate() <
            (now() - heartbeatExpireInterval));
  }
    
  private void setDatanodeDead(DatanodeDescriptor node) {
    node.setLastUpdate(0);
  }

  /**
   * The given node has reported in.  This method should:
   * 1) Record the heartbeat, so the datanode isn't timed out
   * 2) Adjust usage stats for future block allocation
   * 
   * If a substantial amount of time passed since the last datanode 
   * heartbeat then request an immediate block report.  
   * 
   * @return an array of datanode commands 
   * @throws IOException
   */
  DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
      long capacity, long dfsUsed, long remaining,
      int xceiverCount, int xmitsInProgress) throws IOException {
    DatanodeCommand cmd = null;
    synchronized (heartbeats) {
      synchronized (datanodeMap) {
        DatanodeDescriptor nodeinfo = null;
        try {
          nodeinfo = getDatanode(nodeReg);
        } catch(UnregisteredDatanodeException e) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }
          
        // Check if this datanode should actually be shutdown instead. 
        if (nodeinfo != null && shouldNodeShutdown(nodeinfo)) {
          setDatanodeDead(nodeinfo);
          throw new DisallowedDatanodeException(nodeinfo);
        }

        if (nodeinfo == null || !nodeinfo.isAlive) {
          return new DatanodeCommand[]{DatanodeCommand.REGISTER};
        }

        updateStats(nodeinfo, false);
        nodeinfo.updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
        updateStats(nodeinfo, true);
        
        //check lease recovery
        cmd = nodeinfo.getLeaseRecoveryCommand(Integer.MAX_VALUE);
        if (cmd != null) {
          return new DatanodeCommand[] {cmd};
        }
      
        ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();
        //check pending replication
        cmd = nodeinfo.getReplicationCommand(
              maxReplicationStreams - xmitsInProgress);
        if (cmd != null) {
          cmds.add(cmd);
        }
        //check block invalidation
        cmd = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
        if (cmd != null) {
          cmds.add(cmd);
        }
        // check access key update
        if (isAccessTokenEnabled && nodeinfo.needKeyUpdate) {
          cmds.add(new KeyUpdateCommand(accessTokenHandler.exportKeys()));
          nodeinfo.needKeyUpdate = false;
        }
        // check for balancer bandwidth update
        if (nodeinfo.getBalancerBandwidth() > 0) {
          cmds.add(new BalancerBandwidthCommand(nodeinfo.getBalancerBandwidth()));
          // set back to 0 to indicate that datanode has been sent the new value
          nodeinfo.setBalancerBandwidth(0);
        }
        if (!cmds.isEmpty()) {
          return cmds.toArray(new DatanodeCommand[cmds.size()]);
        }
      }
    }

    //check distributed upgrade
    cmd = getDistributedUpgradeCommand();
    if (cmd != null) {
      return new DatanodeCommand[] {cmd};
    }
    return null;
  }

  private void updateStats(DatanodeDescriptor node, boolean isAdded) {
    //
    // The statistics are protected by the heartbeat lock
    //
    assert(Thread.holdsLock(heartbeats));
    if (isAdded) {
      capacityTotal += node.getCapacity();
      capacityUsed += node.getDfsUsed();
      capacityRemaining += node.getRemaining();
      totalLoad += node.getXceiverCount();
    } else {
      capacityTotal -= node.getCapacity();
      capacityUsed -= node.getDfsUsed();
      capacityRemaining -= node.getRemaining();
      totalLoad -= node.getXceiverCount();
    }
  }

  /**
   * Update access keys.
   */
  void updateAccessKey() throws IOException {
    this.accessTokenHandler.updateKeys();
    synchronized (heartbeats) {
      for (DatanodeDescriptor nodeInfo : heartbeats) {
        nodeInfo.needKeyUpdate = true;
      }
    }
  }

  /**
   * Periodically calls heartbeatCheck() and updateAccessKey()
   */
  class HeartbeatMonitor implements Runnable {
    private long lastHeartbeatCheck;
    private long lastAccessKeyUpdate;
    /**
     */
    public void run() {
      while (fsRunning) {
        try {
          long now = now();
          if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
            heartbeatCheck();
            lastHeartbeatCheck = now;
          }
          if (isAccessTokenEnabled && (lastAccessKeyUpdate + accessKeyUpdateInterval < now)) {
            updateAccessKey();
            lastAccessKeyUpdate = now;
          }
        } catch (Exception e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(5000);  // 5 seconds
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Periodically calls computeReplicationWork().
   */
  class ReplicationMonitor implements Runnable {
    ReplicateQueueProcessingStats replicateQueueStats = 
        new ReplicateQueueProcessingStats();
    InvalidateQueueProcessingStats invalidateQueueStats = 
        new InvalidateQueueProcessingStats();
    
    public void run() {
      while (fsRunning) {
        try {
          computeDatanodeWork();
          processPendingReplications();
          Thread.sleep(replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("ReplicationMonitor thread received InterruptedException" + ie);
          break;
        } catch (IOException ie) {
          LOG.warn("ReplicationMonitor thread received exception. " + ie +  " " +
                   StringUtils.stringifyException(ie));
        } catch (Throwable t) {
          LOG.warn("ReplicationMonitor thread received Runtime exception. " + t + " " +
                   StringUtils.stringifyException(t));
          Runtime.getRuntime().exit(-1);
        }
      }
    }

    /**
     * Just before exiting safe mode, {@link SafeModeInfo.leave()} scans all 
     * blocks and identifies under-replicated and invalid blocks.  These are 
     * placed in work queues.  Immediately after leaving safe mode,
     * {@link ReplicationMonitor} starts processing these queues via calls to
     * {@link #computeDatanodeWork()}.  Each call does a chunk of work, then
     * waits 3 seconds before doing the next chunk of work, to avoid monopolizing
     * the CPUs and the global lock.  It may take several cycles before the
     * queue is completely flushed.
     * 
     * Here we use two concrete subclasses of {@link QueueProcessingStatistics}
     * to collect stats about these processes.
     */
    private class ReplicateQueueProcessingStats 
    extends QueueProcessingStatistics {
      
      ReplicateQueueProcessingStats() {
        super("ReplicateQueue", "blocks", FSNamesystem.LOG);
      }
      
      // @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem.QueueProcessingStatistics#preCheckIsLastCycle(int)
      @Override
      public boolean preCheckIsLastCycle(int maxWorkToProcess) {
        return (maxWorkToProcess >= neededReplications.size());
      }
  
      // @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem.QueueProcessingStatistics#postCheckIsLastCycle(int)
      @Override
      public boolean postCheckIsLastCycle(int workFound) {
        return false;
      }
    }
  
    private class InvalidateQueueProcessingStats 
    extends QueueProcessingStatistics {
      
      InvalidateQueueProcessingStats() {
        super("InvalidateQueue", "blocks", FSNamesystem.LOG);
      }
  
      // @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem.QueueProcessingStatistics#preCheckIsLastCycle(int)
      @Override
      public boolean preCheckIsLastCycle(int maxWorkToProcess) {
        return false;
      }
  
      // @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem.QueueProcessingStatistics#postCheckIsLastCycle(int)
      @Override
      public boolean postCheckIsLastCycle(int workFound) {
        return recentInvalidateSets.isEmpty();
      }
    }
  }
  
  /////////////////////////////////////////////////////////
  //
  // These methods are called by the Namenode system, to see
  // if there is any work for registered datanodes.
  //
  /////////////////////////////////////////////////////////
  /**
   * Compute block replication and block invalidation work 
   * that can be scheduled on data-nodes.
   * The datanode will be informed of this work at the next heartbeat.
   * 
   * @return number of blocks scheduled for replication or removal.
   */
  public int computeDatanodeWork() throws IOException {
    int replicationWorkFound = 0;
    int invalidationWorkFound = 0;
    int blocksToProcess = 0;
    int nodesToProcess = 0;

    // blocks should not be replicated or removed if safe mode is on
    if (isInSafeMode()) {
      replmon.replicateQueueStats.checkRestart();
      replmon.invalidateQueueStats.checkRestart();
      return 0;
    }
    synchronized(heartbeats) {
      blocksToProcess = (int)(heartbeats.size() 
          * this.blocksReplWorkMultiplier);
      nodesToProcess = (int)Math.ceil((double)heartbeats.size() 
          * this.blocksInvalidateWorkPct);
    }

    replmon.replicateQueueStats.startCycle(blocksToProcess);
    replicationWorkFound = computeReplicationWork(blocksToProcess);
    replmon.replicateQueueStats.endCycle(replicationWorkFound);
    
    // Update FSNamesystemMetrics counters
    synchronized (this) {
      pendingReplicationBlocksCount = pendingReplications.size();
      underReplicatedBlocksCount = neededReplications.size();
      scheduledReplicationBlocksCount = replicationWorkFound;
      corruptReplicaBlocksCount = corruptReplicas.size();
    }
    
    replmon.invalidateQueueStats.startCycle(nodesToProcess);
    invalidationWorkFound = computeInvalidateWork(nodesToProcess);
    replmon.invalidateQueueStats.endCycle(invalidationWorkFound);

    return replicationWorkFound + invalidationWorkFound;
  }

  /**
   * Schedule blocks for deletion at datanodes
   * @param nodesToProcess number of datanodes to schedule deletion work
   * @return total number of block for deletion
   */
  int computeInvalidateWork(int nodesToProcess) {
    int numOfNodes = 0;
    ArrayList<String> keyArray;
    synchronized (this) {
      numOfNodes = recentInvalidateSets.size();
      // get an array of the keys
      keyArray = new ArrayList<String>(recentInvalidateSets.keySet());
    }
    nodesToProcess = Math.min(numOfNodes, nodesToProcess);

    // randomly pick up <i>nodesToProcess</i> nodes 
    // and put them at [0, nodesToProcess)
    int remainingNodes = numOfNodes - nodesToProcess;
    if (nodesToProcess < remainingNodes) {
      for(int i=0; i<nodesToProcess; i++) {
        int keyIndex = r.nextInt(numOfNodes-i)+i;
        Collections.swap(keyArray, keyIndex, i); // swap to front
      }
    } else {
      for(int i=0; i<remainingNodes; i++) {
        int keyIndex = r.nextInt(numOfNodes-i);
        Collections.swap(keyArray, keyIndex, numOfNodes-i-1); // swap to end
      }
    }
    
    int blockCnt = 0;
    for(int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++ ) {
      blockCnt += invalidateWorkForOneNode(keyArray.get(nodeCnt));
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReplications} and assign replication
   * work to data-nodes they belong to. 
   * 
   * The number of process blocks equals either twice the number of live 
   * data-nodes or the number of under-replicated blocks whichever is less.
   * 
   * @return number of blocks scheduled for replication during this iteration.
   */
  private int computeReplicationWork(
                                  int blocksToProcess) throws IOException {
    // stall only useful for unit tests (see TestFileAppend4.java)
    if (stallReplicationWork)  {
      return 0;
    }
    
    // Choose the blocks to be replicated
    List<List<Block>> blocksToReplicate = 
      chooseUnderReplicatedBlocks(blocksToProcess);

    // replicate blocks
    int scheduledReplicationCount = 0;
    for (int i=0; i<blocksToReplicate.size(); i++) {
      for(Block block : blocksToReplicate.get(i)) {
        if (computeReplicationWorkForBlock(block, i)) {
          scheduledReplicationCount++;
        }
      }
    }
    return scheduledReplicationCount;
  }
  
  /** Get a list of block lists to be replicated
   * The index of block lists represents the 
   * 
   * @param blocksToProcess
   * @return Return a list of block lists to be replicated. 
   *         The block list index represents its replication priority.
   */
  synchronized List<List<Block>> chooseUnderReplicatedBlocks(int blocksToProcess) {
    // initialize data structure for the return value
    List<List<Block>> blocksToReplicate = 
      new ArrayList<List<Block>>(UnderReplicatedBlocks.LEVEL);
    for (int i=0; i<UnderReplicatedBlocks.LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<Block>());
    }
    
    synchronized(neededReplications) {
      if (neededReplications.size() == 0) {
        missingBlocksInCurIter = 0;
        missingBlocksInPrevIter = 0;
        return blocksToReplicate;
      }
      
      // Go through all blocks that need replications.
      BlockIterator neededReplicationsIterator = neededReplications.iterator();
      // skip to the first unprocessed block, which is at replIndex 
      for(int i=0; i < replIndex && neededReplicationsIterator.hasNext(); i++) {
        neededReplicationsIterator.next();
      }
      // # of blocks to process equals either twice the number of live 
      // data-nodes or the number of under-replicated blocks whichever is less
      blocksToProcess = Math.min(blocksToProcess, neededReplications.size());

      for (int blkCnt = 0; blkCnt < blocksToProcess; blkCnt++, replIndex++) {
        if( ! neededReplicationsIterator.hasNext()) {
          // start from the beginning
          replIndex = 0;
          missingBlocksInPrevIter = missingBlocksInCurIter;
          missingBlocksInCurIter = 0;
          blocksToProcess = Math.min(blocksToProcess, neededReplications.size());
          if(blkCnt >= blocksToProcess)
            break;
          neededReplicationsIterator = neededReplications.iterator();
          assert neededReplicationsIterator.hasNext() : 
                                  "neededReplications should not be empty";
        }

        Block block = neededReplicationsIterator.next();
        int priority = neededReplicationsIterator.getPriority();
        if (priority < 0 || priority >= blocksToReplicate.size()) {
          LOG.warn("Unexpected replication priority: " + priority + " " + block);
        } else {
          blocksToReplicate.get(priority).add(block);
        }
      } // end for
    } // end synchronized
    return blocksToReplicate;
 }
  
  /** Replicate a block
   * 
   * @param block block to be replicated
   * @param priority a hint of its priority in the neededReplication queue
   * @return if the block gets replicated or not
   */
  boolean computeReplicationWorkForBlock(Block block, int priority) {
    int requiredReplication, numEffectiveReplicas; 
    List<DatanodeDescriptor> containingNodes;
    DatanodeDescriptor srcNode;
    
    INodeFile fileINode = null;
    synchronized (this) {
      synchronized (neededReplications) {
        // block should belong to a file
        fileINode = blocksMap.getINode(block);
        // abandoned block or block reopened for append
        if(fileINode == null || fileINode.isUnderConstruction()) { 
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          return false;
        }
        requiredReplication = fileINode.getReplication(); 

        // get a source data-node
        containingNodes = new ArrayList<DatanodeDescriptor>();
        NumberReplicas numReplicas = new NumberReplicas();
        srcNode = chooseSourceDatanode(block, containingNodes, numReplicas);
        if ((numReplicas.liveReplicas() + numReplicas.decommissionedReplicas())
            <= 0) {          
          missingBlocksInCurIter++;
        }
        if(srcNode == null) // block can not be replicated from any node
          return false;

        // do not schedule more if enough replicas is already pending
        numEffectiveReplicas = numReplicas.liveReplicas() +
                                pendingReplications.getNumReplicas(block);
        if(numEffectiveReplicas >= requiredReplication) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          NameNode.stateChangeLog.info("BLOCK* Removing " + block
              + " from neededReplications as it has enough replicas");
          return false;
        }
      }
    }

    // choose replication targets: NOT HOLDING THE GLOBAL LOCK
    // It is costly to extract the filename for which chooseTargets is called,
    // so for now we pass in the Inode itself.
    DatanodeDescriptor targets[] = replicator.chooseTarget(fileINode,
        requiredReplication - numEffectiveReplicas,
        srcNode, containingNodes, block.getNumBytes());
    if(targets.length == 0)
      return false;

    synchronized (this) {
      synchronized (neededReplications) {
        // Recheck since global lock was released
        // block should belong to a file
        fileINode = blocksMap.getINode(block);
        // abandoned block or block reopened for append
        if(fileINode == null || fileINode.isUnderConstruction()) { 
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          return false;
        }
        requiredReplication = fileINode.getReplication(); 

        // do not schedule more if enough replicas is already pending
        NumberReplicas numReplicas = countNodes(block);
        numEffectiveReplicas = numReplicas.liveReplicas() +
        pendingReplications.getNumReplicas(block);
        if(numEffectiveReplicas >= requiredReplication) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
          NameNode.stateChangeLog.info("BLOCK* Removing " + block
              + " from neededReplications as it has enough replicas");
          return false;
        } 

        // Add block to the to be replicated list
        srcNode.addBlockToBeReplicated(block, targets);

        for (DatanodeDescriptor dn : targets) {
          dn.incBlocksScheduled();
        }
        
        // Move the block-replication into a "pending" state.
        // The reason we use 'pending' is so we can retry
        // replications that fail after an appropriate amount of time.
        pendingReplications.increment(block, targets.length);
        NameNode.stateChangeLog.debug("BLOCK* " + block
            + " is moved from neededReplications to pendingReplications");

        // remove from neededReplications
        if(numEffectiveReplicas + targets.length >= requiredReplication) {
          neededReplications.remove(block, priority); // remove from neededReplications
          replIndex--;
        }
      }
    }
    
    if (NameNode.stateChangeLog.isInfoEnabled()) {
      StringBuilder targetList = new StringBuilder("datanode(s)");
      for (int k = 0; k < targets.length; k++) {
        targetList.append(' ');
        targetList.append(targets[k].getName());
      }
      NameNode.stateChangeLog.info( "BLOCK* ask " + srcNode.getName() + 
          " to replicate " + block + " to " + targetList);
      NameNode.stateChangeLog.debug(
                "BLOCK* neededReplications = " + neededReplications.size()
                + " pendingReplications = " + pendingReplications.size());
    }
    
    return true;
  }

  /** Choose a datanode near to the given address. */ 
  public DatanodeInfo chooseDatanode(String srcPath, String address, long blocksize) {
    final DatanodeDescriptor clientNode = host2DataNodeMap.getDatanodeByHost(
        address);
    if (clientNode != null) {
      HashMap<Node,Node> excludedNodes = null;
      final DatanodeDescriptor[] datanodes = replicator.chooseTarget(
          srcPath, 1, clientNode, excludedNodes, blocksize);
      if (datanodes.length > 0) {
        return datanodes[0];
      }
    }
    return null;
  }

  /**
   * Parse the data-nodes the block belongs to and choose one,
   * which will be the replication source.
   * 
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source.
   * Otherwise we choose a random node among those that did not reach their 
   * replication limit.
   * 
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   */
  private DatanodeDescriptor chooseSourceDatanode(
                                    Block block,
                                    List<DatanodeDescriptor> containingNodes,
                                    NumberReplicas numReplicas) {
    containingNodes.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    while(it.hasNext()) {
      DatanodeDescriptor node = it.next();
      Collection<Block> excessBlocks = 
        excessReplicateMap.get(node.getStorageID());
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node)))
        corrupt++;
      else if (node.isDecommissionInProgress() || node.isDecommissioned())
        decommissioned++;
      else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess++;
      } else {
        live++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node))
        continue;
      if(node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams)
        continue; // already reached replication limit
      // the block must not be scheduled for removal on srcNode
      if(excessBlocks != null && excessBlocks.contains(block))
        continue;
      // never use already decommissioned nodes
      if(node.isDecommissioned())
        continue;
      // we prefer nodes that are in DECOMMISSION_INPROGRESS state
      if(node.isDecommissionInProgress() || srcNode == null) {
        srcNode = node;
        continue;
      }
      if(srcNode.isDecommissionInProgress())
        continue;
      // switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if(r.nextBoolean())
        srcNode = node;
    }
    if(numReplicas != null)
      numReplicas.initialize(live, decommissioned, corrupt, excess);
    return srcNode;
  }

  /**
   * Get blocks to invalidate for <i>nodeId</i> 
   * in {@link #recentInvalidateSets}.
   * 
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode(String nodeId) {
    ArrayList<Block> blocksToInvalidate = 
        new ArrayList<Block>(blockInvalidateLimit);
    DatanodeDescriptor dn = null;
    
    synchronized (this) {
      // blocks should not be replicated or removed if safe mode is on
      if (isInSafeMode())
        return 0;
      // get blocks to invalidate for the nodeId
      assert nodeId != null;
      dn = datanodeMap.get(nodeId);
      if (dn == null) {
        recentInvalidateSets.remove(nodeId);
        return 0;
      }
      Collection<Block> invalidateSet = recentInvalidateSets.get(nodeId);
      if (invalidateSet == null) {
        return 0;
      }
  
      // # blocks that can be sent in one message is limited
      Iterator<Block> it = invalidateSet.iterator();
      for(int blkCount = 0; blkCount < blockInvalidateLimit && it.hasNext();
                                                                  blkCount++) {
        blocksToInvalidate.add(it.next());
        it.remove();
      }
  
      // If we send everything in this message, remove this node entry
      if (!it.hasNext()) {
        recentInvalidateSets.remove(nodeId);
      }
  
      dn.addBlocksToBeInvalidated(blocksToInvalidate);
      pendingDeletionBlocksCount -= blocksToInvalidate.size();
    }

    if(NameNode.stateChangeLog.isInfoEnabled()) {
      StringBuilder blockList = new StringBuilder();
      for(Block blk : blocksToInvalidate) {
        blockList.append(' ');
        blockList.append(blk);
      }
      NameNode.stateChangeLog.info("BLOCK* ask "
          + dn.getName() + " to delete " + blockList);
    }
    return blocksToInvalidate.size();
  }

  public void setNodeReplicationLimit(int limit) {
    this.maxReplicationStreams = limit;
  }

  /**
   * If there were any replication requests that timed out, reap them
   * and put them back into the neededReplication queue
   */
  void processPendingReplications() {
    Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      synchronized (this) {
        for (int i = 0; i < timedOutItems.length; i++) {
          NumberReplicas num = countNodes(timedOutItems[i]);
          neededReplications.add(timedOutItems[i], 
                                 num.liveReplicas(),
                                 num.decommissionedReplicas(),
                                 getReplication(timedOutItems[i]));
        }
      }
      /* If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }

  /**
   * remove a datanode descriptor
   * @param nodeID datanode ID
   */
  synchronized public void removeDatanode(DatanodeID nodeID) 
    throws IOException {
    DatanodeDescriptor nodeInfo = getDatanode(nodeID);
    if (nodeInfo != null) {
      removeDatanode(nodeInfo);
    } else {
      NameNode.stateChangeLog.warn("BLOCK* removeDatanode: "
                                   + nodeID.getName() + " does not exist");
    }
  }
  
  /**
   * remove a datanode descriptor
   * @param nodeInfo datanode descriptor
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo) {
    synchronized (heartbeats) {
      if (nodeInfo.isAlive) {
        updateStats(nodeInfo, false);
        heartbeats.remove(nodeInfo);
        nodeInfo.isAlive = false;
      }
    }

    for (Iterator<Block> it = nodeInfo.getBlockIterator(); it.hasNext();) {
      removeStoredBlock(it.next(), nodeInfo);
    }
    unprotectedRemoveDatanode(nodeInfo);
    clusterMap.remove(nodeInfo);

    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  void unprotectedRemoveDatanode(DatanodeDescriptor nodeDescr) {
    nodeDescr.resetBlocks();
    removeFromInvalidates(nodeDescr.getStorageID());
    NameNode.stateChangeLog.debug(
                                  "BLOCK* unprotectedRemoveDatanode: "
                                  + nodeDescr.getName() + " is out of service now");
  }
    
  void unprotectedAddDatanode(DatanodeDescriptor nodeDescr) {
    /* To keep host2DataNodeMap consistent with datanodeMap,
       remove  from host2DataNodeMap the datanodeDescriptor removed
       from datanodeMap before adding nodeDescr to host2DataNodeMap.
    */
    host2DataNodeMap.remove(
                            datanodeMap.put(nodeDescr.getStorageID(), nodeDescr));
    host2DataNodeMap.add(nodeDescr);
      
    NameNode.stateChangeLog.debug(
                                  "BLOCK* unprotectedAddDatanode: "
                                  + "node " + nodeDescr.getName() + " is added to datanodeMap");
  }

  /**
   * Physically remove node from datanodeMap.
   * 
   * @param nodeID node
   */
  void wipeDatanode(DatanodeID nodeID) throws IOException {
    String key = nodeID.getStorageID();
    host2DataNodeMap.remove(datanodeMap.remove(key));
    NameNode.stateChangeLog.debug(
                                  "BLOCK* wipeDatanode: "
                                  + nodeID.getName() + " storage " + key 
                                  + " is removed from datanodeMap");
  }

  FSImage getFSImage() {
    return dir.fsImage;
  }

  FSEditLog getEditLog() {
    return getFSImage().getEditLog();
  }

  /**
   * Check if there are any expired heartbeats, and if so,
   * whether any blocks have to be re-replicated.
   * While removing dead datanodes, make sure that only one datanode is marked
   * dead at a time within the synchronized section. Otherwise, a cascading
   * effect causes more datanodes to be declared dead.
   */
  void heartbeatCheck() {
    if (isInSafeMode()) {
      // not to check dead nodes if in safemode
      return;
    }
    boolean allAlive = false;
    while (!allAlive) {
      boolean foundDead = false;
      DatanodeID dead = null;
      // check the number of stale nodes
      int numOfStaleNodes = 0;

      // locate the first dead node. If need to check stale nodes,
      // also count the number of stale nodes
      synchronized (heartbeats) {
        for (Iterator<DatanodeDescriptor> it = heartbeats.iterator(); it
            .hasNext();) {
          DatanodeDescriptor nodeInfo = it.next();
          if (dead == null && isDatanodeDead(nodeInfo)) {
            foundDead = true;
            dead = nodeInfo;
          }
          if (nodeInfo.isStale(this.staleInterval)) {
            numOfStaleNodes++;
          }
        }

        setNumStaleNodes(numOfStaleNodes);
      }

      // acquire the fsnamesystem lock, and then remove the dead node.
      if (foundDead) {
        synchronized (this) {
          synchronized(heartbeats) {
            synchronized (datanodeMap) {
              DatanodeDescriptor nodeInfo = null;
              try {
                nodeInfo = getDatanode(dead);
              } catch (IOException e) {
                nodeInfo = null;
              }
              if (nodeInfo != null && isDatanodeDead(nodeInfo)) {
                NameNode.stateChangeLog.info("BLOCK* heartbeatCheck: "
                                             + "lost heartbeat from " + nodeInfo.getName());
                removeDatanode(nodeInfo);
              }
            }
          }
        }
      }
      allAlive = !foundDead;
    }
  }
  
  /**
   * Log a rejection of an addStoredBlock RPC, invalidate the reported block,
   * and return it.
   */
  private Block rejectAddStoredBlock(Block block, DatanodeDescriptor node,
      String msg) {
    NameNode.stateChangeLog.info("BLOCK* addStoredBlock: "
        + "addStoredBlock request received for " + block + " on "
        + node.getName() + " size " + block.getNumBytes()
        + " but was rejected: " + msg);
    addToInvalidates(block, node);
    return block;
  }

  /**
   * It will update the targets for INodeFileUnderConstruction
   * 
   * @param nodeID
   *          - DataNode ID
   * @param blocksBeingWritten
   *          - list of blocks which are still inprogress.
   * @throws IOException
   */
  public synchronized void processBlocksBeingWrittenReport(DatanodeID nodeID,
      BlockListAsLongs blocksBeingWritten) throws IOException {
    DatanodeDescriptor dataNode = getDatanode(nodeID);
    if (dataNode == null) {
      throw new IOException("ProcessReport from unregistered node: "
          + nodeID.getName());
    }

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(dataNode)) {
      setDatanodeDead(dataNode);
      throw new DisallowedDatanodeException(dataNode);
    }

    Block block = new Block();

    for (int i = 0; i < blocksBeingWritten.getNumberOfBlocks(); i++) {
      block.set(blocksBeingWritten.getBlockId(i), blocksBeingWritten
          .getBlockLen(i), blocksBeingWritten.getBlockGenStamp(i));

      BlockInfo storedBlock = blocksMap.getStoredBlockWithoutMatchingGS(block);

      if (storedBlock == null) {
        rejectAddStoredBlock(new Block(block), dataNode,
            "Block not in blockMap with any generation stamp");
        continue;
      }

      INodeFile inode = storedBlock.getINode();
      if (inode == null) {
        rejectAddStoredBlock(new Block(block), dataNode,
            "Block does not correspond to any file");
        continue;
      }

      boolean underConstruction = inode.isUnderConstruction();
      boolean isLastBlock = inode.getLastBlock() != null
          && inode.getLastBlock().getBlockId() == block.getBlockId();

      // Must be the last block of a file under construction,
      if (!underConstruction) {
        rejectAddStoredBlock(new Block(block), dataNode,
            "Reported as block being written but is a block of closed file");
        continue;
      }

      if (!isLastBlock) {
        rejectAddStoredBlock(new Block(block), dataNode,
            "Reported as block being written but not the last block of "
                + "an under-construction file");
        continue;
      }

      INodeFileUnderConstruction pendingFile = (INodeFileUnderConstruction) inode;
      pendingFile.addTarget(dataNode);
      incrementSafeBlockCount(pendingFile.getTargets().length);
    }
  }
  
  /**
   * The given node is reporting all its blocks.  Use this info to 
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public synchronized void processReport(DatanodeID nodeID, 
                                         BlockListAsLongs newReport
                                        ) throws IOException {
    long startTime = now();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* processReport: "
                             + "from " + nodeID.getName()+" " + 
                             newReport.getNumberOfBlocks()+" blocks");
    }
    DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null || !node.isAlive) {
      throw new IOException("ProcessReport from dead or unregisterted node: "
                            + nodeID.getName());
    }

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }
    
    // To minimize startup time, we discard any second (or later) block reports
    // that we receive while still in startup phase.
    if (isInStartupSafeMode() && !node.firstBlockReport()) {
      NameNode.stateChangeLog.info("BLOCK* processReport: "
          + "discarded non-initial block report from " + nodeID.getName()
          + " because namenode still in startup phase");
      return;
    }

    //
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    Collection<Block> toAdd = new LinkedList<Block>();
    Collection<Block> toRemove = new LinkedList<Block>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    node.reportDiff(blocksMap, newReport, toAdd, toRemove, toInvalidate);
        
    for (Block b : toRemove) {
      removeStoredBlock(b, node);
    }
    for (Block b : toAdd) {
      addStoredBlock(b, node, null);
    }
    for (Block b : toInvalidate) {
      NameNode.stateChangeLog.info("BLOCK* processReport: " 
          + b + " on " + node.getName() + " size " + b.getNumBytes()
          + " does not belong to any file");
      addToInvalidates(b, node);
    }
    long endTime = now();
    NameNode.getNameNodeMetrics().addBlockReport(endTime - startTime);
    NameNode.stateChangeLog.info("*BLOCK* processReport: from "
        + nodeID.getName() + ", blocks: " + newReport.getNumberOfBlocks()
        + ", processing time: " + (endTime - startTime) + " msecs");
    node.processedBlockReport();
  }

  /**
   * Modify (block-->datanode) map.  Remove block from set of 
   * needed replications if this takes care of the problem.
   * @return the block that is stored in blockMap.
   */
  synchronized Block addStoredBlock(Block block, 
                                    DatanodeDescriptor node,
                                    DatanodeDescriptor delNodeHint) {
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      // If we have a block in the block map with the same ID, but a different
      // generation stamp, and the corresponding file is under construction,
      // then we need to do some special processing.
      storedBlock = blocksMap.getStoredBlockWithoutMatchingGS(block);

      if (storedBlock == null) {
        return rejectAddStoredBlock(
          block, node,
          "Block not in blockMap with any generation stamp");
      }

      INodeFile inode = storedBlock.getINode();
      if (inode == null) {
        return rejectAddStoredBlock(
          block, node,
          "Block does not correspond to any file");
      }

      boolean reportedOldGS = block.getGenerationStamp() < storedBlock.getGenerationStamp();
      boolean reportedNewGS = block.getGenerationStamp() > storedBlock.getGenerationStamp();
      boolean underConstruction = inode.isUnderConstruction();
      boolean isLastBlock = inode.getLastBlock() != null &&
        inode.getLastBlock().getBlockId() == block.getBlockId();

      // We can report a stale generation stamp for the last block under construction,
      // we just need to make sure it ends up in targets.
      if (reportedOldGS && !(underConstruction && isLastBlock)) {
        return rejectAddStoredBlock(
          block, node,
          "Reported block has old generation stamp but is not the last block of " +
          "an under-construction file. (current generation is " +
          storedBlock.getGenerationStamp() + ")");
      }

      // Don't add blocks to the DN when they're part of the in-progress last block
      // and have an inconsistent generation stamp. Instead just add them to targets
      // for recovery purposes. They will get added to the node when
      // commitBlockSynchronization runs
      if (underConstruction && isLastBlock && (reportedOldGS || reportedNewGS)) {
        NameNode.stateChangeLog.info("BLOCK* addStoredBlock: Targets updated: "
            + block + " on " + node.getName() + " is added as a target for " 
            + storedBlock + " with size " + block.getNumBytes());
        ((INodeFileUnderConstruction)inode).addTarget(node);
        return block;
      }
    }

    INodeFile fileINode = storedBlock.getINode();
    if (fileINode == null) {
      return rejectAddStoredBlock(
        block, node,
        "Block does not correspond to any file");
    }
    assert storedBlock != null : "Block must be stored by now";

    // add block to the data-node
    boolean added = node.addBlock(storedBlock);    


    // Is the block being reported the last block of an underconstruction file?
    boolean blockUnderConstruction = false;
    if (fileINode.isUnderConstruction()) {
      Block last = fileINode.getLastBlock();
      if (last == null) {
        // This should never happen, but better to handle it properly than to throw
        // an NPE below.
        LOG.error("Null blocks for reported =" + block + " stored=" + storedBlock +
          " inode=" + fileINode);
        return block;
      }
      blockUnderConstruction = last.equals(storedBlock);
    }

    // block == storedBlock when this addStoredBlock is the result of a block report
    if (block != storedBlock) {
      if (block.getNumBytes() >= 0) {
        long cursize = storedBlock.getNumBytes();
        INodeFile file = storedBlock.getINode();
        if (cursize == 0) {
          storedBlock.setNumBytes(block.getNumBytes());
        } else if (cursize != block.getNumBytes()) {
          LOG.warn("Inconsistent size for " + block + 
                   " reported from " + node.getName() + 
                   " current size is " + cursize +
                   " reported size is " + block.getNumBytes());
          try {
            if (cursize > block.getNumBytes() && !blockUnderConstruction) {
              // new replica is smaller in size than existing block.
              // Mark the new replica as corrupt.
              LOG.warn("Mark new replica " + block + " from " + node.getName() + 
                  "as corrupt because its length is shorter than existing ones");
              markBlockAsCorrupt(block, node);
            } else {
              // new replica is larger in size than existing block.
              if (!blockUnderConstruction) {
                // Mark pre-existing replicas as corrupt.
                int numNodes = blocksMap.numNodes(block);
                int count = 0;
                DatanodeDescriptor nodes[] = new DatanodeDescriptor[numNodes];
                Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block);
                for (; it != null && it.hasNext();) {
                  DatanodeDescriptor dd = it.next();
                  if (!dd.equals(node)) {
                    nodes[count++] = dd;
                  }
                }
                for (int j = 0; j < count; j++) {
                  LOG.warn("Mark existing replica "
                      + block
                      + " from "
                      + node.getName()
                      + " as corrupt because its length is shorter than the new one");
                  markBlockAsCorrupt(block, nodes[j]);
                }
              }
              //
              // change the size of block in blocksMap
              //
              storedBlock.setNumBytes(block.getNumBytes());
            }
          } catch (IOException e) {
            LOG.warn("Error in deleting bad " + block + e);
          }
        }
        
        //Updated space consumed if required.
        long diff = (file == null) ? 0 :
                    (file.getPreferredBlockSize() - storedBlock.getNumBytes());
        
        if (diff > 0 && file.isUnderConstruction() &&
            cursize < storedBlock.getNumBytes()) {
          try {
            String path = /* For finding parents */ 
              leaseManager.findPath((INodeFileUnderConstruction)file);
            dir.updateSpaceConsumed(path, 0, -diff*file.getReplication());
          } catch (IOException e) {
            LOG.warn("Unexpected exception while updating disk space : " +
                     e.getMessage());
          }
        }
      }
      block = storedBlock;
    }
    assert storedBlock == block : "Block must be stored by now";
        
    int curReplicaDelta = 0;
        
    if (added) {
      curReplicaDelta = 1;
      // 
      // At startup time, because too many new blocks come in
      // they take up lots of space in the log file. 
      // So, we log only when namenode is out of safemode.
      //
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("BLOCK* addStoredBlock: "
                                      +"blockMap updated: "+node.getName()+" is added to "+block+" size "+block.getNumBytes());
      }
    } else {
      NameNode.stateChangeLog.warn("BLOCK* addStoredBlock: " + 
          "Redundant addStoredBlock request received for " + block + " on " + 
          node.getName() + " size " + block.getNumBytes());
    }

    // filter out containingNodes that are marked for decommission.
    NumberReplicas num = countNodes(storedBlock);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica = numLiveReplicas
      + pendingReplications.getNumReplicas(block);

    // check whether safe replication is reached for the block
    incrementSafeBlockCount(numCurrentReplica);
 
    //
    // if file is being actively written to, then do not check 
    // replication-factor here. It will be checked when the file is closed.
    //
    if (blockUnderConstruction) {
      INodeFileUnderConstruction cons = (INodeFileUnderConstruction) fileINode;
      cons.addTarget(node);
      return block;
    }

    // do not handle mis-replicated blocks during startup
    if(isInSafeMode())
      return block;

    // handle underReplication/overReplication
    short fileReplication = fileINode.getReplication();
    if (numCurrentReplica >= fileReplication) {
      neededReplications.remove(block, numCurrentReplica, 
                                num.decommissionedReplicas, fileReplication);
    } else {
      updateNeededReplications(block, curReplicaDelta, 0);
    }
    if (numCurrentReplica > fileReplication) {
      processOverReplicatedBlock(block, fileReplication, node, delNodeHint);
    }
    // If the file replication has reached desired value
    // we can remove any corrupt replicas the block may have
    int corruptReplicasCount = corruptReplicas.numCorruptReplicas(block); 
    int numCorruptNodes = num.corruptReplicas();
    if ( numCorruptNodes != corruptReplicasCount) {
      LOG.warn("Inconsistent number of corrupt replicas for " + 
          block + "blockMap has " + numCorruptNodes + 
          " but corrupt replicas map has " + corruptReplicasCount);
    }
    if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication)) 
      invalidateCorruptReplicas(block);
    return block;
  }

  /**
   * Invalidate corrupt replicas.
   * <p>
   * This will remove the replicas from the block's location list,
   * add them to {@link #recentInvalidateSets} so that they could be further
   * deleted from the respective data-nodes,
   * and remove the block from corruptReplicasMap.
   * <p>
   * This method should be called when the block has sufficient
   * number of live replicas.
   *
   * @param blk Block whose corrupt replicas need to be invalidated
   */
  void invalidateCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean gotException = false;
    if (nodes == null)
      return;
    // Make a copy of this list, since calling invalidateBlock will modify
    // the original (avoid CME)
    nodes = new ArrayList<DatanodeDescriptor>(nodes);
    NameNode.stateChangeLog.debug("NameNode.invalidateCorruptReplicas: " +
        "invalidating corrupt replicas on " + nodes.size() + "nodes");
    for (Iterator<DatanodeDescriptor> it = nodes.iterator(); it.hasNext(); ) {
      DatanodeDescriptor node = it.next();
      try {
        invalidateBlock(blk, node);
      } catch (IOException e) {
        NameNode.stateChangeLog.info("NameNode.invalidateCorruptReplicas " +
                                      "error in deleting bad " + blk +
                                      " on " + node + e);
        gotException = true;
      }
    }
    // Remove the block from corruptReplicasMap
    if (!gotException)
      corruptReplicas.removeFromCorruptReplicasMap(blk);
  }

  /**
   * For each block in the name-node verify whether it belongs to any file,
   * over or under replicated. Place it into the respective queue.
   */
  private synchronized void processMisReplicatedBlocks() {
    long nrInvalid = 0, nrOverReplicated = 0, nrUnderReplicated = 0;
    neededReplications.clear();
    for(BlocksMap.BlockInfo block : blocksMap.getBlocks()) {
      INodeFile fileINode = block.getINode();
      if(fileINode == null) {
        // block does not belong to any file
        nrInvalid++;
        addToInvalidates(block);
        continue;
      }
      // calculate current replication
      short expectedReplication = fileINode.getReplication();
      NumberReplicas num = countNodes(block);
      int numCurrentReplica = num.liveReplicas();
      // add to under-replicated queue if need to be
      if (neededReplications.add(block, 
                                 numCurrentReplica,
                                 num.decommissionedReplicas(),
                                 expectedReplication)) {
        nrUnderReplicated++;
      }

      if (numCurrentReplica > expectedReplication) {
        // over-replicated block
        nrOverReplicated++;
        processOverReplicatedBlock(block, expectedReplication, null, null);
      }
    }
    LOG.info("Total number of blocks = " + blocksMap.size());
    LOG.info("Number of invalid blocks = " + nrInvalid);
    LOG.info("Number of under-replicated blocks = " + nrUnderReplicated);
    LOG.info("Number of  over-replicated blocks = " + nrOverReplicated);
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   */
  private void processOverReplicatedBlock(Block block, short replication, 
      DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {
    if(addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeDescriptor> nonExcess = new ArrayList<DatanodeDescriptor>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas.getNodes(block);
    for (Iterator<DatanodeDescriptor> it = blocksMap.nodeIterator(block); 
         it.hasNext();) {
      DatanodeDescriptor cur = it.next();
      Collection<Block> excessBlocks = excessReplicateMap.get(cur.getStorageID());
      if (excessBlocks == null || !excessBlocks.contains(block)) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) {
            nonExcess.add(cur);
          }
        }
      }
    }
    chooseExcessReplicates(nonExcess, block, replication, 
        addedNode, delNodeHint);    
  }

  /**
   * We want "replication" replicates for the block, but we now have too many.  
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
   *
   * srcNodes.size() - dstNodes.size() == replication
   *
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack. 
   * If no such a node is available,
   * then pick a node with least free space
   */
  void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess, 
                              Block b, short replication,
                              DatanodeDescriptor addedNode,
                              DatanodeDescriptor delNodeHint) {
    INodeFile inode = blocksMap.getINode(b);
    // first form a rack to datanodes map and
    final Map<String, List<DatanodeDescriptor>> rackMap = 
        new HashMap<String, List<DatanodeDescriptor>>();
    final List<DatanodeDescriptor> moreThanOne = new ArrayList<DatanodeDescriptor>();
    final List<DatanodeDescriptor> exactlyOne = new ArrayList<DatanodeDescriptor>();

    // split nodes into two sets
    // moreThanOne contains nodes on rack with more than one replica
    // exactlyOne contains the remaining nodes
    replicator.splitNodesWithRack(nonExcess, rackMap, moreThanOne, exactlyOne);
    
    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    while (nonExcess.size() - replication > 0) {
      DatanodeInfo cur = null;

      // check if we can del delNodeHint
      if (firstOne && delNodeHint !=null && nonExcess.contains(delNodeHint)
          && (moreThanOne.contains(delNodeHint)
                || (addedNode != null && !moreThanOne.contains(addedNode))) ) {
          cur = delNodeHint;
      } else { // regular excessive replica removal
        cur = replicator.chooseReplicaToDelete(inode, b, replication,
                       moreThanOne, exactlyOne);
      }

      firstOne = false;
      // adjust rackmap, moreThanOne, and exactlyOne
      replicator.adjustSetsWithChosenReplica(rackMap, moreThanOne,
          exactlyOne, cur);

      nonExcess.remove(cur);

      Collection<Block> excessBlocks = excessReplicateMap.get(cur.getStorageID());
      if (excessBlocks == null) {
        excessBlocks = new TreeSet<Block>();
        excessReplicateMap.put(cur.getStorageID(), excessBlocks);
      }
      if (excessBlocks.add(b)) {
        excessBlocksCount++;
        NameNode.stateChangeLog.debug("BLOCK* chooseExcessReplicates: "
                                      +"("+cur.getName()+", "+b
                                      +") is added to excessReplicateMap");
      }

      //
      // The 'excessblocks' tracks blocks until we get confirmation
      // that the datanode has deleted them; the only way we remove them
      // is when we get a "removeBlock" message.  
      //
      // The 'invalidate' list is used to inform the datanode the block 
      // should be deleted.  Items are removed from the invalidate list
      // upon giving instructions to the namenode.
      //
      addToInvalidatesNoLog(b, cur);
      NameNode.stateChangeLog.info("BLOCK* chooseExcessReplicates: "
                +"("+cur.getName()+", "+b+") is added to recentInvalidateSets");
    }
  }

  /**
   * Modify (block-->datanode) map.  Possibly generate 
   * replication tasks, if the removed block is still valid.
   */
  synchronized void removeStoredBlock(Block block, DatanodeDescriptor node) {
    NameNode.stateChangeLog.debug("BLOCK* removeStoredBlock: "
                                  +block + " from "+node.getName());
    if (!blocksMap.removeNode(block, node)) {
      NameNode.stateChangeLog.debug("BLOCK* removeStoredBlock: "
                                    +block+" has already been removed from node "+node);
      return;
    }
        
    //
    // It's possible that the block was removed because of a datanode
    // failure.  If the block is still valid, check if replication is
    // necessary.  In that case, put block on a possibly-will-
    // be-replicated list.
    //
    INode fileINode = blocksMap.getINode(block);
    if (fileINode != null) {
      decrementSafeBlockCount(block);
      updateNeededReplications(block, -1, 0);
    }

    //
    // We've removed a block from a node, so it's definitely no longer
    // in "excess" there.
    //
    Collection<Block> excessBlocks = excessReplicateMap.get(node.getStorageID());
    if (excessBlocks != null) {
      if (excessBlocks.remove(block)) {
        excessBlocksCount--;
        NameNode.stateChangeLog.debug("BLOCK* removeStoredBlock: "
            + block + " is removed from excessBlocks");
        if (excessBlocks.size() == 0) {
          excessReplicateMap.remove(node.getStorageID());
        }
      }
    }
    
    // Remove the replica from corruptReplicas
    corruptReplicas.removeFromCorruptReplicasMap(block, node);
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  public synchronized void blockReceived(DatanodeID nodeID,  
                                         Block block,
                                         String delHint
                                         ) throws IOException {
    DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null || !node.isAlive) {
      NameNode.stateChangeLog.warn("BLOCK* blockReceived: " + block
          + " is received from dead or unregistered node " + nodeID.getName());
      throw new IOException(
          "Got blockReceived message from unregistered or dead node " + block);
    }
        
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* blockReceived: "
                                    +block+" is received from " + nodeID.getName());
    }

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }

    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if(delHint!=null && delHint.length()!=0) {
      delHintNode = datanodeMap.get(delHint);
      if(delHintNode == null) {
        NameNode.stateChangeLog.warn("BLOCK* blockReceived: "
            + block
            + " is expected to be removed from an unrecorded node " 
            + delHint);
      }
    }

    //
    // Modify the blocks->datanode map and node's map.
    // 
    pendingReplications.decrement(block);
    addStoredBlock(block, node, delHintNode );
    
    // decrement number of blocks scheduled to this datanode.
    node.decBlocksScheduled();    
  }

  public long getMissingBlocksCount() {
    // not locking
    return Math.max(missingBlocksInPrevIter, missingBlocksInCurIter); 
  }
  
  long[] getStats() throws IOException {
    checkSuperuserPrivilege();
    synchronized(heartbeats) {
      return new long[] {this.capacityTotal, this.capacityUsed, 
                         this.capacityRemaining,
                         this.underReplicatedBlocksCount,
                         this.corruptReplicaBlocksCount,
                         getMissingBlocksCount()};
    }
  }

  /**
   * Total raw bytes including non-dfs used space.
   */
  public long getCapacityTotal() {
    synchronized (heartbeats) {
      return this.capacityTotal;
    }
  }

  /**
   * Total used space by data nodes
   */
  public long getCapacityUsed() {
    synchronized(heartbeats){
      return this.capacityUsed;
    }
  }
  /**
   * Total used space by data nodes as percentage of total capacity
   */
  public float getCapacityUsedPercent() {
    synchronized(heartbeats){
      if (capacityTotal <= 0) {
        return 100;
      }

      return ((float)capacityUsed * 100.0f)/(float)capacityTotal;
    }
  }
  /**
   * Total used space by data nodes for non DFS purposes such
   * as storing temporary files on the local file system
   */
  public long getCapacityUsedNonDFS() {
    long nonDFSUsed = 0;
    synchronized(heartbeats){
      nonDFSUsed = capacityTotal - capacityRemaining - capacityUsed;
    }
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }
  /**
   * Total non-used raw bytes.
   */
  public long getCapacityRemaining() {
    synchronized (heartbeats) {
      return this.capacityRemaining;
    }
  }

  /**
   * Total remaining space by data nodes as percentage of total capacity
   */
  public float getCapacityRemainingPercent() {
    synchronized(heartbeats){
      if (capacityTotal <= 0) {
        return 0;
      }

      return ((float)capacityRemaining * 100.0f)/(float)capacityTotal;
    }
  }
  /**
   * Total number of connections.
   */
  public int getTotalLoad() {
    synchronized (heartbeats) {
      return this.totalLoad;
    }
  }

  int getNumberOfDatanodes(DatanodeReportType type) {
    return getDatanodeListForReport(type).size(); 
  }

  public synchronized ArrayList<DatanodeDescriptor> getDatanodeListForReport(
                                                      DatanodeReportType type) {                  
    
    boolean listLiveNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.LIVE;
    boolean listDeadNodes = type == DatanodeReportType.ALL ||
                            type == DatanodeReportType.DEAD;

    HashMap<String, String> mustList = new HashMap<String, String>();
    
    if (listDeadNodes) {
      //first load all the nodes listed in include and exclude files.
      for (Iterator<String> it = hostsReader.getHosts().iterator(); 
           it.hasNext();) {
        mustList.put(it.next(), "");
      }
      for (Iterator<String> it = hostsReader.getExcludedHosts().iterator(); 
           it.hasNext();) {
        mustList.put(it.next(), "");
      }
    }
   
    ArrayList<DatanodeDescriptor> nodes = null;
    
    synchronized (datanodeMap) {
      nodes = new ArrayList<DatanodeDescriptor>(datanodeMap.size() + 
                                                mustList.size());
      
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
                                                               it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        boolean isDead = isDatanodeDead(dn);
        if ( (isDead && listDeadNodes) || (!isDead && listLiveNodes) ) {
          nodes.add(dn);
        }
        //Remove any form of the this datanode in include/exclude lists.
        mustList.remove(dn.getName());
        mustList.remove(dn.getHost());
        mustList.remove(dn.getHostName());
      }
    }
    
    if (listDeadNodes) {
      for (Iterator<String> it = mustList.keySet().iterator(); it.hasNext();) {
        DatanodeDescriptor dn = 
            new DatanodeDescriptor(new DatanodeID(it.next()));
        dn.setLastUpdate(0);
        nodes.add(dn);
      }
    }
    
    return nodes;
  }

  public DatanodeInfo[] datanodeReport(DatanodeReportType type
      ) throws AccessControlException {
    checkSuperuserPrivilege();

    synchronized (this) {
      ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(type);
      DatanodeInfo[] arr = new DatanodeInfo[results.size()];
      for (int i = 0; i < arr.length; i++) {
        arr[i] = new DatanodeInfo(results.get(i));
      }
      return arr;
    }
  }

  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   * @throws IOException if 
   */
  void saveNamespace() throws AccessControlException, IOException {
    checkSuperuserPrivilege();
    synchronized (this) {
      if(!isInSafeMode()) {
        throw new IOException("Safe mode should be turned ON " +
                              "in order to create namespace image");
      }
      getFSImage().saveNamespace(true);
      LOG.info("New namespace image has been created");
    }
  }

  /**
   */
  public synchronized void DFSNodesStatus(ArrayList<DatanodeDescriptor> live, 
                                          ArrayList<DatanodeDescriptor> dead) {

    ArrayList<DatanodeDescriptor> results = 
                            getDatanodeListForReport(DatanodeReportType.ALL);    
    for(Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (isDatanodeDead(node))
        dead.add(node);
      else
        live.add(node);
    }
  }

  /**
   * Prints information about all datanodes.
   */
  private synchronized void datanodeDump(PrintWriter out) {
    synchronized (datanodeMap) {
      out.println("Metasave: Number of datanodes: " + datanodeMap.size());
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        out.println(node.dumpDatanode());
      }
    }
  }

  /**
   * Start decommissioning the specified datanode. 
   */
  private void startDecommission (DatanodeDescriptor node) 
    throws IOException {

    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      LOG.info("Start Decommissioning node " + node.getName());
      node.startDecommission();
      node.decommissioningStatus.setStartTime(now());
      //
      // all the blocks that reside on this node have to be 
      // replicated.
      checkDecommissionStateInternal(node);
    }
  }

  /**
   * Stop decommissioning the specified datanodes.
   */
  public void stopDecommission (DatanodeDescriptor node) 
    throws IOException {
    LOG.info("Stop Decommissioning node " + node.getName());
    node.stopDecommission();
  }

  /** 
   */
  public DatanodeInfo getDataNodeInfo(String name) {
    return datanodeMap.get(name);
  }

  /**
   * @deprecated use {@link NameNode#getNameNodeAddress()} instead.
   */
  @Deprecated
  public InetSocketAddress getDFSNameNodeAddress() {
    return nameNodeAddress;
  }

  /**
   */
  public Date getStartTime() {
    return new Date(systemStart); 
  }
    
  short getMaxReplication()     { return (short)maxReplication; }
  short getMinReplication()     { return (short)minReplication; }
  short getDefaultReplication() { return (short)defaultReplication; }
  
  public void stallReplicationWork()   { stallReplicationWork = true;   }
  public void restartReplicationWork() { stallReplicationWork = false;  }
    
  /**
   * A immutable object that stores the number of live replicas and
   * the number of decommissined Replicas.
   */
  static class NumberReplicas {
    private int liveReplicas;
    private int decommissionedReplicas;
    private int corruptReplicas;
    private int excessReplicas;

    NumberReplicas() {
      initialize(0, 0, 0, 0);
    }

    NumberReplicas(int live, int decommissioned, int corrupt, int excess) {
      initialize(live, decommissioned, corrupt, excess);
    }

    void initialize(int live, int decommissioned, int corrupt, int excess) {
      liveReplicas = live;
      decommissionedReplicas = decommissioned;
      corruptReplicas = corrupt;
      excessReplicas = excess;
    }

    int liveReplicas() {
      return liveReplicas;
    }
    int decommissionedReplicas() {
      return decommissionedReplicas;
    }
    int corruptReplicas() {
      return corruptReplicas;
    }
    int excessReplicas() {
      return excessReplicas;
    }
  } 

  /**
   * Counts the number of nodes in the given list into active and
   * decommissioned counters.
   */
  private NumberReplicas countNodes(Block b,
                                    Iterator<DatanodeDescriptor> nodeIter) {
    int count = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(b);
    while ( nodeIter.hasNext() ) {
      DatanodeDescriptor node = nodeIter.next();
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      }
      else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        count++;
      }
      else  {
        Collection<Block> blocksExcess = 
          excessReplicateMap.get(node.getStorageID());
        if (blocksExcess != null && blocksExcess.contains(b)) {
          excess++;
        } else {
          live++;
        }
      }
    }
    return new NumberReplicas(live, count, corrupt, excess);
  }

  /**
   * Return the number of nodes that are live and decommissioned.
   */
  NumberReplicas countNodes(Block b) {
    return countNodes(b, blocksMap.nodeIterator(b));
  }

  private void logBlockReplicationInfo(Block block, DatanodeDescriptor srcNode,
      NumberReplicas num) {
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = getReplication(block);
    INode fileINode = blocksMap.getINode(block);
    Iterator<DatanodeDescriptor> nodeIter = blocksMap.nodeIterator(block);
    StringBuffer nodeList = new StringBuffer();
    while (nodeIter.hasNext()) {
      DatanodeDescriptor node = nodeIter.next();
      nodeList.append(node.name);
      nodeList.append(" ");
    }
    FSNamesystem.LOG.info("Block: " + block + ", Expected Replicas: "
        + curExpectedReplicas + ", live replicas: " + curReplicas
        + ", corrupt replicas: " + num.corruptReplicas()
        + ", decommissioned replicas: " + num.decommissionedReplicas()
        + ", excess replicas: " + num.excessReplicas() + ", Is Open File: "
        + fileINode.isUnderConstruction() + ", Datanodes having this block: "
        + nodeList + ", Current Datanode: " + srcNode.name
        + ", Is current datanode decommissioning: "
        + srcNode.isDecommissionInProgress());
  }

  
  /**
   * Return true if there are any blocks on this node that have not
   * yet reached their replication factor. Otherwise returns false.
   */
  private boolean isReplicationInProgress(DatanodeDescriptor srcNode) {
    boolean status = false;
    int underReplicatedBlocks = 0;
    int decommissionOnlyReplicas = 0;
    int underReplicatedInOpenFiles = 0;

    for(final Iterator<Block> i = srcNode.getBlockIterator(); i.hasNext(); ) {
      final Block block = i.next();
      INode fileINode = blocksMap.getINode(block);

      if (fileINode != null) {
        NumberReplicas num = countNodes(block);
        int curReplicas = num.liveReplicas();
        int curExpectedReplicas = getReplication(block);
        if (curExpectedReplicas > curReplicas) {
          // Log info about one block for this node which needs replication
          if (!status) {
            status = true;
            logBlockReplicationInfo(block, srcNode, num);
          }
          underReplicatedBlocks++;
          if ((curReplicas == 0) && (num.decommissionedReplicas() > 0)) {
            decommissionOnlyReplicas++;
          }
          if (fileINode.isUnderConstruction()) {
            underReplicatedInOpenFiles++;
          }

          if (!neededReplications.contains(block) &&
            pendingReplications.getNumReplicas(block) == 0) {
            //
            // These blocks have been reported from the datanode
            // after the startDecommission method has been executed. These
            // blocks were in flight when the decommission was started.
            //
            neededReplications.add(block, 
                                   curReplicas,
                                   num.decommissionedReplicas(),
                                   curExpectedReplicas);
          }
        }
      }
    }
    srcNode.decommissioningStatus.set(underReplicatedBlocks,
        decommissionOnlyReplicas, underReplicatedInOpenFiles);

    return status;
  }

  /**
   * Change, if appropriate, the admin state of a datanode to 
   * decommission completed. Return true if decommission is complete.
   */
  boolean checkDecommissionStateInternal(DatanodeDescriptor node) {
    //
    // Check to see if all blocks in this decommissioned
    // node has reached their target replication factor.
    //
    if (node.isDecommissionInProgress()) {
      if (!isReplicationInProgress(node)) {
        node.setDecommissioned();
        LOG.info("Decommission complete for node " + node.getName());
      }
    }
    if (node.isDecommissioned()) {
      return true;
    }
    return false;
  }

  /** 
   * Keeps track of which datanodes/ipaddress are allowed to connect to the namenode.
   */
  private boolean inHostsList(DatanodeID node, String ipAddr) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || 
            (ipAddr != null && hostsList.contains(ipAddr)) ||
            hostsList.contains(node.getHost()) ||
            hostsList.contains(node.getName()) || 
            ((node instanceof DatanodeInfo) && 
             hostsList.contains(((DatanodeInfo)node).getHostName())));
  }
  
  private boolean inExcludedHostsList(DatanodeID node, String ipAddr) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return  ((ipAddr != null && excludeList.contains(ipAddr)) ||
            excludeList.contains(node.getHost()) ||
            excludeList.contains(node.getName()) ||
            ((node instanceof DatanodeInfo) && 
             excludeList.contains(((DatanodeInfo)node).getHostName())));
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   * 1. Added to hosts  --> no further work needed here.
   * 2. Removed from hosts --> mark AdminState as decommissioned. 
   * 3. Added to exclude --> start decommission.
   * 4. Removed from exclude --> stop decommission.
   */
  public void refreshNodes(Configuration conf) throws IOException {
    checkSuperuserPrivilege();
    // Reread the config to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    if (conf == null)
      conf = new Configuration();
    hostsReader.updateFileNames(conf.get("dfs.hosts",""), 
                                conf.get("dfs.hosts.exclude", ""));
    hostsReader.refresh();
    synchronized (this) {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator();
           it.hasNext();) {
        DatanodeDescriptor node = it.next();
        // Check if not include.
        if (!inHostsList(node, null)) {
          node.setDecommissioned();  // case 2.
        } else {
          if (inExcludedHostsList(node, null)) {
            if (!node.isDecommissionInProgress() && 
                !node.isDecommissioned()) {
              startDecommission(node);   // case 3.
            }
          } else {
            if (node.isDecommissionInProgress() || 
                node.isDecommissioned()) {
              stopDecommission(node);   // case 4.
            } 
          }
        }
      }
    } 
      
  }
    
  void finalizeUpgrade() throws IOException {
    checkSuperuserPrivilege();
    synchronized (this) {
      getFSImage().finalizeUpgrade();
    }
  }

  /**
   * Checks if the node is not on the hosts list.  If it is not, then
   * it will be ignored.  If the node is in the hosts list, but is also 
   * on the exclude list, then it will be decommissioned.
   * Returns FALSE if node is rejected for registration. 
   * Returns TRUE if node is registered (including when it is on the 
   * exclude list and is being decommissioned). 
   */
  private synchronized boolean verifyNodeRegistration(DatanodeRegistration nodeReg, String ipAddr) 
    throws IOException {
    if (!inHostsList(nodeReg, ipAddr)) {
      return false;    
    }
    if (inExcludedHostsList(nodeReg, ipAddr)) {
      DatanodeDescriptor node = getDatanode(nodeReg);
      if (node == null) {
        throw new IOException("verifyNodeRegistration: unknown datanode " +
                              nodeReg.getName());
      }
      if (!checkDecommissionStateInternal(node)) {
        startDecommission(node);
      }
    } 
    return true;
  }
    
  /**
   * Checks if the Admin state bit is DECOMMISSIONED.  If so, then 
   * we should shut it down. 
   * 
   * Returns true if the node should be shutdown.
   */
  private boolean shouldNodeShutdown(DatanodeDescriptor node) {
    return (node.isDecommissioned());
  }
    
  /**
   * Get data node by storage ID.
   * 
   * @param nodeID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws IOException
   */
  public DatanodeDescriptor getDatanode(DatanodeID nodeID) throws IOException {
    UnregisteredDatanodeException e = null;
    DatanodeDescriptor node = datanodeMap.get(nodeID.getStorageID());
    if (node == null) 
      return null;
    if (!node.getName().equals(nodeID.getName())) {
      e = new UnregisteredDatanodeException(nodeID, node);
      NameNode.stateChangeLog.fatal("BLOCK* getDatanode: "
                                    + e.getLocalizedMessage());
      throw e;
    }
    return node;
  }
    
  /** Stop at and return the datanode at index (used for content browsing)*/
  @Deprecated
  private DatanodeDescriptor getDatanodeByIndex(int index) {
    int i = 0;
    for (DatanodeDescriptor node : datanodeMap.values()) {
      if (i == index) {
        return node;
      }
      i++;
    }
    return null;
  }
    
  @Deprecated
  public String randomDataNode() {
    int size = datanodeMap.size();
    int index = 0;
    if (size != 0) {
      index = r.nextInt(size);
      for(int i=0; i<size; i++) {
        DatanodeDescriptor d = getDatanodeByIndex(index);
        if (d != null && !d.isDecommissioned() && !isDatanodeDead(d) &&
            !d.isDecommissionInProgress()) {
          return d.getHost() + ":" + d.getInfoPort();
        }
        index = (index + 1) % size;
      }
    }
    return null;
  }

  public DatanodeDescriptor getRandomDatanode() {
    return (DatanodeDescriptor)clusterMap.chooseRandom(NodeBase.ROOT);
  }

  /**
   * SafeModeInfo contains information related to the safe mode.
   * <p>
   * An instance of {@link SafeModeInfo} is created when the name node
   * enters safe mode.
   * <p>
   * During name node startup {@link SafeModeInfo} counts the number of
   * <em>safe blocks</em>, those that have at least the minimal number of
   * replicas, and calculates the ratio of safe blocks to the total number
   * of blocks in the system, which is the size of
   * {@link FSNamesystem#blocksMap}. When the ratio reaches the
   * {@link #threshold} it starts the {@link SafeModeMonitor} daemon in order
   * to monitor whether the safe mode {@link #extension} is passed.
   * Then it leaves safe mode and destroys itself.
   * <p>
   * If safe mode is turned on manually then the number of safe blocks is
   * not tracked because the name node is not intended to leave safe mode
   * automatically in the case.
   *
   * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   * @see SafeModeMonitor
   */
  class SafeModeInfo {
    // configuration fields
    /** Safe mode threshold condition %.*/
    private double threshold;
    /** Safe mode minimum number of datanodes alive */
    private int datanodeThreshold;
    /** Safe mode extension after the threshold. */
    private int extension;
    /** Min replication required by safe mode. */
    private int safeReplication;
      
    // internal fields
    /** Time when threshold was reached.
     * 
     * <br>-1 safe mode is off
     * <br> 0 safe mode is on, but threshold is not reached yet 
     */
    private long reached = -1;  
    /** Total number of blocks. */
    int blockTotal; 
    /** Number of safe blocks. */
    private int blockSafe;
    /** time of the last status printout */
    private long lastStatusReport = 0;
      
    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     *  
     * @param conf configuration
     */
    SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat("dfs.safemode.threshold.pct", 0.95f);
      this.datanodeThreshold = conf.getInt(
          DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_KEY,
          DFSConfigKeys.DFS_NAMENODE_SAFEMODE_MIN_DATANODES_DEFAULT);
      this.extension = conf.getInt("dfs.safemode.extension", 0);
      this.safeReplication = conf.getInt("dfs.replication.min", 1);
      this.blockTotal = 0; 
      this.blockSafe = 0;

      LOG.info("dfs.safemode.threshold.pct          = " + threshold);
      LOG.info("dfs.namenode.safemode.min.datanodes = " + datanodeThreshold);
      LOG.info("dfs.safemode.extension              = " + extension);
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually.
     *
     * The {@link #threshold} is set to 1.5 so that it could never be reached.
     * {@link #blockTotal} is set to -1 to indicate that safe mode is manual.
     * 
     * @see SafeModeInfo
     */
    private SafeModeInfo() {
      this.threshold = 1.5f;  // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.blockTotal = -1;
      this.blockSafe = -1;
      this.reached = -1;
      enter();
      reportStatus("STATE* Safe mode is ON", true);
    }
      
    /**
     * Check if safe mode is on.
     * @return true if in safe mode
     */
    synchronized boolean isOn() {
      try {
        assert isConsistent() : " SafeMode: Inconsistent filesystem state: "
          + "Total num of blocks, active blocks, or "
          + "total safe blocks don't match";
      } catch(IOException e) {
        System.err.print(StringUtils.stringifyException(e));
      }
      return this.reached >= 0;
    }
      
    /**
     * Enter safe mode.
     */
    void enter() {
      this.reached = 0;
    }
      
    /**
     * Leave safe mode.
     * <p>
     * Switch to manual safe mode if distributed upgrade is required.<br>
     * Check for invalid, under- & over-replicated blocks in the end of startup.
     */
    synchronized void leave(boolean checkForUpgrades) {
      if(checkForUpgrades) {
        // verify whether a distributed upgrade needs to be started
        boolean needUpgrade = false;
        try {
          needUpgrade = startDistributedUpgradeIfNeeded();
        } catch(IOException e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        if(needUpgrade) {
          // switch to manual safe mode
          safeMode = new SafeModeInfo();
          return;
        }
      }
      // verify blocks replications
      long startTimeMisReplicatedScan = now();
      processMisReplicatedBlocks();
      NameNode.stateChangeLog.info("STATE* Safe mode termination "
          + "scan for invalid, over- and under-replicated blocks "
          + "completed in " + (now() - startTimeMisReplicatedScan)
          + " msec");

      long timeInSafemode = now() - systemStart;
      NameNode.stateChangeLog.info("STATE* Leaving safe mode after " 
                                    + timeInSafemode/1000 + " secs");
      NameNode.getNameNodeMetrics().setSafeModeTime(timeInSafemode);
      
      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF"); 
      }
      reached = -1;
      safeMode = null;
      NameNode.stateChangeLog.info("STATE* Network topology has "
                                   +clusterMap.getNumOfRacks()+" racks and "
                                   +clusterMap.getNumOfLeaves()+ " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has "
                                   +neededReplications.size()+" blocks");
    }
      
    /** 
     * Safe mode can be turned off iff 
     * the threshold is reached and 
     * the extension time have passed.
     * @return true if can leave or false otherwise.
     */
    synchronized boolean canLeave() {
      if (reached == 0)
        return false;
      if (now() - reached < extension) {
        reportStatus("STATE* Safe mode ON", false);
        return false;
      }
      return !needEnter();
    }
      
    /** 
     * There is no need to enter safe mode 
     * if DFS is empty or {@link #threshold} == 0
     */
    boolean needEnter() {
      return getSafeBlockRatio() < threshold ||
          numLiveDataNodes() < datanodeThreshold;
    }
      
    /**
     * Ratio of the number of safe blocks to the total number of blocks 
     * to be compared with the threshold.
     */
    private float getSafeBlockRatio() {
      return (blockTotal == 0 ? 1 : (float)blockSafe/blockTotal);
    }
      
    /**
     * Check and trigger safe mode if needed. 
     */
    private void checkMode() {
      if (needEnter()) {
        enter();
        reportStatus("STATE* Safe mode ON", false);
        return;
      }
      // the threshold is reached
      if (!isOn() ||                           // safe mode is off
          extension <= 0 || threshold <= 0) {  // don't need to wait
        this.leave(true); // leave safe mode
        return;
      }
      if (reached > 0) {  // threshold has already been reached before
        reportStatus("STATE* Safe mode ON", false);
        return;
      }
      // start monitor
      reached = now();
      smmthread = new Daemon(new SafeModeMonitor());
      smmthread.start();
      reportStatus("STATE* Safe mode extension entered", true);
    }
      
    /**
     * Set total number of blocks.
     */
    synchronized void setBlockTotal(int total) {
      this.blockTotal = total; 
      checkMode();
    }
      
    /**
     * Increment number of safe blocks if current block has 
     * reached minimal replication.
     * @param replication current replication 
     */
    synchronized void incrementSafeBlockCount(short replication) {
      if ((int)replication == safeReplication)
        this.blockSafe++;
      checkMode();
    }
      
    /**
     * Decrement number of safe blocks if current block has 
     * fallen below minimal replication.
     * @param replication current replication 
     */
    synchronized void decrementSafeBlockCount(short replication) {
      if (replication == safeReplication-1)
        this.blockSafe--;
      checkMode();
    }

    /**
     * Check if safe mode was entered manually or at startup.
     */
    boolean isManual() {
      return extension == Integer.MAX_VALUE;
    }

    /**
     * Set manual safe mode.
     */
    void setManual() {
      extension = Integer.MAX_VALUE;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() {
      String leaveMsg = "Safe mode will be turned off automatically";
      if(reached < 0)
        return "Safe mode is OFF";
      if(isManual()) {
        if(getDistributedUpgradeState())
          return leaveMsg + " upon completion of " + 
            "the distributed upgrade: upgrade progress = " + 
            getDistributedUpgradeStatus() + "%";
        leaveMsg = "Use \"hadoop dfsadmin -safemode leave\" to turn safe mode off";
      }
      if(blockTotal < 0)
        return leaveMsg + ".";

      int numLive = numLiveDataNodes();
      String msg = "";
      if (reached == 0) {
        if (getSafeBlockRatio() < threshold) {
          msg += String.format(
            "The reported blocks is only %d"
            + " but the threshold is %.4f and the total blocks %d.",
            blockSafe, threshold, blockTotal);
        }
        if (numLive < datanodeThreshold) {
          if (!"".equals(msg)) {
            msg += "\n";
          }
          msg += String.format(
            "The number of live datanodes %d needs an additional %d live "
            + "datanodes to reach the minimum number %d.",
            numLive, (datanodeThreshold - numLive), datanodeThreshold);
        }
        msg += " " + leaveMsg;
      } else {
        msg = String.format("The reported blocks %d has reached the threshold"
            + " %.4f of total blocks %d.", blockSafe, threshold, 
            blockTotal);

        if (datanodeThreshold > 0) {
          msg += String.format(" The number of live datanodes %d has reached "
                               + "the minimum number %d.",
                               numLive, datanodeThreshold);
        }
        msg += " " + leaveMsg;
      }
      if(reached == 0 || isManual()) {  // threshold is not reached or manual       
        return msg + ".";
      }
      // extension period is in progress
      return msg + " in " + Math.abs(reached + extension - now()) / 1000
          + " seconds.";
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) {
      long curTime = now();
      if(!rightNow && (curTime - lastStatusReport < 20 * 1000))
        return;
      NameNode.stateChangeLog.info(msg + " \n" + getTurnOffTip());
      lastStatusReport = curTime;
    }

    /**
     * Returns printable state of the class.
     */
    public String toString() {
      String resText = "Current safe block ratio = " 
        + getSafeBlockRatio() 
        + ". Target threshold = " + threshold
        + ". Minimal replication = " + safeReplication + ".";
      if (reached > 0) 
        resText += " Threshold was reached " + new Date(reached) + ".";
      return resText;
    }
      
    /**
     * Checks consistency of the class state.
     * This is costly and currently called only in assert.
     */
    boolean isConsistent() throws IOException {
      if (blockTotal == -1 && blockSafe == -1) {
        return true; // manual safe mode
      }
      int activeBlocks = blocksMap.size() - (int)pendingDeletionBlocksCount;
      return (blockTotal == activeBlocks) ||
        (blockSafe >= 0 && blockSafe <= blockTotal);
    }
  }
    
  /**
   * Periodically check whether it is time to leave safe mode.
   * This thread starts when the threshold level is reached.
   *
   */
  class SafeModeMonitor implements Runnable {
    /** interval in msec for checking safe mode: {@value} */
    private static final long recheckInterval = 1000;
      
    /**
     */
    public void run() {
      while (fsRunning && (safeMode != null && !safeMode.canLeave())) {
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
        }
      }
      // leave safe mode and stop the monitor
      try {
        leaveSafeMode(true);
      } catch(SafeModeException es) { // should never happen
        String msg = "SafeModeMonitor may not run during distributed upgrade.";
        assert false : msg;
        throw new RuntimeException(msg, es);
      }
      smmthread = null;
    }
  }
    
  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }
    
  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      checkSuperuserPrivilege();
      switch(action) {
      case SAFEMODE_LEAVE: // leave safe mode
        leaveSafeMode(false);
        break;
      case SAFEMODE_ENTER: // enter safe mode
        enterSafeMode();
        break;
      }
    }
    return isInSafeMode();
  }

  /**
   * Check whether the name node is in safe mode.
   * @return true if safe mode is ON, false otherwise
   */
  boolean isInSafeMode() {
    if (safeMode == null)
      return false;
    return safeMode.isOn();
  }
    
  /**
   * Check whether the name node is in startup mode.
   */
  synchronized boolean isInStartupSafeMode() {
    if (safeMode == null)
      return false;
    return safeMode.isOn() && !safeMode.isManual();
  }

  /**
   * Increment number of blocks that reached minimal replication.
   * @param replication current replication 
   */
  void incrementSafeBlockCount(int replication) {
    if (safeMode == null)
      return;
    safeMode.incrementSafeBlockCount((short)replication);
  }

  /**
   * Decrement number of blocks that reached minimal replication.
   */
  void decrementSafeBlockCount(Block b) {
    if (safeMode == null) // mostly true
      return;
    safeMode.decrementSafeBlockCount((short)countNodes(b).liveReplicas());
  }

  /**
   * Set the total number of blocks in the system. 
   */
  void setBlockTotal() {
    if (safeMode == null)
      return;
    safeMode.setBlockTotal((int)getSafeBlockCount());
  }

  /**
   * Get the total number of blocks in the system. 
   */
  public long getBlocksTotal() {
    return blocksMap.size();
  }

  /**
   * There are times when a block is allocated by a client but was never used to
   * write to. This could happen because the response to block allocation
   * request never made it to the client or the client failed right after block
   * allocation. In such a case, NameNode might get stuck in safemode in
   * subsequent restart waiting for such blocks to be reported. To handle this,
   * such blocks should not be counted toward total blocks needed to exit
   * safemode.
   * <br>
   * This method returns the total number of blocks excluding the last blocks of
   * files under construction with length zero.
   */
  private long getSafeBlockCount() {
    // Calculate number of blocks excluded in safe block count
    long numExcludedBlocks = 0;
    for (Lease lease : leaseManager.getSortedLeases()) {
      for (String path : lease.getPaths()) {
        INode node = dir.getFileINode(path);
        if (node == null) {
          LOG.error("Found a lease for nonexisting file: " + path);
          continue;
        }
        if (!node.isUnderConstruction()) {
          LOG.error("Found a lease for file that is not under construction:"
              + path);
          continue;
        }
        INodeFileUnderConstruction cons = (INodeFileUnderConstruction) node;
        BlockInfo[] blocks = cons.getBlocks();
        if (blocks == null || blocks.length == 0) {
          continue;
        }
        // Exclude the last block of a file under construction with zero length
        if (blocks[blocks.length - 1].getNumBytes() == 0) {
          numExcludedBlocks++;
        }
      }
    }
    LOG.info("Number of blocks excluded by safe block count: "
        + numExcludedBlocks + " total blocks: " + getBlocksTotal()
        + " and thus the safe blocks: "
        + (getBlocksTotal() - numExcludedBlocks));

    return getBlocksTotal() - numExcludedBlocks;
  }
  
  /**
   * Enter safe mode manually.
   * @throws IOException
   */
  synchronized void enterSafeMode() throws IOException {
    if (!isInSafeMode()) {
      safeMode = new SafeModeInfo();
      return;
    }
    safeMode.setManual();
    getEditLog().logSync();
    NameNode.stateChangeLog.info("STATE* Safe mode is ON. " 
                                + safeMode.getTurnOffTip());
  }

  /**
   * Leave safe mode.
   * @throws IOException
   */
  synchronized void leaveSafeMode(boolean checkForUpgrades) throws SafeModeException {
    if (!isInSafeMode()) {
      NameNode.stateChangeLog.info("STATE* Safe mode is already OFF."); 
      return;
    }
    if(getDistributedUpgradeState())
      throw new SafeModeException("Distributed upgrade is in progress",
                                  safeMode);
    safeMode.leave(checkForUpgrades);
  }
    
  public String getSafeModeTip() {
    if (!isInSafeMode())
      return "";
    return safeMode.getTurnOffTip();
  }

  long getEditLogSize() throws IOException {
    return getEditLog().getEditLogSize();
  }

  synchronized CheckpointSignature rollEditLog() throws IOException {
    checkSuperuserPrivilege();
    synchronized (this) {
      if (isInSafeMode()) {
        throw new SafeModeException("Log not rolled", safeMode);
      }
      LOG.info("Roll Edit Log from " + Server.getRemoteAddress());
      return getFSImage().rollEditLog();
    }
  }

  synchronized void rollFSImage() throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Checkpoint not created",
                                  safeMode);
    }
    LOG.info("Roll FSImage from " + Server.getRemoteAddress());
    getFSImage().rollFSImage();
  }

  /**
   * Returns whether the given block is one pointed-to by a file.
   */
  private boolean isValidBlock(Block b) {
    return (blocksMap.getINode(b) != null);
  }

  // Distributed upgrade manager
  UpgradeManagerNamenode upgradeManager = new UpgradeManagerNamenode();

  UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action 
                                                 ) throws IOException {
    return upgradeManager.distributedUpgradeProgress(action);
  }

  UpgradeCommand processDistributedUpgradeCommand(UpgradeCommand comm) throws IOException {
    return upgradeManager.processUpgradeCommand(comm);
  }

  int getDistributedUpgradeVersion() {
    return upgradeManager.getUpgradeVersion();
  }

  UpgradeCommand getDistributedUpgradeCommand() throws IOException {
    return upgradeManager.getBroadcastCommand();
  }

  boolean getDistributedUpgradeState() {
    return upgradeManager.getUpgradeState();
  }

  short getDistributedUpgradeStatus() {
    return upgradeManager.getUpgradeStatus();
  }

  boolean startDistributedUpgradeIfNeeded() throws IOException {
    return upgradeManager.startUpgrade();
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return new PermissionStatus(fsOwner.getShortUserName(), supergroup, permission);
  }

  /*
   * Caller of this method should NOT hold FSNamesystem lock. Otherwise, if
   * FSPermissionChecker instantiation hangs, NN will hang also.
   */
  private FSPermissionChecker getPermissionChecker()
      throws AccessControlException {
    return (isPermissionEnabled) ? new FSPermissionChecker(
        fsOwnerShortUserName, supergroup) : null;
  }

  private void checkOwner(FSPermissionChecker pc, String path)
      throws AccessControlException {
    checkPermission(pc, path, true, null, null, null, null);
  }

  private void checkPathAccess(FSPermissionChecker pc,
      String path, FsAction access
      ) throws AccessControlException {
    checkPermission(pc, path, false, null, null, access, null);
  }

  private void checkParentAccess(FSPermissionChecker pc,
      String path, FsAction access
      ) throws AccessControlException {
    checkPermission(pc, path, false, null, access, null, null);
  }

  private void checkAncestorAccess(FSPermissionChecker pc,
      String path, FsAction access
      ) throws AccessControlException {
    checkPermission(pc, path, false, access, null, null, null);
  }

  private void checkTraverse(FSPermissionChecker pc, String path
      ) throws AccessControlException {
    checkPermission(pc, path, false, null, null, null, null);
  }

  private void checkSuperuserPrivilege() throws AccessControlException {
    if (isPermissionEnabled) {
      FSPermissionChecker pc = getPermissionChecker();
      pc.checkSuperuserPrivilege();
    }
  }

  /**
   * Check whether current user have permissions to access the path.
   * For more details of the parameters, see
   * {@link FSPermissionChecker#checkPermission(String, INodeDirectory, boolean, FsAction, FsAction, FsAction, FsAction)}.
   */
  private void checkPermission(FSPermissionChecker pc,
      String path, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess) throws AccessControlException {
    if (!pc.isSuperUser()) {
      dir.waitForReady();
      synchronized (this) {
        pc.checkPermission(path, dir.rootDir, doCheckOwner, ancestorAccess,
            parentAccess, access, subAccess);
      }
    }
  }

  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  void checkFsObjectLimit() throws IOException {
    if (maxFsObjects != 0 &&
        maxFsObjects <= dir.totalInodes() + getBlocksTotal()) {
      throw new IOException("Exceeded the configured number of objects " +
                             maxFsObjects + " in the filesystem.");
    }
  }

  /**
   * Get the total number of objects in the system. 
   */
  long getMaxObjects() {
    return maxFsObjects;
  }

  public long getFilesTotal() {
    return this.dir.totalInodes();
  }

  public long getPendingReplicationBlocks() {
    return pendingReplicationBlocksCount;
  }

  public long getUnderReplicatedBlocks() {
    return underReplicatedBlocksCount;
  }

  /** Returns number of blocks with corrupt replicas */
  public long getCorruptReplicaBlocks() {
    return corruptReplicaBlocksCount;
  }

  public long getScheduledReplicationBlocks() {
    return scheduledReplicationBlocksCount;
  }

  public long getPendingDeletionBlocks() {
    return pendingDeletionBlocksCount;
  }

  public long getExcessBlocks() {
    return excessBlocksCount;
  }
  
  public synchronized int getBlockCapacity() {
    return blocksMap.getCapacity();
  }

  public String getFSState() {
    return isInSafeMode() ? "safeMode" : "Operational";
  }
  
  private ObjectName mbeanName;
  
  private ObjectName mxBean = null;
  /**
   * Register the FSNamesystem MBean using the name
   *        "hadoop:service=NameNode,name=FSNamesystemState"
   * Register the FSNamesystem MXBean using the name
   *        "hadoop:service=NameNode,name=NameNodeInfo"
   */
  void registerMBean(Configuration conf) {
    // We wrap to bypass standard mbean naming convention.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;
    try {
      bean = new StandardMBean(this,FSNamesystemMBean.class);
      mbeanName = MBeans.register("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
    mxBean = MBeans.register("NameNode", "NameNodeInfo", this);

    LOG.info("Registered FSNamesystemStateMBean and NameNodeMXBean");
  }

  /**
   * shutdown FSNamesystem
   */
  public void shutdown() {
    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
    }
    if (mxBean != null) {
      MBeans.unregister(mxBean);
    }
    if (dir != null) {
      dir.shutdown();
    }
  }
  

  /**
   * Number of live data nodes
   * @return Number of live data nodes
   */
  public int numLiveDataNodes() {
    int numLive = 0;
    synchronized (datanodeMap) {   
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
                                                               it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        if (!isDatanodeDead(dn) ) {
          numLive++;
        }
      }
    }
    return numLive;
  }
  

  /**
   * Number of dead data nodes
   * @return Number of dead data nodes
   */
  public int numDeadDataNodes() {
    int numDead = 0;
    synchronized (datanodeMap) {   
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
                                                               it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        if (isDatanodeDead(dn) ) {
          numDead++;
        }
      }
    }
    return numDead;
  }

  /**
   * Sets the generation stamp for this filesystem
   */
  public void setGenerationStamp(long stamp) {
    generationStamp.setStamp(stamp);
  }

  /**
   * Gets the generation stamp for this filesystem
   */
  public long getGenerationStamp() {
    return generationStamp.getStamp();
  }

  /**
   * Increments, logs and then returns the stamp
   */
  private long nextGenerationStamp() {
    long gs = generationStamp.nextStamp();
    getEditLog().logGenerationStamp(gs);
    return gs;
  }

  /**
   * Verifies that the block is associated with a file that has a lease.
   * Increments, logs and then returns the stamp
   *
   * @param block block
   * @param fromNN if it is for lease recovery initiated by NameNode
   * @return a new generation stamp
   */  
  synchronized long nextGenerationStampForBlock(Block block, boolean fromNN) throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot get nextGenStamp for " + block, safeMode);
    }
    BlockInfo storedBlock = blocksMap.getStoredBlock(block);
    if (storedBlock == null) {
      BlockInfo match =
        blocksMap.getStoredBlock(block.getWithWildcardGS());
      String msg = (match == null)
        ? block + " is missing"
        : block + " has out of date GS " + block.getGenerationStamp() +
                  " found " + match.getGenerationStamp() +
                  ", may already be committed";
      LOG.info(msg);
      throw new IOException(msg);
    }
    INodeFile fileINode = storedBlock.getINode();
    if (!fileINode.isUnderConstruction()) {
      String msg = block + " is already commited, !fileINode.isUnderConstruction().";
      LOG.info(msg);
      throw new IOException(msg);
    }
    // Disallow client-initiated recovery once
    // NameNode initiated lease recovery starts
    if (!fromNN && HdfsConstants.NN_RECOVERY_LEASEHOLDER.equals(
        leaseManager.getLeaseByPath(FSDirectory.getFullPathName(fileINode)).getHolder())) {
      String msg = block +
        "is being recovered by NameNode, ignoring the request from a client";
      LOG.info(msg);
      throw new IOException(msg);
    }
    if (!((INodeFileUnderConstruction)fileINode).setLastRecoveryTime(now())) {
      String msg = block + " is already being recovered, ignoring this request.";
      LOG.info(msg);
      throw new IOException(msg);
    }
    return nextGenerationStamp();
  }

  // rename was successful. If any part of the renamed subtree had
  // files that were being written to, update with new filename.
  //
  void changeLease(String src, String dst, HdfsFileStatus dinfo) 
                   throws IOException {
    String overwrite;
    String replaceBy;

    boolean destinationExisted = true;
    if (dinfo == null) {
      destinationExisted = false;
    }

    if (destinationExisted && dinfo.isDir()) {
      Path spath = new Path(src);
      Path parent = spath.getParent();
      if (isRoot(parent)) {
        overwrite = parent.toString();
      } else {
        overwrite = parent.toString() + Path.SEPARATOR;
      }
      replaceBy = dst + Path.SEPARATOR;
    } else {
      overwrite = src;
      replaceBy = dst;
    }

    leaseManager.changeLease(src, dst, overwrite, replaceBy);
  }

  private boolean isRoot(Path path) {
    return path.getParent() == null;
  }

  /**
   * Serializes leases. 
   */
  void saveFilesUnderConstruction(DataOutputStream out) throws IOException {
    synchronized (leaseManager) {
      out.writeInt(leaseManager.countPath()); // write the size

      for (Lease lease : leaseManager.getSortedLeases()) {
        for(String path : lease.getPaths()) {
          // verify that path exists in namespace
          INode node = dir.getFileINode(path);
          if (node == null) {
            throw new IOException("saveLeases found path " + path +
                                  " but no matching entry in namespace.");
          }
          if (!node.isUnderConstruction()) {
            throw new IOException("saveLeases found path " + path +
                                  " but is not under construction.");
          }
          INodeFileUnderConstruction cons = (INodeFileUnderConstruction) node;
          FSImage.writeINodeUnderConstruction(out, cons, path);
        }
      }
    }
  }
  
  public synchronized ArrayList<DatanodeDescriptor> getDecommissioningNodes() {
    ArrayList<DatanodeDescriptor> decommissioningNodes = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(DatanodeReportType.LIVE);
    for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (node.isDecommissionInProgress()) {
        decommissioningNodes.add(node);
      }
    }
    return decommissioningNodes;
  }

  /*
   * Delegation Token
   */
  
  private DelegationTokenSecretManager createDelegationTokenSecretManager(
      Configuration conf) {
    return new DelegationTokenSecretManager(conf.getLong(
        "dfs.namenode.delegation.key.update-interval", 24*60*60*1000),
        conf.getLong(
            "dfs.namenode.delegation.token.max-lifetime", 7*24*60*60*1000),
        conf.getLong(
            "dfs.namenode.delegation.token.renew-interval", 24*60*60*1000),
        DELEGATION_TOKEN_REMOVER_SCAN_INTERVAL, this);
  }

  /**
   * Returns the DelegationTokenSecretManager instance in the namesystem.
   * @return delegation token secret manager object
   */
  public DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return dtSecretManager;
  }

  /**
   * @param renewer
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot issue delegation token", safeMode);
    }
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be issued only with kerberos or web authentication");
    }
    if(dtSecretManager == null || !dtSecretManager.isRunning()) {
      LOG.warn("trying to get DT with no secret manager running");
      return null;
    }
    
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner,
        renewer, realUser);
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
        dtId, dtSecretManager);
    long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
    logGetDelegationToken(dtId, expiryTime);
    return token;
  }

  /**
   * 
   * @param token
   * @return New expiryTime of the token
   * @throws InvalidToken
   * @throws IOException
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
      throws InvalidToken, IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot renew delegation token", safeMode);
    }
    if (!isAllowedDelegationTokenOp()) {
      throw new IOException(
          "Delegation Token can be renewed only with kerberos or web authentication");
    }
    String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
    long expiryTime = dtSecretManager.renewToken(token, renewer);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    id.readFields(in);
    logRenewDelegationToken(id, expiryTime);
    return expiryTime;
  }

  /**
   * 
   * @param token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
      throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot cancel delegation token", safeMode);
    }
    String canceller = UserGroupInformation.getCurrentUser().getUserName();
    DelegationTokenIdentifier id = dtSecretManager
        .cancelToken(token, canceller);
    logCancelDelegationToken(id);
  }
  
  /**
   * @param out save state of the secret manager
   */
  void saveSecretManagerState(DataOutputStream out) throws IOException {
    dtSecretManager.saveSecretManagerState(out);
  }

  /**
   * @param in load the state of secret manager from input stream
   */
  void loadSecretManagerState(DataInputStream in) throws IOException {
    dtSecretManager.loadSecretManagerState(in);
  }

  /**
   * Log the getDelegationToken operation to edit logs
   * 
   * @param id identifer of the new delegation token
   * @param expiryTime when delegation token expires
   */
  private void logGetDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) throws IOException {
    synchronized (this) {
      getEditLog().logGetDelegationToken(id, expiryTime);
    }
    getEditLog().logSync();
  }

  /**
   * Log the renewDelegationToken operation to edit logs
   * 
   * @param id identifer of the delegation token being renewed
   * @param expiryTime when delegation token expires
   */
  private void logRenewDelegationToken(DelegationTokenIdentifier id,
      long expiryTime) throws IOException {
    synchronized (this) {
      getEditLog().logRenewDelegationToken(id, expiryTime);
    }
    getEditLog().logSync();
  }

  
  /**
   * Log the cancelDelegationToken operation to edit logs
   * 
   * @param id identifer of the delegation token being cancelled
   */
  private void logCancelDelegationToken(DelegationTokenIdentifier id)
      throws IOException {
    synchronized (this) {
      getEditLog().logCancelDelegationToken(id);
    }
    getEditLog().logSync();
  }

  /**
   * Log the updateMasterKey operation to edit logs
   * 
   * @param key new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) throws IOException {
    synchronized (this) {
      getEditLog().logUpdateMasterKey(key);
    }
    getEditLog().logSync();
  }
  
  /**
   * 
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled()
        && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL)
        && (authMethod != AuthenticationMethod.CERTIFICATE)) {
      return false;
    }
    return true;
  }
  
  /**
   * Returns authentication method used to establish the connection
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod()
      throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }
  
  @Override // NameNodeMXBean
  public String getHostName() {
    return this.nameNodeHostName;
  }
  
  @Override // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion() + ", r" + VersionInfo.getRevision();
  }

  @Override // NameNodeMXBean
  public long getUsed() {
    return this.getCapacityUsed();
  }

  @Override // NameNodeMXBean
  public long getFree() {
    return this.getCapacityRemaining();
  }

  @Override // NameNodeMXBean
  public long getTotal() {
    return this.getCapacityTotal();
  }

  @Override // NameNodeMXBean
  public String getSafemode() {
    if (!this.isInSafeMode())
      return "";
    return "Safe mode is ON." + this.getSafeModeTip();
  }

  @Override // NameNodeMXBean
  public boolean isUpgradeFinalized() {
    return this.getFSImage().isUpgradeFinalized();
  }

  @Override // NameNodeMXBean
  public long getNonDfsUsedSpace() {
    return getCapacityUsedNonDFS();
  }

  @Override // NameNodeMXBean
  public float getPercentUsed() {
    return getCapacityUsedPercent();
  }

  @Override // NameNodeMXBean
  public float getPercentRemaining() {
    return getCapacityRemainingPercent();
  }

  @Override // NameNodeMXBean
  public long getTotalBlocks() {
    return getBlocksTotal();
  }

  @Override // NameNodeMXBean
  public long getTotalFiles() {
    return getFilesTotal();
  }

  @Override // NameNodeMXBean
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of live node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getLiveNodes() {
    final Map<String, Object> info = new HashMap<String, Object>();
    final ArrayList<DatanodeDescriptor> aliveNodeList =
      this.getDatanodeListForReport(DatanodeReportType.LIVE); 
    for (DatanodeDescriptor node : aliveNodeList) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      innerinfo.put("usedSpace", getDfsUsed(node));
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Object> info = new HashMap<String, Object>();
    final ArrayList<DatanodeDescriptor> deadNodeList =
      this.getDatanodeListForReport(DatanodeReportType.DEAD); 
    removeDecomNodeFromDeadList(deadNodeList);
    for (DatanodeDescriptor node : deadNodeList) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("lastContact", getLastContact(node));
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decomisioning node attribute keys to its values
   */
  @Override // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Object> info = new HashMap<String, Object>();
    final ArrayList<DatanodeDescriptor> decomNodeList = 
      this.getDecommissioningNodes();
    for (DatanodeDescriptor node : decomNodeList) {
      final Map<String, Object> innerinfo = new HashMap<String, Object>();
      innerinfo.put("underReplicatedBlocks", node.decommissioningStatus
          .getUnderReplicatedBlocks());
      innerinfo.put("decommissionOnlyReplicas", node.decommissioningStatus
          .getDecommissionOnlyReplicas());
      innerinfo.put("underReplicateInOpenFiles", node.decommissioningStatus
          .getUnderReplicatedInOpenFiles());
      info.put(node.getHostName(), innerinfo);
    }
    return JSON.toString(info);
  }

  @Override  // NameNodeMXBean
  public String getNameDirStatuses() {
    Map<String, Map<File, StorageDirType>> statusMap =
      new HashMap<String, Map<File, StorageDirType>>();
    
    Map<File, StorageDirType> activeDirs = new HashMap<File, StorageDirType>();
    for (Iterator<StorageDirectory> it
        = getFSImage().dirIterator(); it.hasNext();) {
      StorageDirectory st = it.next();
      activeDirs.put(st.getRoot(), st.getStorageDirType());
    }
    statusMap.put("active", activeDirs);
    
    List<Storage.StorageDirectory> removedStorageDirs
        = getFSImage().getRemovedStorageDirs();
    Map<File, StorageDirType> failedDirs = new HashMap<File, StorageDirType>();
    for (StorageDirectory st : removedStorageDirs) {
      failedDirs.put(st.getRoot(), st.getStorageDirType());
    }
    statusMap.put("failed", failedDirs);
    
    return JSON.toString(statusMap);
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (System.currentTimeMillis() - alivenode.getLastUpdate())/1000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
  }

  private static int roundBytesToGBytes(long bytes) {
    return Math.round(((float)bytes/(1024 * 1024 * 1024)));
  }

  @Override
  public void getMetrics(MetricsBuilder builder, boolean all) {
    builder.addRecord("FSNamesystem").setContext("dfs")
      .addGauge("FilesTotal", "", getFilesTotal())
      .addGauge("BlocksTotal", "", getBlocksTotal())
      .addGauge("CapacityTotalGB", "",
                roundBytesToGBytes(getCapacityTotal()))
      .addGauge("CapacityUsedGB", "",
                roundBytesToGBytes(getCapacityUsed()))
      .addGauge("CapacityRemainingGB", "",
                roundBytesToGBytes(getCapacityRemaining()))
      .addGauge("CapacityTotal", "", getCapacityTotal())
      .addGauge("CapacityUsed", "", getCapacityUsed())
      .addGauge("CapacityRemaining", "", getCapacityRemaining())
      .addGauge("CapacityUsedNonDFS", "", getNonDfsUsedSpace())
      .addGauge("TotalLoad", "", getTotalLoad())
      .addGauge("CorruptBlocks", "", getCorruptReplicaBlocks())
      .addGauge("ExcessBlocks", "", getExcessBlocks())
      .addGauge("PendingDeletionBlocks", "", getPendingDeletionBlocks())
      .addGauge("PendingReplicationBlocks", "", getPendingReplicationBlocks())
      .addGauge("UnderReplicatedBlocks", "", getUnderReplicatedBlocks())
      .addGauge("ScheduledReplicationBlocks", "",
                getScheduledReplicationBlocks())
      .addGauge("MissingBlocks", "", getMissingBlocksCount())
      .addGauge("BlockCapacity", "", getBlockCapacity())
      .addGauge("StaleDataNodes", 
          "Number of datanodes marked stale due to delayed heartbeat", 
          this.getNumStaleNodes());
  }

  private void registerWith(MetricsSystem ms) {
    ms.register("FSNamesystemMetrics", "FSNamesystem metrics", this);
  }

  
  /**
   * If the remote IP for namenode method invokation is null, then the
   * invocation is internal to the namenode. Client invoked methods are invoked
   * over RPC and always have address != null.
   */
  private boolean isExternalInvocation() {
    return Server.getRemoteIp() != null;
  }
  
  /**
   * Log fsck event in the audit log 
   */
  void logFsckEvent(String src, InetAddress remoteAddress) throws IOException {
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(),
                    remoteAddress,
                    "fsck", src, null, null);
    }
  }


  /**
   * Remove an already decommissioned data node who is neither in include nor
   * exclude lists from the dead node list.
   * @param dead, array list of dead nodes
   */
  void removeDecomNodeFromDeadList(ArrayList<DatanodeDescriptor> dead) {
    // If the include list is empty, any nodes are welcomed and it does not 
    // make sense to exclude any nodes from the cluster.  Therefore, no remove.
    if (hostsReader.getHosts().isEmpty()) {
      return;
    }
    for (Iterator<DatanodeDescriptor> it = dead.iterator();it.hasNext();){
      DatanodeDescriptor node = it.next();
      if ((!inHostsList(node,null)) 
          && (!inExcludedHostsList(node, null))
          && node.isDecommissioned()){
        // Include list is not empty, an existing datanode does not appear 
        // in both include or exclude lists and it has been decommissioned.
        // Remove it from dead node list.
        it.remove();
      } 
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + host2DataNodeMap;
  }

  /**
   * Set the number of stale DataNodes, based on DataNodes' heartbeats.
   *
   * @param numStaleNodes
   *          The number of stale DataNodes to be set.
   */
  void setNumStaleNodes(int numStaleNodes) {
    this.numStaleNodes = numStaleNodes;
  }

  /**
   * @return Return the current number of stale DataNodes (detected by
   *         HeartbeatMonitor).
   */
  public int getNumStaleNodes() {
    return this.numStaleNodes;
  }

  /*
   * Whether stale datanodes should be avoided as targets on the write path.
   * The result of this function may change if the number of stale datanodes
   * eclipses a configurable threshold.
   * 
   * @return whether stale datanodes should be avoided on the write path
   */
  public boolean shouldAvoidStaleDataNodesForWrite() {
    // If # stale exceeds maximum staleness ratio, disable stale
    // datanode avoidance on the write path
    return avoidStaleDataNodesForWrite &&
        (numStaleNodes <= heartbeats.size()
            * ratioUseStaleDataNodesForWrite);
  }
  
  /**
   * Used by {@link FSNamesystem#getCorruptFileBlocks()} and
   * {@link FSNamesystem#listCorruptFileBlocks()} to represent information about
   * corrupt file and its corresponding block
   */
  static class CorruptFileBlockInfo {
    String path;
    Block block;
    
    public CorruptFileBlockInfo(String p, Block b) {
      path = p;
      block = b;
    }
    
    @Override
    public String toString() {
      return block.getBlockName() + "\t" + path;
    }
  }
  
  /**
   * @return a collection of corrupt files with their blocks information, with a
   *         maximum of {@link FSNamesystem#maxCorruptFilesReturned} files
   *         listed in total
   */
  private Collection<CorruptFileBlockInfo> getCorruptFileBlocks() {
    ArrayList<CorruptFileBlockInfo> corruptFiles = 
        new ArrayList<CorruptFileBlockInfo>();
    for (Block blk : neededReplications.getCorruptQueue()){
      INode inode = blocksMap.getINode(blk);
      if (inode != null && countNodes(blk).liveReplicas() == 0) {
        String filePath = inode.getFullPathName();
        CorruptFileBlockInfo info = new CorruptFileBlockInfo(filePath, blk);
        corruptFiles.add(info);
        if (corruptFiles.size() >= this.maxCorruptFilesReturned) {
          break;
        }
      }
    }
    return corruptFiles;
  }
  
  /**
   * @return Collection of CorruptFileBlockInfo objects representing files with
   * corrupted blocks.
   * @throws AccessControlException
   * @throws IOException
   */
  synchronized Collection<CorruptFileBlockInfo> listCorruptFileBlocks()
      throws AccessControlException, IOException {
    checkSuperuserPrivilege();
    return getCorruptFileBlocks();
  }
  
}
