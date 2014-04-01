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

package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;

/**
 * Provides methods for writing to and reading from job history. 
 * Job History works in an append mode, JobHistory and its inner classes provide methods 
 * to log job events. 
 * 
 * JobHistory is split into multiple files, format of each file is plain text where each line 
 * is of the format [type (key=value)*], where type identifies the type of the record. 
 * Type maps to UID of one of the inner classes of this class. 
 * 
 * Job history is maintained in a master index which contains star/stop times of all jobs with
 * a few other job level properties. Apart from this each job's history is maintained in a seperate history 
 * file. name of job history files follows the format jobtrackerId_jobid
 *  
 * For parsing the job history it supports a listener based interface where each line is parsed
 * and passed to listener. The listener can create an object model of history or look for specific 
 * events and discard rest of the history.  
 * 
 * CHANGE LOG :
 * Version 0 : The history has the following format : 
 *             TAG KEY1="VALUE1" KEY2="VALUE2" and so on. 
               TAG can be Job, Task, MapAttempt or ReduceAttempt. 
               Note that a '"' is the line delimiter.
 * Version 1 : Changes the line delimiter to '.'
               Values are now escaped for unambiguous parsing. 
               Added the Meta tag to store version info.
 */
public class JobHistory {
  
  static final long VERSION = 1L;

  static final int DONE_DIRECTORY_FORMAT_VERSION = 1;

  static final String DONE_DIRECTORY_FORMAT_DIRNAME
    = "version-" + DONE_DIRECTORY_FORMAT_VERSION;

  static final String UNDERSCORE_ESCAPE = "%5F";

  public static final Log LOG = LogFactory.getLog(JobHistory.class);
  private static final char DELIMITER = ' ';
  static final char LINE_DELIMITER_CHAR = '.';
  static final char[] charsToEscape = new char[] {'"', '=', 
                                                LINE_DELIMITER_CHAR};
  static final String DIGITS = "[0-9]+";

  static final String KEY = "(\\w+)";
  // value is any character other than quote, but escaped quotes can be there
  static final String VALUE = "[^\"\\\\]*+(?:\\\\.[^\"\\\\]*+)*+";
  
  static final Pattern pattern = Pattern.compile(KEY + "=" + "\"" + VALUE + "\"");

  static final int MAXIMUM_DATESTRING_COUNT = 200000;
  
  public static final int JOB_NAME_TRIM_LENGTH = 50;
  private static String JOBTRACKER_UNIQUE_STRING = null;
  private static String LOG_DIR = null;
  private static final String SECONDARY_FILE_SUFFIX = ".recover";
  private static long jobHistoryBlockSize = 0;
  private static String jobtrackerHostname;
  private static JobHistoryFilesManager fileManager = null;
  final static FsPermission HISTORY_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0755); // rwxr-xr-x
  final static FsPermission HISTORY_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0744); // rwxr--r--
  private static FileSystem LOGDIR_FS; // log dir filesystem
  protected static FileSystem DONEDIR_FS; // Done dir filesystem
  private static JobConf jtConf;
  protected static Path DONE = null; // folder for completed jobs
  private static String DONE_BEFORE_SERIAL_TAIL = doneSubdirsBeforeSerialTail();
  private static String DONE_LEAF_FILES = DONE_BEFORE_SERIAL_TAIL + "/*";
  private static boolean aclsEnabled = false;

  static final String CONF_FILE_NAME_SUFFIX = "_conf.xml";

  private static final int SERIAL_NUMBER_DIRECTORY_DIGITS = 6;
  private static int SERIAL_NUMBER_LOW_DIGITS;

  private static String SERIAL_NUMBER_FORMAT;

  private static final Set<Path> existingDoneSubdirs = new HashSet<Path>();

  private static final SortedMap<Integer, String> idToDateString
    = new TreeMap<Integer, String>();

  /**
   * A filter for conf files
   */  
  private static final PathFilter CONF_FILTER = new PathFilter() {
    public boolean accept(Path path) {
      return path.getName().endsWith(CONF_FILE_NAME_SUFFIX);
    }
  };

  private static final Map<JobID, MovedFileInfo> jobHistoryFileMap =
    Collections.<JobID,MovedFileInfo>synchronizedMap(
        new LinkedHashMap<JobID, MovedFileInfo>());

  private static final SortedMap<Long, String>jobToDirectoryMap
    = new TreeMap<Long, String>();

  // JobHistory filename regex
  public static final Pattern JOBHISTORY_FILENAME_REGEX = 
    Pattern.compile("(" + JobID.JOBID_REGEX + ")_.+");
  // JobHistory conf-filename regex
  public static final Pattern CONF_FILENAME_REGEX =
    Pattern.compile("(" + JobID.JOBID_REGEX + ")_conf.xml");
  
  private static class MovedFileInfo {
    private final String historyFile;
    private final long timestamp;
    public MovedFileInfo(String historyFile, long timestamp) {
      this.historyFile = historyFile;
      this.timestamp = timestamp;
    }
  }

  /**
   * Given the job id, return the history file path from the cache
   */
  public static String getHistoryFilePath(JobID jobId) {
    MovedFileInfo info = jobHistoryFileMap.get(jobId);
    if (info == null) {
      return null;
    }
    return info.historyFile;
  }

  /**
   * A class that manages all the files related to a job. For now 
   *   - writers : list of open files
   *   - job history filename
   *   - job conf filename
   */
  private static class JobHistoryFilesManager {
    // a private (virtual) folder for all the files related to a running job
    private static class FilesHolder {
      ArrayList<PrintWriter> writers = new ArrayList<PrintWriter>();
      Path historyFilename; // path of job history file
      Path confFilename; // path of job's conf
    }
   
    private ThreadPoolExecutor executor = null;
    private final Configuration conf;
    private final JobTracker jobTracker;

   // cache from job-key to files associated with it.
    private Map<JobID, FilesHolder> fileCache = 
      new ConcurrentHashMap<JobID, FilesHolder>();

    JobHistoryFilesManager(Configuration conf, JobTracker jobTracker)
        throws IOException {
      this.conf = conf;
      this.jobTracker = jobTracker;
    }


    void start() {
      executor = new ThreadPoolExecutor(5, 5, 1, 
          TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
      // make core threads to terminate if there has been no work
      // for the keppalive period.
      executor.allowCoreThreadTimeOut(true);
    }

    private FilesHolder getFileHolder(JobID id) {
      FilesHolder holder = fileCache.get(id);
     if (holder == null) {
         holder = new FilesHolder();
         fileCache.put(id, holder);
      }
      return holder;
    }

    void addWriter(JobID id, PrintWriter writer) {
      FilesHolder holder = getFileHolder(id);
      holder.writers.add(writer);
    }

    void setHistoryFile(JobID id, Path file) {
      FilesHolder holder = getFileHolder(id);
      holder.historyFilename = file;
    }

    void setConfFile(JobID id, Path file) {
      FilesHolder holder = getFileHolder(id);
      holder.confFilename = file;
    }

    ArrayList<PrintWriter> getWriters(JobID id) {
      FilesHolder holder = fileCache.get(id);
      return holder == null ? null : holder.writers;
    }

    Path getHistoryFile(JobID id) {
      FilesHolder holder = fileCache.get(id);
      return holder == null ? null : holder.historyFilename;
    }

    Path getConfFileWriters(JobID id) {
      FilesHolder holder = fileCache.get(id);
      return holder == null ? null : holder.confFilename;
    }

    void purgeJob(JobID id) {
      fileCache.remove(id);
    }

    void moveToDone(final JobID id) {
      final List<Path> paths = new ArrayList<Path>();
      final Path historyFile = fileManager.getHistoryFile(id);
      if (historyFile == null) {
        LOG.info("No file for job-history with " + id + " found in cache!");
      } else {
        paths.add(historyFile);
      }

      final Path confPath = fileManager.getConfFileWriters(id);
      if (confPath == null) {
        LOG.info("No file for jobconf with " + id + " found in cache!");
      } else {
        paths.add(confPath);
      }

      executor.execute(new Runnable() {

        public void run() {
          long millisecondTime = System.currentTimeMillis();

          Path resultDir = canonicalHistoryLogPath(id, millisecondTime);

          //move the files to DONE canonical subfolder
          try {
            for (Path path : paths) {
              //check if path exists, in case of retries it may not exist
              if (LOGDIR_FS.exists(path)) {
                maybeMakeSubdirectory(id, millisecondTime);

                LOG.info("Moving " + path.toString() + " to " + 
                    resultDir.toString()); 
                DONEDIR_FS.moveFromLocalFile(path, resultDir);
                DONEDIR_FS.setPermission(new Path(resultDir, path.getName()), 
                    new FsPermission(HISTORY_FILE_PERMISSION));
              }
            }
          } catch (Throwable e) {
            LOG.error("Unable to move history file to DONE canonical subfolder.", e);
          }
          String historyFileDonePath = null;
          if (historyFile != null) {
            historyFileDonePath = new Path(resultDir, 
                historyFile.getName()).toString();
          }

          jobHistoryFileMap.put(id, new MovedFileInfo(historyFileDonePath,
                                                      millisecondTime));
          jobTracker.historyFileCopied(id, historyFileDonePath);
          
          //purge the job from the cache
          fileManager.purgeJob(id);
        }

      });
    }

    void removeWriter(JobID jobId, PrintWriter writer) {
      fileManager.getWriters(jobId).remove(writer);
    }
  }

  // several methods for manipulating the subdirectories of the DONE
  // directory 

  private static int jobSerialNumber(JobID id) {
    return id.getId();
  }

  private static String serialNumberDirectoryComponent(JobID id) {
    return String.format(SERIAL_NUMBER_FORMAT,
                         Integer.valueOf(jobSerialNumber(id)))
              .substring(0, SERIAL_NUMBER_DIRECTORY_DIGITS);
  }

  // directory components may contain internal slashes, but do NOT
  // contain slashes at either end.

  private static String timestampDirectoryComponent(JobID id, long millisecondTime) {
    int serialNumber = jobSerialNumber(id);
    Integer boxedSerialNumber = serialNumber;

    // don't want to do this inside the lock
    Calendar timestamp = Calendar.getInstance();
    timestamp.setTimeInMillis(millisecondTime);

    synchronized (idToDateString) {
      String dateString = idToDateString.get(boxedSerialNumber);

      if (dateString == null) {

        dateString = String.format
          ("%04d/%02d/%02d",
           timestamp.get(Calendar.YEAR),
           // months are 0-based in Calendar, but people will expect January
           // to be month #1.
            timestamp.get(Calendar.MONTH) + 1,
            timestamp.get(Calendar.DAY_OF_MONTH));

        dateString = dateString.intern();

        idToDateString.put(boxedSerialNumber, dateString);

        if (idToDateString.size() > MAXIMUM_DATESTRING_COUNT) {
          idToDateString.remove(idToDateString.firstKey());
        }
      }

      return dateString;
    }
  }

  // returns false iff the directory already existed
  private static boolean maybeMakeSubdirectory(JobID id, long millisecondTime)
          throws IOException {
    Path dir = canonicalHistoryLogPath(id, millisecondTime);

    synchronized (existingDoneSubdirs) {
      if (existingDoneSubdirs.contains(dir)) {
        if (LOG.isDebugEnabled() && !DONEDIR_FS.exists(dir)) {
          LOG.error("JobHistory.maybeMakeSubdirectory -- We believed " + dir
              + " already existed, but it didn't.");
        }
          
        return true;
      }

      if (!DONEDIR_FS.exists(dir)) {
        LOG.info("Creating DONE subfolder at "+ dir);

        if (!FileSystem.mkdirs(DONEDIR_FS, dir,
                               new FsPermission(HISTORY_DIR_PERMISSION))) {
          throw new IOException("Mkdirs failed to create " + dir.toString());
        }

        existingDoneSubdirs.add(dir);

        return false;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.error("JobHistory.maybeMakeSubdirectory -- We believed " + dir
              + " didn't already exist, but it did.");
        }

        return false;
      }
    }
  }

  private static Path canonicalHistoryLogPath(JobID id, long millisecondTime) {
    return new Path(DONE, historyLogSubdirectory(id, millisecondTime));
  }

  private static String historyLogSubdirectory(JobID id, long millisecondTime) {
    String result
      = (DONE_DIRECTORY_FORMAT_DIRNAME
         + "/" + jobtrackerDirectoryComponent(id));

    String serialNumberDirectory = serialNumberDirectoryComponent(id);

    result = (result
              + "/" + timestampDirectoryComponent(id, millisecondTime)
              + "/" + serialNumberDirectory
              + "/");

    return result;
  }

  private static String jobtrackerDirectoryComponent(JobID id) {
    return JOBTRACKER_UNIQUE_STRING;
  }

  private static String doneSubdirsBeforeSerialTail() {
    // job tracker ID
    String result
      = ("/" + DONE_DIRECTORY_FORMAT_DIRNAME
         + "/*");   // job tracker instance ID

    // date
    result = result + "/*/*/*";  // YYYY/MM/DD ;

    return result;
  }

  /**
   * Record types are identifiers for each line of log in history files. 
   * A record type appears as the first token in a single line of log. 
   */
  public static enum RecordTypes {
    Jobtracker, Job, Task, MapAttempt, ReduceAttempt, Meta
  }

  /**
   * Job history files contain key="value" pairs, where keys belong to this enum. 
   * It acts as a global namespace for all keys. 
   */
  public static enum Keys { 
    JOBTRACKERID,
    START_TIME, FINISH_TIME, JOBID, JOBNAME, USER, JOBCONF, SUBMIT_TIME, 
    LAUNCH_TIME, TOTAL_MAPS, TOTAL_REDUCES, FAILED_MAPS, FAILED_REDUCES, 
    FINISHED_MAPS, FINISHED_REDUCES, JOB_STATUS, TASKID, HOSTNAME, TASK_TYPE, 
    ERROR, TASK_ATTEMPT_ID, TASK_STATUS, COPY_PHASE, SORT_PHASE, REDUCE_PHASE, 
    SHUFFLE_FINISHED, SORT_FINISHED, COUNTERS, SPLITS, JOB_PRIORITY, HTTP_PORT, 
    TRACKER_NAME, STATE_STRING, VERSION, MAP_COUNTERS, REDUCE_COUNTERS,
    VIEW_JOB, MODIFY_JOB, JOB_QUEUE, FAIL_REASON, LOCALITY, AVATAAR,
    WORKFLOW_ID, WORKFLOW_NAME, WORKFLOW_NODE_NAME, WORKFLOW_ADJACENCIES,
    WORKFLOW_TAGS
  }

  /**
   * This enum contains some of the values commonly used by history log events. 
   * since values in history can only be strings - Values.name() is used in 
   * most places in history file. 
   */
  public static enum Values {
    SUCCESS, FAILED, KILLED, MAP, REDUCE, CLEANUP, RUNNING, PREP, SETUP
  }

  /**
   * Initialize JobHistory files. 
   * @param conf Jobconf of the job tracker.
   * @param hostname jobtracker's hostname
   * @param jobTrackerStartTime jobtracker's start time
   */
  public static void init(JobTracker jobTracker, JobConf conf,
             String hostname, long jobTrackerStartTime) throws IOException {
    initLogDir(conf);
    SERIAL_NUMBER_LOW_DIGITS = 3;
    SERIAL_NUMBER_FORMAT = ("%0"
       + (SERIAL_NUMBER_DIRECTORY_DIGITS + SERIAL_NUMBER_LOW_DIGITS)
       + "d");
    JOBTRACKER_UNIQUE_STRING = hostname + "_" + 
                                  String.valueOf(jobTrackerStartTime) + "_";
    jobtrackerHostname = hostname;
    Path logDir = new Path(LOG_DIR);
    if (!LOGDIR_FS.exists(logDir)){
      if (!LOGDIR_FS.mkdirs(logDir, new FsPermission(HISTORY_DIR_PERMISSION))) {
        throw new IOException("Mkdirs failed to create " + logDir.toString());
      }
    } else { // directory exists
      checkDirectoryPermissions(LOGDIR_FS, logDir, "hadoop.job.history.location");
    }
    conf.set("hadoop.job.history.location", LOG_DIR);
    // set the job history block size (default is 3MB)
    jobHistoryBlockSize = 
      conf.getLong("mapred.jobtracker.job.history.block.size", 
                   3 * 1024 * 1024);
    jtConf = conf;

    // queue and job level security is enabled on the mapreduce cluster or not
    aclsEnabled = conf.getBoolean(JobConf.MR_ACLS_ENABLED, false);

    // initialize the file manager
    fileManager = new JobHistoryFilesManager(conf, jobTracker);
  }

  private static void initLogDir(JobConf conf) throws IOException {
    LOG_DIR = conf.get("hadoop.job.history.location" ,
      "file:///" + new File(
      System.getProperty("hadoop.log.dir")).getAbsolutePath()
      + File.separator + "history");
    Path logDir = new Path(LOG_DIR);
    LOGDIR_FS = logDir.getFileSystem(conf);
  }

  static void initDone(JobConf conf, FileSystem fs) throws IOException {
    initDone(conf, fs, true);
  }

  static void initDone(JobConf conf, FileSystem fs,
                                     boolean setup)
      throws IOException {
    //if completed job history location is set, use that
    String doneLocation = conf.
                     get("mapred.job.tracker.history.completed.location");
    if (doneLocation != null) {
      DONE = fs.makeQualified(new Path(doneLocation));
      DONEDIR_FS = fs;
    } else {
      if (!setup) {
        initLogDir(conf);
      }
      DONE = new Path(LOG_DIR, "done");
      DONEDIR_FS = LOGDIR_FS;
    }
    Path versionSubdir = new Path(DONE, DONE_DIRECTORY_FORMAT_DIRNAME);
    //If not already present create the done folder with appropriate 
    //permission
    if (!DONEDIR_FS.exists(DONE)) {
      LOG.info("Creating DONE folder at "+ DONE);
      if (!DONEDIR_FS.mkdirs(DONE, 
          new FsPermission(HISTORY_DIR_PERMISSION))) {
        throw new IOException("Mkdirs failed to create " + DONE.toString());
      }

      if (!DONEDIR_FS.exists(versionSubdir)) {
        if (!DONEDIR_FS.mkdirs(versionSubdir,
                               new FsPermission(HISTORY_DIR_PERMISSION))) {
          throw new IOException("Mkdirs failed to create " + versionSubdir);
        }
      }
    } else { // directory exists. Checks version subdirectory permissions as
      // well.
      checkDirectoryPermissions(DONEDIR_FS, DONE,
          "mapred.job.tracker.history.completed.location");
      if (DONEDIR_FS.exists(versionSubdir))
        checkDirectoryPermissions(DONEDIR_FS, versionSubdir,
            "mapred.job.tracker.history.completed.location-versionsubdir");
    }

    if (!setup) {
      return;
    }

    fileManager.start();
    
    HistoryCleaner.cleanupFrequency =
    	      conf.getLong("mapreduce.jobhistory.cleaner.interval-ms",
    	      HistoryCleaner.DEFAULT_CLEANUP_FREQUENCY);
    HistoryCleaner.maxAgeOfHistoryFiles =
    	      conf.getLong("mapreduce.jobhistory.max-age-ms",
    	      HistoryCleaner.DEFAULT_HISTORY_MAX_AGE);
    LOG.info(String.format("Job History MaxAge is %d ms (%.2f days), " +
    	      "Cleanup Frequency is %d ms (%.2f days)",
    	      HistoryCleaner.maxAgeOfHistoryFiles,
    	      ((float) HistoryCleaner.maxAgeOfHistoryFiles)/HistoryCleaner.ONE_DAY_IN_MS,
    	      HistoryCleaner.cleanupFrequency,
    	      ((float) HistoryCleaner.cleanupFrequency)/HistoryCleaner.ONE_DAY_IN_MS));
  }

  /**
   * @param FileSystem
   * @param Path
   * @param configKey 
   * @throws IOException
   * @throws DiskErrorException
   */
  static void checkDirectoryPermissions(FileSystem fs, Path path,
      String configKey) throws IOException, DiskErrorException {
    FileStatus stat = fs.getFileStatus(path);
    FsPermission actual = stat.getPermission();
    if (!stat.isDir())
      throw new DiskErrorException(configKey + " - not a directory: "
          + path.toString());
    FsAction user = actual.getUserAction();
    if (!user.implies(FsAction.READ))
      throw new DiskErrorException("bad " + configKey
          + "- directory is not readable: " + path.toString());
    if (!user.implies(FsAction.WRITE))
      throw new DiskErrorException("bad " + configKey
          + "- directory is not writable " + path.toString());
  }

  /**
   * Manages job-history's meta information such as version etc.
   * Helps in logging version information to the job-history and recover
   * version information from the history. 
   */
  static class MetaInfoManager implements Listener {
    private long version = 0L;
    private KeyValuePair pairs = new KeyValuePair();
    
    // Extract the version of the history that was used to write the history
    public MetaInfoManager(String line) throws IOException {
      if (null != line) {
        // Parse the line
        parseLine(line, this, false);
      }
    }
    
    // Get the line delimiter
    char getLineDelim() {
      if (version == 0) {
        return '"';
      } else {
        return LINE_DELIMITER_CHAR;
      }
    }
    
    // Checks if the values are escaped or not
    boolean isValueEscaped() {
      // Note that the values are not escaped in version 0
      return version != 0;
    }
    
    public void handle(RecordTypes recType, Map<Keys, String> values) 
    throws IOException {
      // Check if the record is of type META
      if (RecordTypes.Meta == recType) {
        pairs.handle(values);
        version = pairs.getLong(Keys.VERSION); // defaults to 0
      }
    }
    
    /**
     * Logs history meta-info to the history file. This needs to be called once
     * per history file. 
     * @param jobId job id, assigned by jobtracker. 
     */
    static void logMetaInfo(ArrayList<PrintWriter> writers){
      if (null != writers){
         JobHistory.log(writers, RecordTypes.Meta, 
             new Keys[] {Keys.VERSION},
             new String[] {String.valueOf(VERSION)}); 
      }
    }
  }
  
  /** Escapes the string especially for {@link JobHistory}
   */
  static String escapeString(String data) {
    return StringUtils.escapeString(data, StringUtils.ESCAPE_CHAR, 
                                    charsToEscape);
  }
  
  /**
   * Parses history file and invokes Listener.handle() for 
   * each line of history. It can be used for looking through history
   * files for specific items without having to keep whole history in memory. 
   * @param path path to history file
   * @param l Listener for history events 
   * @param fs FileSystem where history file is present
   * @throws IOException
   */
  public static void parseHistoryFromFS(String path, Listener l, FileSystem fs)
  throws IOException{
    FSDataInputStream in = fs.open(new Path(path));
    BufferedReader reader = new BufferedReader(new InputStreamReader (in));
    try {
      String line = null; 
      StringBuffer buf = new StringBuffer(); 
      
      // Read the meta-info line. Note that this might a jobinfo line for files
      // written with older format
      line = reader.readLine();
      
      // Check if the file is empty
      if (line == null) {
        return;
      }
      
      // Get the information required for further processing
      MetaInfoManager mgr = new MetaInfoManager(line);
      boolean isEscaped = mgr.isValueEscaped();
      String lineDelim = String.valueOf(mgr.getLineDelim());  
      String escapedLineDelim = 
        StringUtils.escapeString(lineDelim, StringUtils.ESCAPE_CHAR, 
                                 mgr.getLineDelim());
      
      do {
        buf.append(line); 
        if (!line.trim().endsWith(lineDelim) 
            || line.trim().endsWith(escapedLineDelim)) {
          buf.append("\n");
          continue; 
        }
        parseLine(buf.toString(), l, isEscaped);
        buf = new StringBuffer(); 
      } while ((line = reader.readLine())!= null);
    } finally {
      try { reader.close(); } catch (IOException ex) {}
    }
  }

  /**
   * Parse a single line of history. 
   * @param line
   * @param l
   * @throws IOException
   */
  private static void parseLine(String line, Listener l, boolean isEscaped) 
  throws IOException{
    // extract the record type 
    int idx = line.indexOf(' '); 
    String recType = line.substring(0, idx);
    String data = line.substring(idx+1, line.length());
    
    Matcher matcher = pattern.matcher(data); 
    Map<Keys,String> parseBuffer = new HashMap<Keys, String>();

    while(matcher.find()){
      String tuple = matcher.group(0);
      String []parts = StringUtils.split(tuple, StringUtils.ESCAPE_CHAR, '=');
      String value = parts[1].substring(1, parts[1].length() -1);
      if (isEscaped) {
        value = StringUtils.unEscapeString(value, StringUtils.ESCAPE_CHAR,
                                           charsToEscape);
      }
      parseBuffer.put(Keys.valueOf(parts[0]), value);
    }

    l.handle(RecordTypes.valueOf(recType), parseBuffer); 
    
    parseBuffer.clear(); 
  }
  
  
  /**
   * Log a raw record type with keys and values. This is method is generally not used directly. 
   * @param recordType type of log event
   * @param key key
   * @param value value
   */
  
  static void log(PrintWriter out, RecordTypes recordType, Keys key, 
                  String value){
    value = escapeString(value);
    out.println(recordType.name() + DELIMITER + key + "=\"" + value + "\""
                + DELIMITER + LINE_DELIMITER_CHAR); 
  }
  
  /**
   * Log a number of keys and values with record. the array length of keys and values
   * should be same. 
   * @param recordType type of log event
   * @param keys type of log event
   * @param values type of log event
   */

  /**
   * Log a number of keys and values with record. the array length of keys and values
   * should be same. 
   * @param recordType type of log event
   * @param keys type of log event
   * @param values type of log event
   */

  static void log(ArrayList<PrintWriter> writers, RecordTypes recordType, 
                  Keys[] keys, String[] values) {
    log(writers, recordType, keys, values, null);
  }

  static class JobHistoryLogger {
    static final Log LOG = LogFactory.getLog(JobHistoryLogger.class);
  }
  
  /**
   * Log a number of keys and values with record. the array length of keys and values
   * should be same. 
   * @param recordType type of log event
   * @param keys type of log event
   * @param values type of log event
   * @param JobID jobid of the job  
   */

  static void log(ArrayList<PrintWriter> writers, RecordTypes recordType, 
                  Keys[] keys, String[] values, JobID id) {

    // First up calculate the length of buffer, so that we are performant
    // enough.
    int length = recordType.name().length() + keys.length * 4 + 2;
    for (int i = 0; i < keys.length; i++) { 
      values[i] = escapeString(values[i]);
      length += values[i].length() + keys[i].toString().length();
    }

    // We have the length of the buffer, now construct it.
    StringBuilder builder = new StringBuilder(length);
    builder.append(recordType.name());
    builder.append(DELIMITER); 
    for(int i =0; i< keys.length; i++){
      builder.append(keys[i]);
      builder.append("=\"");
      builder.append(values[i]);
      builder.append("\"");
      builder.append(DELIMITER); 
    }
    builder.append(LINE_DELIMITER_CHAR);

    String logLine = builder.toString();
    for (Iterator<PrintWriter> iter = writers.iterator(); iter.hasNext();) {
      PrintWriter out = iter.next();
      out.println(logLine);
      if (out.checkError() && id != null) {
        LOG.info("Logging failed for job " + id + "removing PrintWriter from FileManager");
        iter.remove();
      }
    }
    if (recordType != RecordTypes.Meta) {
      JobHistoryLogger.LOG.debug(logLine);
    }
  }
  
  /**
   * Get the history location
   */
  static Path getJobHistoryLocation() {
    return new Path(LOG_DIR);
  } 
  
  /**
   * Get the history location for completed jobs
   */
  static Path getCompletedJobHistoryLocation() {
    return DONE;
  }

  static int serialNumberDirectoryDigits() {
    return SERIAL_NUMBER_DIRECTORY_DIGITS;
  }

  static int serialNumberTotalDigits() {
    return serialNumberDirectoryDigits() + SERIAL_NUMBER_LOW_DIGITS;
  }

  /**
   * Get the 
   */
  
  /**
   * Base class contais utility stuff to manage types key value pairs with enums. 
   */
  static class KeyValuePair{
    private Map<Keys, String> values = new HashMap<Keys, String>(); 

    /**
     * Get 'String' value for given key. Most of the places use Strings as 
     * values so the default get' method returns 'String'.  This method never returns 
     * null to ease on GUIs. if no value is found it returns empty string ""
     * @param k 
     * @return if null it returns empty string - "" 
     */
    public String get(Keys k){
      String s = values.get(k); 
      return s == null ? "" : s; 
    }
    /**
     * Convert value from history to int and return. 
     * if no value is found it returns 0.
     * @param k key 
     */
    public int getInt(Keys k){
      String s = values.get(k); 
      if (null != s){
        return Integer.parseInt(s);
      }
      return 0; 
    }
    /**
     * Convert value from history to int and return. 
     * if no value is found it returns 0.
     * @param k
     */
    public long getLong(Keys k){
      String s = values.get(k); 
      if (null != s){
        return Long.parseLong(s);
      }
      return 0; 
    }
    /**
     * Set value for the key. 
     * @param k
     * @param s
     */
    public void set(Keys k, String s){
      values.put(k, s); 
    }
    /**
     * Adds all values in the Map argument to its own values. 
     * @param m
     */
    public void set(Map<Keys, String> m){
      values.putAll(m);
    }
    /**
     * Reads values back from the history, input is same Map as passed to Listener by parseHistory().  
     * @param values
     */
    public synchronized void handle(Map<Keys, String> values){
      set(values); 
    }
    /**
     * Returns Map containing all key-values. 
     */
    public Map<Keys, String> getValues(){
      return values; 
    }
  }

  // hasMismatches is just used to return a second value if you want
  // one.  I would have used MutableBoxedBoolean if such had been provided.
  static Path[] filteredStat2Paths
          (FileStatus[] stats, boolean dirs, AtomicBoolean hasMismatches) {
    int resultCount = 0;

    if (hasMismatches == null) {
      hasMismatches = new AtomicBoolean(false);
    }

    for (int i = 0; i < stats.length; ++i) {
      if (stats[i].isDir() == dirs) {
        stats[resultCount++] = stats[i];
      } else {
        hasMismatches.set(true);
      }
    }

    Path[] paddedResult = FileUtil.stat2Paths(stats);

    Path[] result = new Path[resultCount];

    System.arraycopy(paddedResult, 0, result, 0, resultCount);

    return result;
  }

  static FileStatus[] localGlobber
        (FileSystem fs, Path root, String tail) 
      throws IOException {
    return localGlobber(fs, root, tail, null);
  }

  static FileStatus[] localGlobber
        (FileSystem fs, Path root, String tail, PathFilter filter) 
      throws IOException {
    return localGlobber(fs, root, tail, filter, null);
  }
  
  private static FileStatus[] nullToEmpty(FileStatus[] result) {
    return result == null ? new FileStatus[0] : result;
  }
      
  private static FileStatus[] listFilteredStatus
        (FileSystem fs, Path root, PathFilter filter)
     throws IOException {
    return filter == null ? fs.listStatus(root) : fs.listStatus(root, filter);
  }

  // hasMismatches is just used to return a second value if you want
  // one.  I would have used MutableBoxedBoolean if such had been provided.
  static FileStatus[] localGlobber
    (FileSystem fs, Path root, String tail, PathFilter filter,
     AtomicBoolean hasFlatFiles)
      throws IOException {
    if (tail.equals("")) {
      return nullToEmpty(listFilteredStatus(fs, root, filter));
    }

    if (tail.startsWith("/*")) {
      Path[] subdirs = filteredStat2Paths(nullToEmpty(fs.listStatus(root)),
                                          true, hasFlatFiles);

      FileStatus[][] subsubdirs = new FileStatus[subdirs.length][];

      int subsubdirCount = 0;

      if (subsubdirs.length == 0) {
        return new FileStatus[0];
      }

      String newTail = tail.substring(2);

      for (int i = 0; i < subdirs.length; ++i) {
        subsubdirs[i] = localGlobber(fs, subdirs[i], newTail, filter, null);
        subsubdirCount += subsubdirs[i].length;
      }

      FileStatus[] result = new FileStatus[subsubdirCount];

      int segmentStart = 0;

      for (int i = 0; i < subsubdirs.length; ++i) {
        System.arraycopy(subsubdirs[i], 0, result, segmentStart, subsubdirs[i].length);
        segmentStart += subsubdirs[i].length;
      }

      return result;
    }

    if (tail.startsWith("/")) {
      int split = tail.indexOf('/', 1);

      if (split < 0) {
        return nullToEmpty
          (listFilteredStatus(fs, new Path(root, tail.substring(1)), filter));
      } else {
        String thisSegment = tail.substring(1, split);
        String newTail = tail.substring(split);
        return localGlobber
          (fs, new Path(root, thisSegment), newTail, filter, hasFlatFiles);
      }
    }

    IOException e = new IOException("localGlobber: bad tail");

    throw e;
  }

  static Path confPathFromLogFilePath(Path logFile) {
    String jobId = jobIdNameFromLogFileName(logFile.getName());
      
    Path logDir = logFile.getParent();

    return new Path(logDir, jobId + CONF_FILE_NAME_SUFFIX);
  }

  static String jobIdNameFromLogFileName(String logFileName) {
    String[] jobDetails = logFileName.split("_");
    return jobDetails[0] + "_" + jobDetails[1] + "_" + jobDetails[2];
  }

  static String userNameFromLogFileName(String logFileName) {
    String[] jobDetails = logFileName.split("_");
    return jobDetails[3];
  }

  static String jobNameFromLogFileName(String logFileName) {
    String[] jobDetails = logFileName.split("_");
    return jobDetails[4];
  }


  // This code will be inefficient if the subject contains dozens of underscores
  static String escapeUnderscores(String escapee) {
    return replaceStringInstances(escapee, "_", UNDERSCORE_ESCAPE);
  }

  static String nonOccursString(String logFileName) {
    int adHocIndex = 0;

    String unfoundString = "q" + adHocIndex;

    while (logFileName.contains(unfoundString)) {
      unfoundString = "q" + ++adHocIndex;
    }

    return unfoundString + "q";
  }

  // I tolerate this code because I expect a low number of
  // occurrences in a relatively short string
  static String replaceStringInstances
      (String logFileName, String old, String replacement) {
    int index = logFileName.indexOf(old);

    while (index > 0) {
      logFileName = (logFileName.substring(0, index)
                     + replacement
                     + replaceStringInstances
                         (logFileName.substring(index + old.length()),
                          old, replacement));

      index = logFileName.indexOf(old);
    }

    return logFileName;
  }      

  
  /**
   * Helper class for logging or reading back events related to job start, finish or failure. 
   */
  public static class JobInfo extends KeyValuePair{
    
    private Map<String, Task> allTasks = new TreeMap<String, Task>();
    private Map<JobACL, AccessControlList> jobACLs =
        new HashMap<JobACL, AccessControlList>();
    private String queueName = null;// queue to which this job was submitted to
    
    /** Create new JobInfo */
    public JobInfo(String jobId){ 
      set(Keys.JOBID, jobId);  
    }

    /**
     * Returns all map and reduce tasks <taskid-Task>. 
     */
    public Map<String, Task> getAllTasks() { return allTasks; }

    /**
     * Get the job acls.
     * 
     * @return a {@link Map} from {@link JobACL} to {@link AccessControlList}
     */
    public Map<JobACL, AccessControlList> getJobACLs() {
      return jobACLs;
    }

    @Override
    public synchronized void handle(Map<Keys, String> values) {
      if (values.containsKey(Keys.SUBMIT_TIME)) {// job submission
        // construct the job ACLs
        String viewJobACL = values.get(Keys.VIEW_JOB);
        String modifyJobACL = values.get(Keys.MODIFY_JOB);
        if (viewJobACL != null) {
          jobACLs.put(JobACL.VIEW_JOB, new AccessControlList(viewJobACL));
        }
        if (modifyJobACL != null) {
          jobACLs.put(JobACL.MODIFY_JOB, new AccessControlList(modifyJobACL));
        }
        // get the job queue name
        queueName = values.get(Keys.JOB_QUEUE);
      }
      super.handle(values);
    }

    String getJobQueue() {
      return queueName;
    }

    /**
     * Get the path of the locally stored job file
     * @param jobId id of the job
     * @return the path of the job file on the local file system 
     */
    public static String getLocalJobFilePath(JobID jobId){
      return System.getProperty("hadoop.log.dir") + File.separator +
               jobId + CONF_FILE_NAME_SUFFIX;
    }
                      
    
    /**
     * Helper function to encode the URL of the path of the job-history
     * log file. 
     * 
     * @param logFile path of the job-history file
     * @return URL encoded path
     * @throws IOException
     */
    public static String encodeJobHistoryFilePath(String logFile)
    throws IOException {
      Path rawPath = new Path(logFile);
      String encodedFileName = null;
      try {
        encodedFileName = URLEncoder.encode(rawPath.getName(), "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      
      Path encodedPath = new Path(rawPath.getParent(), encodedFileName);
      return encodedPath.toString();
    }
    
    /**
     * Helper function to encode the URL of the filename of the job-history 
     * log file.
     * 
     * @param logFileName file name of the job-history file
     * @return URL encoded filename
     * @throws IOException
     */
    public static String encodeJobHistoryFileName(String logFileName)
    throws IOException {
      String replacementUnderscoreEscape = null;

      if (logFileName.contains(UNDERSCORE_ESCAPE)) {
        replacementUnderscoreEscape = nonOccursString(logFileName);

        logFileName = replaceStringInstances
          (logFileName, UNDERSCORE_ESCAPE, replacementUnderscoreEscape);
      }

      String encodedFileName = null;
      try {
        encodedFileName = URLEncoder.encode(logFileName, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      
      if (replacementUnderscoreEscape != null) {
        encodedFileName = replaceStringInstances
          (encodedFileName, replacementUnderscoreEscape, UNDERSCORE_ESCAPE);
      }

      return encodedFileName;
    }
    
    /**
     * Helper function to decode the URL of the filename of the job-history 
     * log file.
     * 
     * @param logFileName file name of the job-history file
     * @return URL decoded filename
     * @throws IOException
     */
    public static String decodeJobHistoryFileName(String logFileName)
    throws IOException {
      String decodedFileName = null;
      try {
        decodedFileName = URLDecoder.decode(logFileName, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      return decodedFileName;
    }
    
    /**
     * Get the job name from the job conf
     */
    static String getJobName(JobConf jobConf) {
      String jobName = jobConf.getJobName();
      if (jobName == null || jobName.length() == 0) {
        jobName = "NA";
      }
      return jobName;
    }
    
    /**
     * Get the user name from the job conf
     */
    public static String getUserName(JobConf jobConf) {
      String user = jobConf.getUser();
      if (user == null || user.length() == 0) {
        user = "NA";
      }
      return user;
    }
    
    /**
     * Get the workflow adjacencies from the job conf
     * The string returned is of the form "key"="value" "key"="value" ...
     */
    public static String getWorkflowAdjacencies(Configuration conf) {
      int prefixLen = JobConf.WORKFLOW_ADJACENCY_PREFIX_STRING.length();
      Map<String,String> adjacencies = 
          conf.getValByRegex(JobConf.WORKFLOW_ADJACENCY_PREFIX_PATTERN);
      if (adjacencies.isEmpty())
        return "";
      int size = 0;
      for (Entry<String,String> entry : adjacencies.entrySet()) {
        int keyLen = entry.getKey().length();
        size += keyLen - prefixLen;
        size += entry.getValue().length() + 6;
      }
      StringBuilder sb = new StringBuilder(size);
      for (Entry<String,String> entry : adjacencies.entrySet()) {
        int keyLen = entry.getKey().length();
        sb.append("\"");
        sb.append(escapeString(entry.getKey().substring(prefixLen, keyLen)));
        sb.append("\"=\"");
        sb.append(escapeString(entry.getValue()));
        sb.append("\" ");
      }
      return sb.toString();
    }
    
    /**
     * Get the job history file path given the history filename
     */
    public static Path getJobHistoryLogLocation(String logFileName)
    {
      return LOG_DIR == null ? null : new Path(LOG_DIR, logFileName);
    }

    /**
     * Get the user job history file path
     */
    public static Path getJobHistoryLogLocationForUser(String logFileName, 
                                                       JobConf jobConf) {
      // find user log directory 
      Path userLogFile = null;
      Path outputPath = FileOutputFormat.getOutputPath(jobConf);
      String userLogDir = jobConf.get("hadoop.job.history.user.location",
                                      outputPath == null 
                                      ? null 
                                      : outputPath.toString());
      if ("none".equals(userLogDir)) {
        userLogDir = null;
      }
      if (userLogDir != null) {
        userLogDir = userLogDir + Path.SEPARATOR + "_logs" + Path.SEPARATOR 
                     + "history";
        userLogFile = new Path(userLogDir, logFileName);
      }
      return userLogFile;
    }

    /**
     * Generates the job history filename for a new job
     */
    private static String getNewJobHistoryFileName(JobConf jobConf, JobID id, long submitTime) {
      return
        id.toString() + "_"
        + submitTime + "_"
        + escapeUnderscores(getUserName(jobConf)) + "_" 
        + escapeUnderscores(trimJobName(getJobName(jobConf)));
    }
    
    /**
     * Trims the job-name if required
     */
    private static String trimJobName(String jobName) {
      if (jobName.length() > JOB_NAME_TRIM_LENGTH) {
        jobName = jobName.substring(0, JOB_NAME_TRIM_LENGTH);
      }
      return jobName;
    }
    
    private static String escapeRegexChars( String string ) {
      return "\\Q"+string.replaceAll("\\\\E", "\\\\E\\\\\\\\E\\\\Q")+"\\E";
    }

    /**
     * Recover the job history filename from the history folder. 
     * Uses the following pattern
     *    $jt-hostname_[0-9]*_$job-id_$user_$job-name*
     * @param jobConf the job conf
     * @param id job id
     */
    public static synchronized String getJobHistoryFileName(JobConf jobConf, 
                                                            JobID id) 
    throws IOException {                    
      return getJobHistoryFileName(jobConf, id, new Path(LOG_DIR), LOGDIR_FS);
    }

    // Returns that portion of the pathname that sits under the DONE directory
    static synchronized String getDoneJobHistoryFileName(JobConf jobConf, 
        JobID id) throws IOException {
      if (DONE == null) {
        return null;
      }
      return getJobHistoryFileName(jobConf, id, DONE, DONEDIR_FS);
    }
    
    /**
     * @param dir The directory where to search.
     */
    private static synchronized String getJobHistoryFileName(JobConf jobConf, 
                                          JobID id, Path dir, FileSystem fs) 
    throws IOException {
      String user =  getUserName(jobConf);
      String jobName = trimJobName(getJobName(jobConf));
      if (LOG_DIR == null) {
        return null;
      }

      // Make the pattern matching the job's history file

      final String regexp
        = id.toString() + "_" + DIGITS + "_" + user + "_"
             + escapeRegexChars(jobName) + "+";
      
      final Pattern historyFilePattern = Pattern.compile(regexp);

      // a path filter that matches 4 parts of the filenames namely
      //  - jt-hostname
      //  - job-id
      //  - username
      //  - jobname
      PathFilter filter = new PathFilter() {
        public boolean accept(Path path) {
          String unescapedFileName = path.getName();
          String fileName = null;
          try {
            fileName = decodeJobHistoryFileName(unescapedFileName);
          } catch (IOException ioe) {
            LOG.info("Error while decoding history file " + fileName + "."
                     + " Ignoring file.", ioe);
            return false;
          }

          return historyFilePattern.matcher(fileName).find();
        }
      };

      FileStatus[] statuses = null;

      if (dir == DONE) {
        final String scanTail
          = (DONE_BEFORE_SERIAL_TAIL
             + "/" + serialNumberDirectoryComponent(id));

        if (LOG.isDebugEnabled()) {
          LOG.debug("JobHistory.getJobHistoryFileName DONE dir: scanning "
              + scanTail);
          if (LOG.isTraceEnabled()) {
            LOG.trace(Thread.currentThread().getStackTrace());
          }
        }

        statuses = localGlobber(fs, DONE, scanTail, filter);
      } else {
        statuses = fs.listStatus(dir, filter);
      }
      
      String filename = null;
      if (statuses == null || statuses.length == 0) {
        LOG.info("Nothing to recover for job " + id);
      } else {
        // return filename considering that fact the name can be a 
        // secondary filename like filename.recover
        filename = getPrimaryFilename(statuses[0].getPath().getName(), jobName);
        if (dir == DONE) {
          Path parent = statuses[0].getPath().getParent();
          String parentPathName = parent.toString();
          String donePathName = DONE.toString();
          filename = (parentPathName.substring(donePathName.length() + Path.SEPARATOR.length())
                      + Path.SEPARATOR + filename);
        }
        
        LOG.info("Recovered job history filename for job " + id + " is " 
                 + filename);
      }
      return filename;
    }
    
    // removes all extra extensions from a filename and returns the core/primary
    // filename
    private static String getPrimaryFilename(String filename, String jobName) 
    throws IOException{
      filename = decodeJobHistoryFileName(filename);
      // Remove the '.recover' suffix if it exists
      if (filename.endsWith(jobName + SECONDARY_FILE_SUFFIX)) {
        int newLength = filename.length() - SECONDARY_FILE_SUFFIX.length();
        filename = filename.substring(0, newLength);
      }
      return encodeJobHistoryFileName(filename);
    }    

    /** Since there was a restart, there should be a master file and 
     * a recovery file. Once the recovery is complete, the master should be 
     * deleted as an indication that the recovery file should be treated as the 
     * master upon completion or next restart.
     * @param fileName the history filename that needs checkpointing
     * @param conf Job conf
     * @throws IOException
     */
    static synchronized void checkpointRecovery(String fileName, JobConf conf) 
    throws IOException {
      Path logPath = JobHistory.JobInfo.getJobHistoryLogLocation(fileName);
      if (logPath != null) {
        LOG.info("Deleting job history file " + logPath.getName());
        LOGDIR_FS.delete(logPath, false);
      }
      // do the same for the user file too
      logPath = JobHistory.JobInfo.getJobHistoryLogLocationForUser(fileName, 
                                                                   conf);
      if (logPath != null) {
        FileSystem fs = logPath.getFileSystem(conf);
        fs.delete(logPath, false);
      }
    }
    
    static String getSecondaryJobHistoryFile(String filename) 
    throws IOException {
      return encodeJobHistoryFileName(
          decodeJobHistoryFileName(filename) + SECONDARY_FILE_SUFFIX);
    }
    
    /** Selects one of the two files generated as a part of recovery. 
     * The thumb rule is that always select the oldest file. 
     * This call makes sure that only one file is left in the end. 
     * @param conf job conf
     * @param logFilePath Path of the log file
     * @throws IOException 
     */
    public synchronized static Path recoverJobHistoryFile(JobConf conf, 
                                                          Path logFilePath) 
    throws IOException {
      Path ret;
      String logFileName = logFilePath.getName();
      String tmpFilename = getSecondaryJobHistoryFile(logFileName);
      Path logDir = logFilePath.getParent();
      Path tmpFilePath = new Path(logDir, tmpFilename);
      if (LOGDIR_FS.exists(logFilePath)) {
        LOG.info(logFileName + " exists!");
        if (LOGDIR_FS.exists(tmpFilePath)) {
          LOG.info("Deleting " + tmpFilename 
                   + "  and using " + logFileName + " for recovery.");
          LOGDIR_FS.delete(tmpFilePath, false);
        }
        ret = tmpFilePath;
      } else {
        LOG.info(logFileName + " doesnt exist! Using " 
                 + tmpFilename + " for recovery.");
        if (LOGDIR_FS.exists(tmpFilePath)) {
          LOG.info("Renaming " + tmpFilename + " to " + logFileName);
          LOGDIR_FS.rename(tmpFilePath, logFilePath);
          ret = tmpFilePath;
        } else {
          ret = logFilePath;
        }
      }

      // do the same for the user files too
      logFilePath = getJobHistoryLogLocationForUser(logFileName, conf);
      if (logFilePath != null) {
        FileSystem fs = logFilePath.getFileSystem(conf);
        logDir = logFilePath.getParent();
        tmpFilePath = new Path(logDir, tmpFilename);
        if (fs.exists(logFilePath)) {
          LOG.info(logFileName + " exists!");
          if (fs.exists(tmpFilePath)) {
            LOG.info("Deleting " + tmpFilename + "  and making " + logFileName 
                     + " as the master history file for user.");
            fs.delete(tmpFilePath, false);
          }
        } else {
          LOG.info(logFileName + " doesnt exist! Using " 
                   + tmpFilename + " as the master history file for user.");
          if (fs.exists(tmpFilePath)) {
            LOG.info("Renaming " + tmpFilename + " to " + logFileName 
                     + " in user directory");
            fs.rename(tmpFilePath, logFilePath);
          }
        }
      }
      
      return ret;
    }

    /** Finalize the recovery and make one file in the end. 
     * This invloves renaming the recover file to the master file.
     * Note that this api should be invoked only if recovery is involved.
     * @param id Job id  
     * @param conf the job conf
     * @throws IOException
     */
    static synchronized void finalizeRecovery(JobID id, JobConf conf)
    throws IOException {
       Path tmpLogPath = fileManager.getHistoryFile(id);
       if (tmpLogPath == null) {
         if (LOG.isDebugEnabled()) {
           LOG.debug("No file for job with " + id + " found in cache!");
         }
         return;
       }
       String tmpLogFileName = tmpLogPath.getName();
       
       // get the primary filename from the cached filename
       String masterLogFileName = 
         getPrimaryFilename(tmpLogFileName, getJobName(conf));
       Path masterLogPath = new Path(tmpLogPath.getParent(), masterLogFileName);
       
       // rename the tmp file to the master file. Note that this should be 
       // done only when the file is closed and handles are released.
       LOG.info("Renaming " + tmpLogFileName + " to " + masterLogFileName);
       LOGDIR_FS.rename(tmpLogPath, masterLogPath);
       // update the cache
       fileManager.setHistoryFile(id, masterLogPath);

      // do the same for the user file too
      masterLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocationForUser(masterLogFileName,
                                                           conf);
      tmpLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocationForUser(tmpLogFileName, 
                                                           conf);
      if (masterLogPath != null) {
        FileSystem fs = masterLogPath.getFileSystem(conf);
        if (fs.exists(tmpLogPath)) {
          LOG.info("Renaming " + tmpLogFileName + " to " + masterLogFileName
                   + " in user directory");
          fs.rename(tmpLogPath, masterLogPath);
        }
      }
    }

    /**
     * Deletes job data from the local disk.
     * For now just deletes the localized copy of job conf
     */
    static void cleanupJob(JobID id) {
      String localJobFilePath =  JobInfo.getLocalJobFilePath(id);
      File f = new File (localJobFilePath);
      LOG.info("Deleting localized job conf at " + f);
      if (!f.delete()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to delete file " + f);
        }
      }
    }

    /**
     * Delete job conf from the history folder.
     */
    static void deleteConfFiles() throws IOException {
      LOG.info("Cleaning up config files from the job history folder");
      FileSystem fs = new Path(LOG_DIR).getFileSystem(jtConf);
      FileStatus[] status = fs.listStatus(new Path(LOG_DIR), CONF_FILTER);
      for (FileStatus s : status) {
        LOG.info("Deleting conf file " + s.getPath());
        fs.delete(s.getPath(), false);
      }
    }

    /**
     * Move the completed job into the completed folder.
     * This assumes that the jobhistory file is closed and all operations on the
     * jobhistory file is complete.
     * This *should* be the last call to jobhistory for a given job.
     */
    static void markCompleted(JobID id) throws IOException {
      fileManager.moveToDone(id);
    }

     /**
     * Log job submitted event to history. Creates a new file in history 
     * for the job. if history file creation fails, it disables history 
     * for all other events. 
     * @param jobId job id assigned by job tracker.
     * @param jobConf job conf of the job
     * @param jobConfPath path to job conf xml file in HDFS.
     * @param submitTime time when job tracker received the job
     * @throws IOException
     * @deprecated Use 
     *     {@link #logSubmitted(JobID, JobConf, String, long, boolean)} instead.
     */
    @Deprecated
    public static void logSubmitted(JobID jobId, JobConf jobConf, 
                                    String jobConfPath, long submitTime) 
    throws IOException {
      logSubmitted(jobId, jobConf, jobConfPath, submitTime, true);
    }
    
    public static void logSubmitted(JobID jobId, JobConf jobConf, 
                                    String jobConfPath, long submitTime, 
                                    boolean restarted) 
    throws IOException {
      FileSystem fs = null;
      String userLogDir = null;
      String jobUniqueString = jobId.toString();

      // Get the username and job name to be used in the actual log filename;
      // sanity check them too        
      String jobName = getJobName(jobConf);
      String user = getUserName(jobConf);
      
      // get the history filename
      String logFileName = null;
      if (restarted) {
        logFileName = getJobHistoryFileName(jobConf, jobId);
        if (logFileName == null) {
          logFileName =
            encodeJobHistoryFileName(getNewJobHistoryFileName
                                     (jobConf, jobId, submitTime));
        } else {
          String parts[] = logFileName.split("_");
          //TODO this is a hack :(
          // jobtracker-hostname_jobtracker-identifier_
          String jtUniqueString = parts[0] + "_" + parts[1] + "_";
          jobUniqueString = jobId.toString();
        }
      } else {
        logFileName = 
          encodeJobHistoryFileName(getNewJobHistoryFileName
                                   (jobConf, jobId, submitTime));
      }

      // setup the history log file for this job
      Path logFile = getJobHistoryLogLocation(logFileName);
      
      // find user log directory
      Path userLogFile = 
        getJobHistoryLogLocationForUser(logFileName, jobConf);
      PrintWriter writer = null;
      try{
        FSDataOutputStream out = null;
        if (LOG_DIR != null) {
          // create output stream for logging in hadoop.job.history.location
          if (restarted) {
            logFile = recoverJobHistoryFile(jobConf, logFile);
            logFileName = logFile.getName();
          }
          
          int defaultBufferSize = 
            LOGDIR_FS.getConf().getInt("io.file.buffer.size", 4096);
          out = LOGDIR_FS.create(logFile, 
                          new FsPermission(HISTORY_FILE_PERMISSION),
                          true, 
                          defaultBufferSize, 
                          LOGDIR_FS.getDefaultReplication(), 
                          jobHistoryBlockSize, null);
          writer = new PrintWriter(out);
          fileManager.addWriter(jobId, writer);

          // cache it ...
          fileManager.setHistoryFile(jobId, logFile);
        }
        if (userLogFile != null) {
          // Get the actual filename as recoverJobHistoryFile() might return
          // a different filename
          userLogDir = userLogFile.getParent().toString();
          userLogFile = new Path(userLogDir, logFileName);
          
          // create output stream for logging 
          // in hadoop.job.history.user.location
          fs = userLogFile.getFileSystem(jobConf);
 
          out = fs.create(userLogFile, true, 4096);
          writer = new PrintWriter(out);
          fileManager.addWriter(jobId, writer);
        }
        
        ArrayList<PrintWriter> writers = fileManager.getWriters(jobId);
        // Log the history meta info
        JobHistory.MetaInfoManager.logMetaInfo(writers);

        String viewJobACL = "*";
        String modifyJobACL = "*";
        if (aclsEnabled) {
          viewJobACL = jobConf.get(JobACL.VIEW_JOB.getAclName(), " ");
          modifyJobACL = jobConf.get(JobACL.MODIFY_JOB.getAclName(), " ");
        }
        //add to writer as well 
        JobHistory.log(writers, RecordTypes.Job,
                       new Keys[]{Keys.JOBID, Keys.JOBNAME, Keys.USER,
                                  Keys.SUBMIT_TIME, Keys.JOBCONF,
                                  Keys.VIEW_JOB, Keys.MODIFY_JOB,
                                  Keys.JOB_QUEUE, Keys.WORKFLOW_ID,
                                  Keys.WORKFLOW_NAME, Keys.WORKFLOW_NODE_NAME,
                                  Keys.WORKFLOW_ADJACENCIES,
                                  Keys.WORKFLOW_TAGS}, 
                       new String[]{jobId.toString(), jobName, user, 
                                    String.valueOf(submitTime) , jobConfPath,
                                    viewJobACL, modifyJobACL,
                                    jobConf.getQueueName(),
                                    jobConf.get(JobConf.WORKFLOW_ID, ""),
                                    jobConf.get(JobConf.WORKFLOW_NAME, ""),
                                    jobConf.get(JobConf.WORKFLOW_NODE_NAME, ""),
                                    getWorkflowAdjacencies(jobConf),
                                    jobConf.get(JobConf.WORKFLOW_TAGS, ""),
                                    }, 
                                    jobId
                      ); 
           
      }catch(IOException e){
        LOG.error("Failed creating job history log file for job " + jobId, e);
        if (writer != null) {
          fileManager.removeWriter(jobId, writer);
        }
      }
      // Always store job conf on local file system 
      String localJobFilePath =  JobInfo.getLocalJobFilePath(jobId); 
      File localJobFile = new File(localJobFilePath);
      FileOutputStream jobOut = null;
      try {
        jobOut = new FileOutputStream(localJobFile);
        jobConf.writeXml(jobOut);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Job conf for " + jobId + " stored at " 
                    + localJobFile.getAbsolutePath());
        }
      } catch (IOException ioe) {
        LOG.error("Failed to store job conf on the local filesystem ", ioe);
      } finally {
        if (jobOut != null) {
          try {
            jobOut.close();
          } catch (IOException ie) {
            LOG.info("Failed to close the job configuration file " 
                       + StringUtils.stringifyException(ie));
          }
        }
      }

      /* Storing the job conf on the log dir */
      Path jobFilePath = null;
      if (LOG_DIR != null) {
        jobFilePath = new Path(LOG_DIR + File.separator + 
                               jobUniqueString + CONF_FILE_NAME_SUFFIX);
        fileManager.setConfFile(jobId, jobFilePath);
      }
      Path userJobFilePath = null;
      if (userLogDir != null) {
        userJobFilePath = new Path(userLogDir + File.separator +
                                   jobUniqueString + CONF_FILE_NAME_SUFFIX);
      }
      FSDataOutputStream jobFileOut = null;
      try {
        if (LOG_DIR != null) {
          int defaultBufferSize = 
              LOGDIR_FS.getConf().getInt("io.file.buffer.size", 4096);
          if (!LOGDIR_FS.exists(jobFilePath)) {
            jobFileOut = LOGDIR_FS.create(jobFilePath, 
                                   new FsPermission(HISTORY_FILE_PERMISSION),
                                   true, 
                                   defaultBufferSize, 
                                   LOGDIR_FS.getDefaultReplication(), 
                                   LOGDIR_FS.getDefaultBlockSize(), null);
            jobConf.writeXml(jobFileOut);
            jobFileOut.close();
          }
        } 
        if (userLogDir != null) {
          fs = new Path(userLogDir).getFileSystem(jobConf);
          jobFileOut = fs.create(userJobFilePath);
          jobConf.writeXml(jobFileOut);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Job conf for " + jobId + " stored at " 
                    + jobFilePath + "and" + userJobFilePath );
        }
      } catch (IOException ioe) {
        LOG.error("Failed to store job conf in the log dir", ioe);
      } finally {
        if (jobFileOut != null) {
          try {
            jobFileOut.close();
          } catch (IOException ie) {
            LOG.info("Failed to close the job configuration file " 
                     + StringUtils.stringifyException(ie));
          }
        }
      } 
    }
    /**
     * Logs launch time of job. 
     * 
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     */
    public static void logInited(JobID jobId, long startTime, 
                                 int totalMaps, int totalReduces) {
      ArrayList<PrintWriter> writer = fileManager.getWriters(jobId); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Job, 
            new Keys[] {Keys.JOBID, Keys.LAUNCH_TIME, Keys.TOTAL_MAPS, 
                        Keys.TOTAL_REDUCES, Keys.JOB_STATUS},
            new String[] {jobId.toString(), String.valueOf(startTime), 
                          String.valueOf(totalMaps), 
                          String.valueOf(totalReduces), 
                          Values.PREP.name()}, jobId); 
      }
    }
    
   /**
     * Logs the job as RUNNING. 
     *
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     * @deprecated Use {@link #logInited(JobID, long, int, int)} and 
     * {@link #logStarted(JobID)}
     */
    @Deprecated
    public static void logStarted(JobID jobId, long startTime, 
                                  int totalMaps, int totalReduces) {
      logStarted(jobId);
    }
    
    /**
     * Logs job as running 
     * @param jobId job id, assigned by jobtracker. 
     */
    public static void logStarted(JobID jobId){
      ArrayList<PrintWriter> writer = fileManager.getWriters(jobId); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Job, 
            new Keys[] {Keys.JOBID, Keys.JOB_STATUS},
            new String[] {jobId.toString(),  
                          Values.RUNNING.name()}, jobId); 
      }
    }
    
    /**
     * Log job finished. closes the job file in history. 
     * @param jobId job id, assigned by jobtracker. 
     * @param finishTime finish time of job in ms. 
     * @param finishedMaps no of maps successfully finished. 
     * @param finishedReduces no of reduces finished sucessfully. 
     * @param failedMaps no of failed map tasks. 
     * @param failedReduces no of failed reduce tasks. 
     * @param counters the counters from the job
     */ 
    public static void logFinished(JobID jobId, long finishTime, 
                                   int finishedMaps, int finishedReduces,
                                   int failedMaps, int failedReduces,
                                   Counters mapCounters,
                                   Counters reduceCounters,
                                   Counters counters) {
        // close job file for this job
      ArrayList<PrintWriter> writer = fileManager.getWriters(jobId); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Job,          
                       new Keys[] {Keys.JOBID, Keys.FINISH_TIME, 
                                   Keys.JOB_STATUS, Keys.FINISHED_MAPS, 
                                   Keys.FINISHED_REDUCES,
                                   Keys.FAILED_MAPS, Keys.FAILED_REDUCES,
                                   Keys.MAP_COUNTERS, Keys.REDUCE_COUNTERS,
                                   Keys.COUNTERS},
                       new String[] {jobId.toString(),  Long.toString(finishTime), 
                                     Values.SUCCESS.name(), 
                                     String.valueOf(finishedMaps), 
                                     String.valueOf(finishedReduces),
                                     String.valueOf(failedMaps), 
                                     String.valueOf(failedReduces),
                                     mapCounters.makeEscapedCompactString(),
                                     reduceCounters.makeEscapedCompactString(),
                                     counters.makeEscapedCompactString()}, jobId);
        for (PrintWriter out : writer) {
          out.close();
        }
      }
      Thread historyCleaner  = new Thread(new HistoryCleaner());
      historyCleaner.start(); 
    }
    /**
     * Logs job failed event. Closes the job history log file. 
     * @param jobid job id
     * @param timestamp time when job failure was detected in ms.  
     * @param finishedMaps no finished map tasks. 
     * @param finishedReduces no of finished reduce tasks. 
     */
    public static void logFailed(JobID jobid, long timestamp, int finishedMaps, int finishedReduces, String failReason){
      ArrayList<PrintWriter> writer = fileManager.getWriters(jobid); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Job,
                       new Keys[] {Keys.JOBID, Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS, Keys.FINISHED_REDUCES, Keys.FAIL_REASON },
                       new String[] {jobid.toString(),  String.valueOf(timestamp), Values.FAILED.name(), String.valueOf(finishedMaps), 
                                     String.valueOf(finishedReduces), failReason}, jobid); 
        for (PrintWriter out : writer) {
          out.close();
        }
      }
    }
    /**
     * Logs job killed event. Closes the job history log file.
     * 
     * @param jobid
     *          job id
     * @param timestamp
     *          time when job killed was issued in ms.
     * @param finishedMaps
     *          no finished map tasks.
     * @param finishedReduces
     *          no of finished reduce tasks.
     */
    public static void logKilled(JobID jobid, long timestamp, int finishedMaps,
        int finishedReduces) {
      ArrayList<PrintWriter> writer = fileManager.getWriters(jobid);

      if (null != writer) {
        JobHistory.log(writer, RecordTypes.Job, new Keys[] { Keys.JOBID,
            Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS,
            Keys.FINISHED_REDUCES }, new String[] { jobid.toString(),
            String.valueOf(timestamp), Values.KILLED.name(),
            String.valueOf(finishedMaps), String.valueOf(finishedReduces) }, jobid);
        for (PrintWriter out : writer) {
          out.close();
        }
      }
    }
    /**
     * Log job's priority. 
     * @param jobid job id
     * @param priority Jobs priority 
     */
    public static void logJobPriority(JobID jobid, JobPriority priority){
      ArrayList<PrintWriter> writer = fileManager.getWriters(jobid); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Job,
                       new Keys[] {Keys.JOBID, Keys.JOB_PRIORITY},
                       new String[] {jobid.toString(), priority.toString()}, jobid);
      }
    }
    /**
     * Log job's submit-time/launch-time 
     * @param jobid job id
     * @param submitTime job's submit time
     * @param launchTime job's launch time
     * @param restartCount number of times the job got restarted
     * @deprecated Use {@link #logJobInfo(JobID, long, long)} instead.
     */
    @Deprecated
    public static void logJobInfo(JobID jobid, long submitTime, long launchTime,
                                  int restartCount){
      logJobInfo(jobid, submitTime, launchTime);
    }

    public static void logJobInfo(JobID jobid, long submitTime, long launchTime)
    {
      ArrayList<PrintWriter> writer = fileManager.getWriters(jobid); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Job,
                       new Keys[] {Keys.JOBID, Keys.SUBMIT_TIME, 
                                   Keys.LAUNCH_TIME},
                       new String[] {jobid.toString(), 
                                     String.valueOf(submitTime), 
                                     String.valueOf(launchTime)}, jobid);
      }
    }
  }
  
  /**
   * Helper class for logging or reading back events related to Task's start, finish or failure. 
   * All events logged by this class are logged in a separate file per job in 
   * job tracker history. These events map to TIPs in jobtracker. 
   */
  public static class Task extends KeyValuePair{
    private Map <String, TaskAttempt> taskAttempts = new TreeMap<String, TaskAttempt>(); 

    /**
     * Log start time of task (TIP).
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param startTime startTime of tip. 
     */
    public static void logStarted(TaskID taskId, String taskType, 
                                  long startTime, String splitLocations) {
      JobID id = taskId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Task, 
                       new Keys[]{Keys.TASKID, Keys.TASK_TYPE ,
                                  Keys.START_TIME, Keys.SPLITS}, 
                       new String[]{taskId.toString(), taskType,
                                    String.valueOf(startTime),
                                    splitLocations}, id);
      }
    }
    /**
     * Log finish time of task. 
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param finishTime finish timeof task in ms
     */
    public static void logFinished(TaskID taskId, String taskType, 
                                   long finishTime, Counters counters){
      JobID id = taskId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id);

      if (null != writer){
         JobHistory.log(writer, RecordTypes.Task, 
                        new Keys[]{Keys.TASKID, Keys.TASK_TYPE, 
                                   Keys.TASK_STATUS, Keys.FINISH_TIME,
                                   Keys.COUNTERS}, 
                        new String[]{ taskId.toString(), taskType, Values.SUCCESS.name(), 
                                      String.valueOf(finishTime),
                                      counters.makeEscapedCompactString()}, id);
      }
    }

    /**
     * Update the finish time of task. 
     * @param taskId task id
     * @param finishTime finish time of task in ms
     */
    public static void logUpdates(TaskID taskId, long finishTime){
      JobID id = taskId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.Task, 
                       new Keys[]{Keys.TASKID, Keys.FINISH_TIME}, 
                       new String[]{ taskId.toString(), 
                                     String.valueOf(finishTime)}, id);
      }
    }

    /**
     * Log job failed event.
     * @param taskId task id
     * @param taskType MAP or REDUCE.
     * @param time timestamp when job failed detected. 
     * @param error error message for failure. 
     */
    public static void logFailed(TaskID taskId, String taskType, long time, String error){
      logFailed(taskId, taskType, time, error, null);
    }
    
    /**
     * @param failedDueToAttempt The attempt that caused the failure, if any
     */
    public static void logFailed(TaskID taskId, String taskType, long time,
                                 String error, 
                                 TaskAttemptID failedDueToAttempt){
      JobID id = taskId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        String failedAttempt = failedDueToAttempt == null
                               ? ""
                               : failedDueToAttempt.toString();
        JobHistory.log(writer, RecordTypes.Task, 
                       new Keys[]{Keys.TASKID, Keys.TASK_TYPE, 
                                  Keys.TASK_STATUS, Keys.FINISH_TIME, 
                                  Keys.ERROR, Keys.TASK_ATTEMPT_ID}, 
                       new String[]{ taskId.toString(),  taskType, 
                                    Values.FAILED.name(), 
                                    String.valueOf(time) , error, 
                                    failedAttempt}, id);
      }
    }
    /**
     * Returns all task attempts for this task. <task attempt id - TaskAttempt>
     */
    public Map<String, TaskAttempt> getTaskAttempts(){
      return this.taskAttempts;
    }
  }

  /**
   * Base class for Map and Reduce TaskAttempts. 
   */
  public static class TaskAttempt extends Task{} 

  /**
   * Helper class for logging or reading back events related to start, finish or failure of 
   * a Map Attempt on a node.
   */
  public static class MapAttempt extends TaskAttempt{
    /**
     * Log start time of this map task attempt. 
     * @param taskAttemptId task attempt id
     * @param startTime start time of task attempt as reported by task tracker. 
     * @param hostName host name of the task attempt. 
     * @deprecated Use 
     *             {@link #logStarted(TaskAttemptID, long, String, int, String)}
     */
    @Deprecated
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime, String hostName){
      logStarted(taskAttemptId, startTime, hostName, -1, Values.MAP.name());
    }

    @Deprecated
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime,
        String trackerName, int httpPort, String taskType) {
      logStarted(taskAttemptId, startTime, trackerName, httpPort, taskType, 
          Locality.OFF_SWITCH, Avataar.VIRGIN);
    }

    /**
     * Log start time of this map task attempt.
     *  
     * @param taskAttemptId task attempt id
     * @param startTime start time of task attempt as reported by task tracker. 
     * @param trackerName name of the tracker executing the task attempt.
     * @param httpPort http port of the task tracker executing the task attempt
     * @param taskType Whether the attempt is cleanup or setup or map
     * @param locality the data locality of the task attempt
     * @param Avataar the avataar of the task attempt
     */
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime,
                                  String trackerName, int httpPort, 
                                  String taskType,
                                  Locality locality, Avataar avataar) {
      JobID id = taskAttemptId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.MapAttempt, 
                       new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                   Keys.TASK_ATTEMPT_ID, Keys.START_TIME,
                                   Keys.TRACKER_NAME, Keys.HTTP_PORT,
                                   Keys.LOCALITY, Keys.AVATAAR},
                       new String[]{taskType,
                                    taskAttemptId.getTaskID().toString(), 
                                    taskAttemptId.toString(), 
                                    String.valueOf(startTime), trackerName,
                                    httpPort == -1 ? "" : String.valueOf(httpPort),
                                    locality.toString(), avataar.toString()},
                       id
                       ); 
      }
    }
    
    /**
     * Log finish time of map task attempt. 
     * @param taskAttemptId task attempt id 
     * @param finishTime finish time
     * @param hostName host name 
     * @deprecated Use 
     * {@link #logFinished(TaskAttemptID, long, String, String, String, Counters)}
     */
    @Deprecated
    public static void logFinished(TaskAttemptID taskAttemptId, long finishTime, 
                                   String hostName){
      logFinished(taskAttemptId, finishTime, hostName, Values.MAP.name(), "", 
                  new Counters());
    }

    /**
     * Log finish time of map task attempt. 
     * 
     * @param taskAttemptId task attempt id 
     * @param finishTime finish time
     * @param hostName host name 
     * @param taskType Whether the attempt is cleanup or setup or map 
     * @param stateString state string of the task attempt
     * @param counter counters of the task attempt
     */
    public static void logFinished(TaskAttemptID taskAttemptId, 
                                   long finishTime, 
                                   String hostName,
                                   String taskType,
                                   String stateString, 
                                   Counters counter) {
      JobID id = taskAttemptId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.MapAttempt, 
                       new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                   Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                   Keys.FINISH_TIME, Keys.HOSTNAME, 
                                   Keys.STATE_STRING, Keys.COUNTERS},
                       new String[]{taskType, 
                                    taskAttemptId.getTaskID().toString(),
                                    taskAttemptId.toString(), 
                                    Values.SUCCESS.name(),  
                                    String.valueOf(finishTime), hostName, 
                                    stateString, 
                                    counter.makeEscapedCompactString()}, id); 
      }
    }

    /**
     * Log task attempt failed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt.
     * @deprecated Use
     * {@link #logFailed(TaskAttemptID, long, String, String, String)} 
     */
    @Deprecated
    public static void logFailed(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, 
                                 String error) {
      logFailed(taskAttemptId, timestamp, hostName, error, Values.MAP.name());
    }

    /**
     * Log task attempt failed event. 
     *  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logFailed(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, 
                                 String error, String taskType) {
      JobID id = taskAttemptId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.MapAttempt, 
                       new Keys[]{Keys.TASK_TYPE, Keys.TASKID, 
                                  Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                  Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR},
                       new String[]{ taskType, 
                                     taskAttemptId.getTaskID().toString(),
                                     taskAttemptId.toString(), 
                                     Values.FAILED.name(),
                                     String.valueOf(timestamp), 
                                     hostName, error}, id); 
      }
    }
    
    /**
     * Log task attempt killed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @deprecated Use 
     * {@link #logKilled(TaskAttemptID, long, String, String, String)}
     */
    @Deprecated
    public static void logKilled(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, String error){
      logKilled(taskAttemptId, timestamp, hostName, error, Values.MAP.name());
    } 
    
    /**
     * Log task attempt killed event.  
     * 
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logKilled(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName,
                                 String error, String taskType) {
      JobID id = taskAttemptId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.MapAttempt, 
                       new Keys[]{Keys.TASK_TYPE, Keys.TASKID,
                                  Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                  Keys.FINISH_TIME, Keys.HOSTNAME,
                                  Keys.ERROR},
                       new String[]{ taskType, 
                                     taskAttemptId.getTaskID().toString(), 
                                     taskAttemptId.toString(),
                                     Values.KILLED.name(),
                                     String.valueOf(timestamp), 
                                     hostName, error}, id); 
      }
    } 
  }
  /**
   * Helper class for logging or reading back events related to start, finish or failure of 
   * a Map Attempt on a node.
   */
  public static class ReduceAttempt extends TaskAttempt{
    /**
     * Log start time of  Reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param startTime start time
     * @param hostName host name 
     * @deprecated Use 
     * {@link #logStarted(TaskAttemptID, long, String, int, String)}
     */
    @Deprecated
    public static void logStarted(TaskAttemptID taskAttemptId, 
                                  long startTime, String hostName){
      logStarted(taskAttemptId, startTime, hostName, -1, Values.REDUCE.name());
    }

    @Deprecated
    public static void logStarted(TaskAttemptID taskAttemptId, 
        long startTime, String trackerName, 
        int httpPort, 
        String taskType) {
      logStarted(taskAttemptId, startTime, trackerName, httpPort, taskType, 
          Locality.OFF_SWITCH, Avataar.VIRGIN);
    }
    /**
     * Log start time of  Reduce task attempt. 
     * 
     * @param taskAttemptId task attempt id
     * @param startTime start time
     * @param trackerName tracker name 
     * @param httpPort the http port of the tracker executing the task attempt
     * @param taskType Whether the attempt is cleanup or setup or reduce
     * @param locality the data locality of the task attempt
     * @param Avataar the avataar of the task attempt
     */
    public static void logStarted(TaskAttemptID taskAttemptId, 
                                  long startTime, String trackerName, 
                                  int httpPort, 
                                  String taskType,
                                  Locality locality, Avataar avataar) {
      JobID id = taskAttemptId.getJobID();
	    ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.ReduceAttempt,
            new Keys[] {Keys.TASK_TYPE, Keys.TASKID,
                Keys.TASK_ATTEMPT_ID, Keys.START_TIME,
                Keys.TRACKER_NAME, Keys.HTTP_PORT,
                Keys.LOCALITY, Keys.AVATAAR},
            new String[]{taskType,
                taskAttemptId.getTaskID().toString(), 
                taskAttemptId.toString(), 
                String.valueOf(startTime), trackerName,
                httpPort == -1 ? "" : 
                String.valueOf(httpPort),
                locality.toString(), avataar.toString()}, 
            id); 
        }
	    }
	    
	    /**
	     * Log finished event of this task. 
	     * @param taskAttemptId task attempt id
	     * @param shuffleFinished shuffle finish time
	     * @param sortFinished sort finish time
	     * @param finishTime finish time of task
	     * @param hostName host name where task attempt executed
	     * @deprecated Use 
	     * {@link #logFinished(TaskAttemptID, long, long, long, String, String, String, Counters)}
	     */
	    @Deprecated
	    public static void logFinished(TaskAttemptID taskAttemptId, long shuffleFinished, 
					   long sortFinished, long finishTime, 
					   String hostName){
	      logFinished(taskAttemptId, shuffleFinished, sortFinished, 
			  finishTime, hostName, Values.REDUCE.name(),
			  "", new Counters());
	    }
	    
	    /**
	     * Log finished event of this task. 
	     * 
	     * @param taskAttemptId task attempt id
	     * @param shuffleFinished shuffle finish time
	     * @param sortFinished sort finish time
	     * @param finishTime finish time of task
	     * @param hostName host name where task attempt executed
	     * @param taskType Whether the attempt is cleanup or setup or reduce 
	     * @param stateString the state string of the attempt
	     * @param counter counters of the attempt
	     */
	    public static void logFinished(TaskAttemptID taskAttemptId, 
					   long shuffleFinished, 
					   long sortFinished, long finishTime, 
					   String hostName, String taskType,
					   String stateString, Counters counter) {
        JobID id = taskAttemptId.getJobID();
        ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                     Keys.SHUFFLE_FINISHED, Keys.SORT_FINISHED,
                                     Keys.FINISH_TIME, Keys.HOSTNAME, 
                                     Keys.STATE_STRING, Keys.COUNTERS},
                         new String[]{taskType,
                                      taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), 
                                      Values.SUCCESS.name(), 
                                      String.valueOf(shuffleFinished), 
                                      String.valueOf(sortFinished),
                                      String.valueOf(finishTime), hostName,
                                      stateString, 
                                      counter.makeEscapedCompactString()}, id); 
        }
    }
    
    /**
     * Log failed reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task.
     * @deprecated Use 
     * {@link #logFailed(TaskAttemptID, long, String, String, String)} 
     */
    @Deprecated
    public static void logFailed(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error){
      logFailed(taskAttemptId, timestamp, hostName, error, Values.REDUCE.name());
    }
    
    /**
     * Log failed reduce task attempt.
     *  
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task. 
     * @param taskType Whether the attempt is cleanup or setup or reduce 
     */
    public static void logFailed(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error, 
                                 String taskType) {
      JobID id = taskAttemptId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                       new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, 
                                    Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME,
                                    Keys.ERROR },
                       new String[]{ taskType, 
                                     taskAttemptId.getTaskID().toString(), 
                                     taskAttemptId.toString(), 
                                     Values.FAILED.name(), 
                                     String.valueOf(timestamp), hostName, error }, id); 
      }
    }
    
    /**
     * Log killed reduce task attempt. 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task.
     * @deprecated Use 
     * {@link #logKilled(TaskAttemptID, long, String, String, String)} 
     */
    @Deprecated
    public static void logKilled(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error) {
      logKilled(taskAttemptId, timestamp, hostName, error, Values.REDUCE.name());
    }
    
    /**
     * Log killed reduce task attempt. 
     * 
     * @param taskAttemptId task attempt id
     * @param timestamp time stamp when task failed
     * @param hostName host name of the task attempt.  
     * @param error error message of the task. 
     * @param taskType Whether the attempt is cleanup or setup or reduce 
    */
    public static void logKilled(TaskAttemptID taskAttemptId, long timestamp, 
                                 String hostName, String error, 
                                 String taskType) {
      JobID id = taskAttemptId.getJobID();
      ArrayList<PrintWriter> writer = fileManager.getWriters(id); 

      if (null != writer){
        JobHistory.log(writer, RecordTypes.ReduceAttempt, 
                       new Keys[]{  Keys.TASK_TYPE, Keys.TASKID, 
                                    Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME, 
                                    Keys.ERROR },
                       new String[]{ taskType,
                                     taskAttemptId.getTaskID().toString(), 
                                     taskAttemptId.toString(), 
                                     Values.KILLED.name(), 
                                     String.valueOf(timestamp), 
                                     hostName, error }, id); 
      }
    }
  }

  /**
   * Callback interface for reading back log events from JobHistory. This interface 
   * should be implemented and passed to JobHistory.parseHistory() 
   *
   */
  public static interface Listener{
    /**
     * Callback method for history parser. 
     * @param recType type of record, which is the first entry in the line. 
     * @param values a map of key-value pairs as thry appear in history.
     * @throws IOException
     */
    public void handle(RecordTypes recType, Map<Keys, String> values) throws IOException; 
  }
  
  /**
   * Returns the time in milliseconds, truncated to the day.
   */
  static long directoryTime(String year, String month, String day) {
    Calendar result = Calendar.getInstance();
    result.clear();

    result.set(Calendar.YEAR, Integer.parseInt(year));

    // months are 0-based in Calendar, but people will expect January
    // to be month #1 .  Therefore the number is bumped before we make the 
    // directory name and must be debumped to seek the time.
    result.set(Calendar.MONTH, Integer.parseInt(month) - 1);

    result.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
    
    // truncate to day granularity
    long timeInMillis = result.getTimeInMillis();
    return timeInMillis;
  }
  
  /**
   * Delete history files older than one month. Update master index and remove all 
   * jobs older than one month. Also if a job tracker has no jobs in last one month
   * remove reference to the job tracker. 
   *
   */
  public static class HistoryCleaner implements Runnable {
    static final long ONE_DAY_IN_MS = 24 * 60 * 60 * 1000L;
    static final long DEFAULT_HISTORY_MAX_AGE = 30 * ONE_DAY_IN_MS;
    static final long DEFAULT_CLEANUP_FREQUENCY = ONE_DAY_IN_MS;
    static long cleanupFrequency = DEFAULT_CLEANUP_FREQUENCY;
    static long maxAgeOfHistoryFiles = DEFAULT_HISTORY_MAX_AGE;
    private long now; 
    private static final AtomicBoolean isRunning = new AtomicBoolean(false); 
    private static long lastRan = 0; 

    private static Pattern parseDirectory
      = Pattern.compile(".+/([0-9]+)/([0-9]+)/([0-9]+)/[0-9]+/?");

    /**
     * Cleans up history data. 
     */
    public void run() {
      if (isRunning.getAndSet(true)) {
        return; 
      }
      now = System.currentTimeMillis();
      // clean history only once a day at max
      if (lastRan != 0 && (now - lastRan) < cleanupFrequency) {
        isRunning.set(false);
        return; 
      }
      lastRan = now;
      clean(now);
    }
    
    public void clean(long now) {
      Set<String> deletedPathnames = new HashSet<String>();

      // XXXXX debug code
      boolean printedOneDeletee = false;
      boolean printedOneMovedFile = false;

      try {
        Path[] datedDirectories
          = FileUtil.stat2Paths(localGlobber(DONEDIR_FS, DONE,
                                             DONE_BEFORE_SERIAL_TAIL, null));

        // any file with a timestamp earlier than cutoff should be deleted
        long cutoff = now - maxAgeOfHistoryFiles;
        Calendar cutoffDay = Calendar.getInstance();
        cutoffDay.setTimeInMillis(cutoff);
        cutoffDay.set(Calendar.HOUR_OF_DAY, 0);
        cutoffDay.set(Calendar.MINUTE, 0);
        cutoffDay.set(Calendar.SECOND, 0);
        cutoffDay.set(Calendar.MILLISECOND, 0);
        
        // find directories older than the maximum age
        for (int i = 0; i < datedDirectories.length; ++i) {
          String thisDir = datedDirectories[i].toString();
          Matcher pathMatcher = parseDirectory.matcher(thisDir);

          if (pathMatcher.matches()) {
            long dirDay = directoryTime(pathMatcher.group(1),
                                         pathMatcher.group(2),
                                         pathMatcher.group(3));

            if (LOG.isDebugEnabled()) {
              LOG.debug("HistoryCleaner.run just parsed " + thisDir
                  + " as year/month/day = " + pathMatcher.group(1) + "/"
                  + pathMatcher.group(2) + "/" + pathMatcher.group(3));
            }
            
            if (dirDay <= cutoffDay.getTimeInMillis()) {
              if (LOG.isDebugEnabled()) {
                Calendar nnow = Calendar.getInstance();
                nnow.setTimeInMillis(now);
                Calendar then = Calendar.getInstance();
                then.setTimeInMillis(dirDay);
                
                LOG.debug("HistoryCleaner.run directory: " + thisDir
                    + " because its time is " + then + " but it's now " + nnow);
              }
            }
            
            // if dirDay is cutoffDay, some files may be old enough and others not
            if (dirDay == cutoffDay.getTimeInMillis()) {
              // remove old enough files in the directory
              FileStatus[] possibleDeletees = DONEDIR_FS.listStatus(datedDirectories[i]);
              
              for (int j = 0; j < possibleDeletees.length; ++j) {
            	  if (possibleDeletees[j].getModificationTime() < now - 
            	      maxAgeOfHistoryFiles) {
            	    Path deletee = possibleDeletees[j].getPath();
                  if (LOG.isDebugEnabled() && !printedOneDeletee) {
                    LOG.debug("HistoryCleaner.run deletee: "
                        + deletee.toString());
                    printedOneDeletee = true;
                  }

                  DONEDIR_FS.delete(deletee);
                  deletedPathnames.add(deletee.toString());
            	  }
              }
            }

            // if the directory is older than cutoffDay, we can flat out
            // delete it because all the files in it are old enough
            if (dirDay < cutoffDay.getTimeInMillis()) {
              synchronized (existingDoneSubdirs) {
                if (!existingDoneSubdirs.contains(datedDirectories[i])) {
                  LOG.warn("JobHistory: existingDoneSubdirs doesn't contain "
                      + datedDirectories[i] + ", but should.");
                }
                DONEDIR_FS.delete(datedDirectories[i], true);
                existingDoneSubdirs.remove(datedDirectories[i]);
              }
            }
          }
        }

        //walking over the map to purge entries from jobHistoryFileMap
        synchronized (jobHistoryFileMap) {
          Iterator<Entry<JobID, MovedFileInfo>> it =
            jobHistoryFileMap.entrySet().iterator();
          while (it.hasNext()) {
            MovedFileInfo info = it.next().getValue();

            if (LOG.isDebugEnabled() && !printedOneMovedFile) {
              LOG.debug("HistoryCleaner.run a moved file: " + info.historyFile);
              printedOneMovedFile = true;
            }            

            if (deletedPathnames.contains(info.historyFile)) {
              it.remove();
            }
          }
        }
      } catch (IOException ie) {
        LOG.info("Error cleaning up history directory" + 
                 StringUtils.stringifyException(ie));
      } finally {
          isRunning.set(false);
      }
    }
    
    static long getLastRan() {
      return lastRan;
    }
  }

  /**
   * Return the TaskLogsUrl of a particular TaskAttempt
   * 
   * @param attempt
   * @return the taskLogsUrl. null if http-port or tracker-name or
   *         task-attempt-id are unavailable.
   */
  public static String getTaskLogsUrl(JobHistory.TaskAttempt attempt) {
    if (attempt.get(Keys.HTTP_PORT).equals("")
        || attempt.get(Keys.TRACKER_NAME).equals("")
        || attempt.get(Keys.TASK_ATTEMPT_ID).equals("")) {
      return null;
    }

    String taskTrackerName =
      JobInProgress.convertTrackerNameToHostName(
        attempt.get(Keys.TRACKER_NAME));
    return TaskLogServlet.getTaskLogUrl(taskTrackerName, attempt
        .get(Keys.HTTP_PORT), attempt.get(Keys.TASK_ATTEMPT_ID));
  }
}
