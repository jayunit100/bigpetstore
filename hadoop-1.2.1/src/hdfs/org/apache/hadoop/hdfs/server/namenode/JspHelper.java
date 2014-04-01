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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSClient.RemoteBlockReader;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.KerberosName;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

public class JspHelper {
  public static final String CURRENT_CONF = "current.conf";
  final static public String WEB_UGI_PROPERTY_NAME = "dfs.web.ugi";
  public static final String DELEGATION_PARAMETER_NAME = DelegationParam.NAME;
  static final String SET_DELEGATION = "&" + DELEGATION_PARAMETER_NAME +
                                              "=";
  private static final Log LOG = LogFactory.getLog(JspHelper.class);

  static FSNamesystem fsn = null;
  public static InetSocketAddress nameNodeAddr;
  
  static Random rand = new Random();

  public JspHelper() {
    fsn = FSNamesystem.getFSNamesystem();
    if (DataNode.getDataNode() != null) {
      nameNodeAddr = DataNode.getDataNode().getNameNodeAddr();
    }
    else {
      nameNodeAddr = fsn.getDFSNameNodeAddress(); 
    }      
  }

  public DatanodeID randomNode() throws IOException {
    return fsn.getRandomDatanode();
  }

  public static DatanodeInfo bestNode(LocatedBlock blk) throws IOException {
    TreeSet<DatanodeInfo> deadNodes = new TreeSet<DatanodeInfo>();
    DatanodeInfo chosenNode = null;
    int failures = 0;
    Socket s = null;
    DatanodeInfo [] nodes = blk.getLocations();
    if (nodes == null || nodes.length == 0) {
      throw new IOException("No nodes contain this block");
    }
    while (s == null) {
      if (chosenNode == null) {
        do {
          chosenNode = nodes[rand.nextInt(nodes.length)];
        } while (deadNodes.contains(chosenNode));
      }
      int index = rand.nextInt(nodes.length);
      chosenNode = nodes[index];

      //just ping to check whether the node is alive
      InetSocketAddress targetAddr = NetUtils.createSocketAddr(
          chosenNode.getHost() + ":" + chosenNode.getInfoPort());
        
      try {
        s = new Socket();
        s.connect(targetAddr, HdfsConstants.READ_TIMEOUT);
        s.setSoTimeout(HdfsConstants.READ_TIMEOUT);
      } catch (IOException e) {
        deadNodes.add(chosenNode);
        s.close();
        s = null;
        failures++;
      }
      if (failures == nodes.length)
        throw new IOException("Could not reach the block containing the data. Please try again");
        
    }
    s.close();
    return chosenNode;
  }
  public void streamBlockInAscii(InetSocketAddress addr, long blockId, 
               Token<BlockTokenIdentifier> accessToken, long genStamp, 
                                 long blockSize, 
                                 long offsetIntoBlock, long chunkSizeToView, 
                                 JspWriter out,
                                 Configuration conf) 
    throws IOException {
    if (chunkSizeToView == 0) return;
    Socket s = new Socket();
    s.connect(addr, HdfsConstants.READ_TIMEOUT);
    s.setSoTimeout(HdfsConstants.READ_TIMEOUT);
      
      long amtToRead = Math.min(chunkSizeToView, blockSize - offsetIntoBlock);     
      
      // Use the block name for file name. 
      BlockReader blockReader = 
        RemoteBlockReader.newBlockReader(s, addr.toString() + ":" + blockId,
                                         blockId, accessToken, genStamp ,offsetIntoBlock, 
                                         amtToRead, 
                                         conf.getInt("io.file.buffer.size", 4096));
        
    byte[] buf = new byte[(int)amtToRead];
    int readOffset = 0;
    int retries = 2;
    while ( amtToRead > 0 ) {
      int numRead;
      try {
        numRead = blockReader.readAll(buf, readOffset, (int)amtToRead);
      }
      catch (IOException e) {
        retries--;
        if (retries == 0)
          throw new IOException("Could not read data from datanode");
        continue;
      }
      amtToRead -= numRead;
      readOffset += numRead;
    }
    blockReader = null;
    s.close();
    out.print(HtmlQuoting.quoteHtmlChars(new String(buf)));
  }
  public void DFSNodesStatus(ArrayList<DatanodeDescriptor> live,
                             ArrayList<DatanodeDescriptor> dead) {
    if (fsn != null) {
      fsn.DFSNodesStatus(live, dead);
      fsn.removeDecomNodeFromDeadList(dead);  
    }
  }

  public void addTableHeader(JspWriter out) throws IOException {
    out.print("<table border=\"1\""+
              " cellpadding=\"2\" cellspacing=\"2\">");
    out.print("<tbody>");
  }
  public void addTableRow(JspWriter out, String[] columns) throws IOException {
    out.print("<tr>");
    for (int i = 0; i < columns.length; i++) {
      out.print("<td style=\"vertical-align: top;\"><B>"+columns[i]+"</B><br></td>");
    }
    out.print("</tr>");
  }
  public void addTableRow(JspWriter out, String[] columns, int row) throws IOException {
    out.print("<tr>");
      
    for (int i = 0; i < columns.length; i++) {
      if (row/2*2 == row) {//even
        out.print("<td style=\"vertical-align: top;background-color:LightGrey;\"><B>"+columns[i]+"</B><br></td>");
      } else {
        out.print("<td style=\"vertical-align: top;background-color:LightBlue;\"><B>"+columns[i]+"</B><br></td>");
          
      }
    }
    out.print("</tr>");
  }
  public void addTableFooter(JspWriter out) throws IOException {
    out.print("</tbody></table>");
  }

  public String getSafeModeText() {
    if (!fsn.isInSafeMode())
      return "";
    return "Safe mode is ON. <em>" + fsn.getSafeModeTip() + "</em><br>";
  }

  public static String getWarningText(FSNamesystem fsn) {
    // Ideally this should be displayed in RED
    long missingBlocks = fsn.getMissingBlocksCount();
    if (missingBlocks > 0) {
      return "<br> WARNING :" + 
             " There are about " + missingBlocks +
             " missing blocks. Please check the log or run fsck. <br><br>";
    }
    return "";
  }
  
  public String getInodeLimitText() {
    long inodes = fsn.dir.totalInodes();
    long blocks = fsn.getBlocksTotal();
    long maxobjects = fsn.getMaxObjects();
    long totalMemory = Runtime.getRuntime().totalMemory();   
    long maxMemory = Runtime.getRuntime().maxMemory();   

    long used = (totalMemory * 100)/maxMemory;
 
    String str = inodes + " files and directories, " +
                 blocks + " blocks = " +
                 (inodes + blocks) + " total";
    if (maxobjects != 0) {
      long pct = ((inodes + blocks) * 100)/maxobjects;
      str += " / " + maxobjects + " (" + pct + "%)";
    }
    str += ".  Heap Size is " + StringUtils.byteDesc(totalMemory) + " / " + 
           StringUtils.byteDesc(maxMemory) + 
           " (" + used + "%) <br>";
    return str;
  }

  public String getUpgradeStatusText() {
    String statusText = "";
    try {
      UpgradeStatusReport status = 
        fsn.distributedUpgradeProgress(UpgradeAction.GET_STATUS);
      statusText = (status == null ? 
          "There are no upgrades in progress." :
            status.getStatusText(false));
    } catch(IOException e) {
      statusText = "Upgrade status unknown.";
    }
    return statusText;
  }

  public void sortNodeList(ArrayList<DatanodeDescriptor> nodes,
                           String field, String order) {
        
    class NodeComapare implements Comparator<DatanodeDescriptor> {
      static final int 
        FIELD_NAME              = 1,
        FIELD_LAST_CONTACT      = 2,
        FIELD_BLOCKS            = 3,
        FIELD_CAPACITY          = 4,
        FIELD_USED              = 5,
        FIELD_PERCENT_USED      = 6,
        FIELD_NONDFS_USED       = 7,
        FIELD_REMAINING         = 8,
        FIELD_PERCENT_REMAINING = 9,
        SORT_ORDER_ASC          = 1,
        SORT_ORDER_DSC          = 2;

      int sortField = FIELD_NAME;
      int sortOrder = SORT_ORDER_ASC;
            
      public NodeComapare(String field, String order) {
        if (field.equals("lastcontact")) {
          sortField = FIELD_LAST_CONTACT;
        } else if (field.equals("capacity")) {
          sortField = FIELD_CAPACITY;
        } else if (field.equals("used")) {
          sortField = FIELD_USED;
        } else if (field.equals("nondfsused")) {
          sortField = FIELD_NONDFS_USED;
        } else if (field.equals("remaining")) {
          sortField = FIELD_REMAINING;
        } else if (field.equals("pcused")) {
          sortField = FIELD_PERCENT_USED;
        } else if (field.equals("pcremaining")) {
          sortField = FIELD_PERCENT_REMAINING;
        } else if (field.equals("blocks")) {
          sortField = FIELD_BLOCKS;
        } else {
          sortField = FIELD_NAME;
        }
                
        if (order.equals("DSC")) {
          sortOrder = SORT_ORDER_DSC;
        } else {
          sortOrder = SORT_ORDER_ASC;
        }
      }

      public int compare(DatanodeDescriptor d1,
                         DatanodeDescriptor d2) {
        int ret = 0;
        switch (sortField) {
        case FIELD_LAST_CONTACT:
          ret = (int) (d2.getLastUpdate() - d1.getLastUpdate());
          break;
        case FIELD_CAPACITY:
          long  dlong = d1.getCapacity() - d2.getCapacity();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_USED:
          dlong = d1.getDfsUsed() - d2.getDfsUsed();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_NONDFS_USED:
          dlong = d1.getNonDfsUsed() - d2.getNonDfsUsed();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_REMAINING:
          dlong = d1.getRemaining() - d2.getRemaining();
          ret = (dlong < 0) ? -1 : ((dlong > 0) ? 1 : 0);
          break;
        case FIELD_PERCENT_USED:
          double ddbl =((d1.getDfsUsedPercent())-
                        (d2.getDfsUsedPercent()));
          ret = (ddbl < 0) ? -1 : ((ddbl > 0) ? 1 : 0);
          break;
        case FIELD_PERCENT_REMAINING:
          ddbl =((d1.getRemainingPercent())-
                 (d2.getRemainingPercent()));
          ret = (ddbl < 0) ? -1 : ((ddbl > 0) ? 1 : 0);
          break;
        case FIELD_BLOCKS:
          ret = d1.numBlocks() - d2.numBlocks();
          break;
        case FIELD_NAME: 
          ret = d1.getHostName().compareTo(d2.getHostName());
          break;
        }
        return (sortOrder == SORT_ORDER_DSC) ? -ret : ret;
      }
    }
        
    Collections.sort(nodes, new NodeComapare(field, order));
  }

  public static void printPathWithLinks(String dir, JspWriter out, 
                                        int namenodeInfoPort,
                                        String tokenString
                                       ) throws IOException {
    try {
      String[] parts = dir.split(Path.SEPARATOR);
      StringBuilder tempPath = new StringBuilder(dir.length());
      out.print("<a href=\"browseDirectory.jsp" + "?dir="+ Path.SEPARATOR
          + "&namenodeInfoPort=" + namenodeInfoPort
          + getDelegationTokenUrlParam(tokenString) + "\">" + Path.SEPARATOR
          + "</a>");
      tempPath.append(Path.SEPARATOR);
      for (int i = 0; i < parts.length-1; i++) {
        if (!parts[i].equals("")) {
          tempPath.append(parts[i]);
          out.print("<a href=\"browseDirectory.jsp" + "?dir="
              + tempPath.toString() + "&namenodeInfoPort=" + namenodeInfoPort
              + getDelegationTokenUrlParam(tokenString));
          out.print("\">" + parts[i] + "</a>" + Path.SEPARATOR);
          tempPath.append(Path.SEPARATOR);
        }
      }
      if(parts.length > 0) {
        out.print(parts[parts.length-1]);
      }
    }
    catch (UnsupportedEncodingException ex) {
      ex.printStackTrace();
    }
  }

  public static void printGotoForm(JspWriter out,
                                   int namenodeInfoPort,
                                   String tokenString,
                                   String file) throws IOException {
    out.print("<form action=\"browseDirectory.jsp\" method=\"get\" name=\"goto\">");
    out.print("Goto : ");
    out.print("<input name=\"dir\" type=\"text\" width=\"50\" id\"dir\" value=\""+ file+"\">");
    out.print("<input name=\"go\" type=\"submit\" value=\"go\">");
    out.print("<input name=\"namenodeInfoPort\" type=\"hidden\" "
        + "value=\"" + namenodeInfoPort  + "\">");
    if (UserGroupInformation.isSecurityEnabled()) {
      out.print("<input name=\"" + DELEGATION_PARAMETER_NAME
          + "\" type=\"hidden\" value=\"" + tokenString + "\">");
    }
    out.print("</form>");
  }
  
  public static void createTitle(JspWriter out, 
                                 HttpServletRequest req, 
                                 String  file) throws IOException{
    if(file == null) file = "";
    int start = Math.max(0,file.length() - 100);
    if(start != 0)
      file = "..." + file.substring(start, file.length());
    out.print("<title>HDFS:" + file + "</title>");
  }

  
  /**
   * If security is turned off, what is the default web user?
   * @param conf the configuration to look in
   * @return the remote user that was configuration
   */
  public static UserGroupInformation getDefaultWebUser(Configuration conf
                                                       ) throws IOException {
    String[] strings = conf.getStrings(JspHelper.WEB_UGI_PROPERTY_NAME);
    if (strings == null || strings.length == 0) {
      throw new IOException("Cannot determine UGI from request or conf");
    }
    return UserGroupInformation.createRemoteUser(strings[0]);
  }

  /** Same as getUGI(null, request, conf). */
  public static UserGroupInformation getUGI(final HttpServletRequest request,
      final Configuration conf) throws IOException {
    return getUGI(null, request, conf);
  }
  
  /**
   * Get {@link UserGroupInformation} and possibly the delegation token out of
   * the request.
   * @param context the Servlet context
   * @param request the http request
   * @param conf configuration
   * @throws AccessControlException if the request has no token
   */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf) throws IOException {
    return getUGI(context, request, conf, AuthenticationMethod.KERBEROS_SSL,
        true);
  }

  /**
   * Get {@link UserGroupInformation} and possibly the delegation token out of
   * the request.
   * @param context the Servlet context
   * @param request the http request
   * @param conf configuration
   * @param secureAuthMethod the AuthenticationMethod used in secure mode.
   * @param tryUgiParameter Should it try the ugi parameter?
   * @return a new user from the request
   * @throws AccessControlException if the request has no token
   */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf,
      final AuthenticationMethod secureAuthMethod,
      final boolean tryUgiParameter) throws IOException {
    final UserGroupInformation ugi;
    final String usernameFromQuery = getUsernameFromQuery(request, tryUgiParameter);
    final String doAsUserFromQuery = request.getParameter(DoAsParam.NAME);

    if(UserGroupInformation.isSecurityEnabled()) {
      final String remoteUser = request.getRemoteUser();
      String tokenString = request.getParameter(DELEGATION_PARAMETER_NAME);
      if (tokenString != null) {
        Token<DelegationTokenIdentifier> token = 
          new Token<DelegationTokenIdentifier>();
        token.decodeFromUrlString(tokenString);
        SecurityUtil.setTokenService(token, NameNode.getAddress(conf));
        token.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);

        ByteArrayInputStream buf = 
          new ByteArrayInputStream(token.getIdentifier());
        DataInputStream in = new DataInputStream(buf);
        DelegationTokenIdentifier id = new DelegationTokenIdentifier();
        id.readFields(in);
        if (context != null) {
          NameNode nn = (NameNode) context.getAttribute("name.node");
          if (nn != null) {
            //Verify the token.
            nn.getNamesystem().getDelegationTokenSecretManager()
                .verifyToken(id, token.getPassword());
          }
        }
        ugi = id.getUser();
        if (ugi.getRealUser() == null) {
          //non-proxy case
          checkUsername(ugi.getShortUserName(), usernameFromQuery);
          checkUsername(null, doAsUserFromQuery);
        } else {
          //proxy case
          checkUsername(ugi.getRealUser().getShortUserName(), usernameFromQuery);
          checkUsername(ugi.getShortUserName(), doAsUserFromQuery);
          ProxyUsers.authorize(ugi, request.getRemoteAddr(), conf);
        }
        ugi.addToken(token);
      } else {
        if(remoteUser == null) {
          throw new IOException("Security enabled but user not " +
                                "authenticated by filter");
        }
        final UserGroupInformation realUgi = UserGroupInformation.createRemoteUser(remoteUser);
        checkUsername(realUgi.getShortUserName(), usernameFromQuery);
        // This is not necessarily true, could have been auth'ed by user-facing
        // filter
        realUgi.setAuthenticationMethod(secureAuthMethod);
        ugi = initUGI(realUgi, doAsUserFromQuery, request, true, conf);
      }
    } else { // Security's not on, pull from url
      final UserGroupInformation realUgi = usernameFromQuery == null?
          getDefaultWebUser(conf) // not specified in request
          : UserGroupInformation.createRemoteUser(usernameFromQuery);
      realUgi.setAuthenticationMethod(AuthenticationMethod.SIMPLE);
      ugi = initUGI(realUgi, doAsUserFromQuery, request, false, conf);
    }
    
    if(LOG.isDebugEnabled())
      LOG.debug("getUGI is returning: " + ugi.getShortUserName());
    return ugi;
  }

  private static UserGroupInformation initUGI(final UserGroupInformation realUgi,
      final String doAsUserFromQuery, final HttpServletRequest request,
      final boolean isSecurityEnabled, final Configuration conf
      ) throws AuthorizationException {
    final UserGroupInformation ugi;
    if (doAsUserFromQuery == null) {
      //non-proxy case
      ugi = realUgi;
    } else {
      //proxy case
      ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, realUgi);
      ugi.setAuthenticationMethod(
          isSecurityEnabled? AuthenticationMethod.PROXY: AuthenticationMethod.SIMPLE);
      ProxyUsers.authorize(ugi, request.getRemoteAddr(), conf);
    }
    return ugi;
  }

  /**
   * Expected user name should be a short name.
   */
  private static void checkUsername(final String expected, final String name
      ) throws IOException {
    if (expected == null && name != null) {
      throw new IOException("Usernames not matched: expecting null but name="
          + name);
    }
    if (name == null) { //name is optional, null is okay
      return;
    }
    KerberosName u = new KerberosName(name);
    String shortName = u.getShortName();
    if (!shortName.equals(expected)) {
      throw new IOException("Usernames not matched: name=" + shortName
          + " != expected=" + expected);
    }
  }

  private static String getUsernameFromQuery(final HttpServletRequest request,
      final boolean tryUgiParameter) {
    String username = request.getParameter(UserParam.NAME);
    if (username == null && tryUgiParameter) {
      //try ugi parameter
      final String ugiStr = request.getParameter("ugi");
      if (ugiStr != null) {
        username = ugiStr.split(",")[0];
      }
    }
    return username;
  }

  public static DFSClient getDFSClient(final UserGroupInformation user,
                                       final InetSocketAddress addr,
                                       final Configuration conf
                                       ) throws IOException,
                                                InterruptedException {
    return
      user.doAs(new PrivilegedExceptionAction<DFSClient>() {
        public DFSClient run() throws IOException {
          return new DFSClient(addr, conf);
        }
      });
  }
  
  /**
   * Returns the url parameter for the given token string.
   * @param tokenString
   * @return url parameter
   */
  public static String getDelegationTokenUrlParam(String tokenString) {
    if (tokenString == null ) {
      return "";
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      return SET_DELEGATION + tokenString;
    } else {
      return "";
    }
  }

   /** Convert a String to chunk-size-to-view. */
   public static int string2ChunkSizeToView(String s, int defaultValue) {
     int n = s == null? 0: Integer.parseInt(s);
     return n > 0? n: defaultValue;
   }

  /**
   * Get the default chunk size.
   * @param conf the configuration
   * @return the number of bytes to chunk in
   */
  public static int getDefaultChunkSize(Configuration conf) {
    return conf.getInt("dfs.default.chunk.view.size", 32 * 1024);
  }
  
  /** Return a table containing version information. */
  static String getVersionTable(FSNamesystem fsn) {
    return "<div class='dfstable'><table>"
        + "\n  <tr><td class='col1'>Started:</td><td>" + fsn.getStartTime()
        + "</td></tr>\n" + "\n  <tr><td class='col1'>Version:</td><td>"
        + VersionInfo.getVersion() + ", " + VersionInfo.getRevision()
        + "</td></tr>\n" + "\n  <tr><td class='col1'>Compiled:</td><td>"
        + VersionInfo.getDate() + " by " + VersionInfo.getUser()
        + "</td></tr>\n</table></div>";
  }

  /** Return a table containing version information. */
  public static String getVersionTable() {
    return "<div id='dfstable'><table>"       
        + "\n  <tr><td id='col1'>Version:</td><td>" + VersionInfo.getVersion() + ", " + VersionInfo.getRevision()
        + "\n  <tr><td id='col1'>Compiled:</td><td>" + VersionInfo.getDate() + " by " + VersionInfo.getUser()
        + "\n</table></div>";
  }
}
