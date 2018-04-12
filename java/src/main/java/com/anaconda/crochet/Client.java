package com.anaconda.crochet;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.DispatcherType;

public class Client {

  private static final Logger LOG = LogManager.getLogger(Client.class);

  private Configuration conf;

  private YarnClient yarnClient;

  private String amJar = "crochet-1.0-SNAPSHOT-jar-with-dependencies.jar";

  private int amMemory = 10;

  private int amVCores = 1;

  private int callbackPort;

  private int port;

  private static String secret = "foobar";

  private static Server server;

  private void startupRestServer() throws Exception {
    // Configure the server
    server = new Server();
    HandlerCollection handlers = new HandlerCollection();
    server.setHandler(handlers);
    server.setStopAtShutdown(true);

    ServerConnector connector = new ServerConnector(server);
    connector.setHost("127.0.0.1");
    connector.setPort(0);
    server.addConnector(connector);

    ServletContextHandler context =
        new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new ClientServlet(this)), "/apps/*");
    FilterHolder holder = context.addFilter(HmacFilter.class, "/*",
                                            EnumSet.of(DispatcherType.REQUEST));
    holder.setInitParameter("secret", secret);
    handlers.addHandler(context);

    // Startup the server
    server.start();

    // Determine port
    port = connector.getLocalPort();
  }

  /** Main Entry Point. **/
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      client.init();
      client.run();
    } catch (Throwable exc) {
      LOG.fatal("Error running Client", exc);
      System.exit(1);
    }
  }

  private void init() {
    secret = System.getenv("CROCHET_SECRET_ACCESS_KEY");
    if (secret == null) {
      LOG.fatal("Couldn't find 'CROCHET_SECRET_ACCESS_KEY' envar");
      System.exit(1);
    }

    String callbackPortEnv = System.getenv("CROCHET_CALLBACK_PORT");
    if (callbackPortEnv == null) {
      LOG.fatal("Couldn't find 'CROCHET_CALLBACK_PORT' envar");
      System.exit(1);
    }
    callbackPort = Integer.valueOf(callbackPortEnv);

    conf = new YarnConfiguration();
  }

  private void run() throws Exception {
    // Start the yarn client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // Start the rest server
    startupRestServer();

    // Report back the port we're listening on
    Socket callback = new Socket("127.0.0.1", callbackPort);
    DataOutputStream dos = new DataOutputStream(callback.getOutputStream());
    dos.writeInt(port);
    dos.close();
    callback.close();

    // Wait until EOF or broken pipe from stdin
    while (System.in.read() != -1) {}
    server.stop();
  }

  /** Start a new application. **/
  public ApplicationId submit() throws IOException, YarnException {
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();

    FileSystem fs = FileSystem.get(conf);
    addFile(localResources, fs, amJar, "crochet.jar", appId.toString());

    StringBuilder classPath = new StringBuilder(Environment.CLASSPATH.$$());
    classPath.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(
             YarnConfiguration.YARN_APPLICATION_CLASSPATH,
             YarnConfiguration
                 .DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPath.append(c.trim());
    }

    Map<String, String> env = new HashMap<String, String>();
    env.put("CLASSPATH", classPath.toString());
    env.put("CROCHET_SECRET_ACCESS_KEY", secret);

    String logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    String command = (Environment.JAVA_HOME.$$() + "/bin/java "
                      + "-Xmx" + amMemory + "m "
                      + "com.anaconda.crochet.ApplicationMaster "
                      + "1>" + logdir + "/appmaster.stdout "
                      + "2>" + logdir + "/appmaster.stderr");


    List<String> commands = new ArrayList<String>();
    commands.add(command);

    ByteBuffer fsTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't determine Yarn ResourceManager Kerberos "
                              + "principal for the RM to use as renewer");
      }

      fs.addDelegationTokens(tokenRenewer, credentials);
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
        localResources, env, commands, null, fsTokens, null);

    appContext.setAMContainerSpec(amContainer);
    appContext.setApplicationName("crochet");
    appContext.setResource(Resource.newInstance(amMemory, amVCores));
    appContext.setPriority(Priority.newInstance(0));
    appContext.setQueue("default");

    LOG.info("Submitting application...");
    yarnClient.submitApplication(appContext);

    return appId;
  }

  /** Get information on a running application. **/
  public ApplicationReport getApplicationReport(ApplicationId appId) {
    try {
      return yarnClient.getApplicationReport(appId);
    } catch (Throwable exc) {
      return null;
    }
  }

  /** Kill a running application. **/
  public boolean killApplication(ApplicationId appId) {
    try {
      yarnClient.killApplication(appId);
    } catch (Throwable exc) {
      return false;
    }
    return true;
  }

  private void addFile(Map<String, LocalResource> localResources,
                       FileSystem fs, String source, String target,
                       String appId) throws IOException {
    Path dest = new Path(fs.getHomeDirectory(),
                         ".crochet/" + appId + "/" + target);
    fs.copyFromLocalFile(new Path(source), dest);
    FileStatus status = fs.getFileStatus(dest);
    LocalResource resource = LocalResource.newInstance(
        ConverterUtils.getYarnUrlFromURI(dest.toUri()),
        LocalResourceType.FILE,
        LocalResourceVisibility.APPLICATION,
        status.getLen(),
        status.getModificationTime());
    localResources.put(target, resource);
  }
}