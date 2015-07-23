import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.List;


public class NameNode extends UnicastRemoteObject implements NameNodeInterface, Serializable {
  // required for RMI
  protected NameNode() throws RemoteException {
    //super(); // super() is called by default even if it's not here
    // TODO Auto-generated constructor stub
  }

  /**
   * 
   */
  private static final long serialVersionUID = 6245153320204750242L;
  
  // base path where all file structure is stored
  static String basePath;
  // HDD quota of space available to use by the clients
  static long hddQuota;
  // name of the file with the list of expected data nodes
  static String dataNodesFileName; 
  // list of data nodes addresses
  static HashSet<String> dataNodesAddresses = new HashSet<String>();
  
  // reference to Logger class
  static Logger logger;
  
  // data nodes to store chunks
  public static DataNodeInterface[] dataNodes = null;
  
  public static void main(String[] args) throws IOException, UninitializedLoggerException {
    try {
      // init everything to log messages
      if (args.length > 2) {
        initLogger(0, args[0]); // 1 - logLevel, 2 - logFilename
      } else {
        initLogger(0, null); // 1 - logLevel, 2 - logFilename
      }
      
      if (args.length < 4) {
        throw new IncompleteArgumentListException();
      }
      
      // get base path from the program parameters
      basePath = args[1].replaceAll("/", "\\\\");
      File f = new File(basePath);
      log(0, "Base path (absolute): " + f.getAbsolutePath() + "\n");
      log(0, "Base path (canonical): " + f.getCanonicalPath() + "\n");
      log(0, "Base path (virtual): " + basePath + "\n");
      basePath = f.getCanonicalPath();
      
      // get HDD quota from the program parameters
      hddQuota = Long.parseLong(args[2]);  
      
      // name of the file with the list of expected data nodes from the program parameters
      dataNodesFileName = args[3];
      
      // check if file exists
      f = new File(dataNodesFileName);
      if(!f.exists() || f.isDirectory()) {
        throw new IncorrectDataNodesFileNameFileException();
      }
      
      // read list of of expected data nodes to connect to
      log(0, "DataNodes' addresses to connect to are:\n");
  
      List<String> lines = Files.readAllLines(Paths.get(dataNodesFileName), Charset.defaultCharset());
      for (String line : lines) {
        log(0, line + "\n");
        dataNodesAddresses.add(line.trim());
      }
      if (dataNodesAddresses.isEmpty()) {
        throw new EmptyDataNodesFileException();
      }
      
      log(0, "There are " + dataNodesAddresses.size() + " data nodes:\n");
      dataNodes = new DataNodeInterface[dataNodesAddresses.size()];
      for (String s : dataNodesAddresses) {
        String[] parts = s.split(":");
        log(0, "ID: " + parts[0] + ", host: " + parts[1] + ", port: " + parts[2] + "\n");
        dataNodes[Integer.parseInt(parts[0]) - 1] = (DataNodeInterface) Naming.lookup("//" + parts[1] + ":" + parts[2] + "/DataNode"); // "//host:port/name"
      }
      
      // Here we start up the server
      String localIP = InetAddress.getLocalHost().getHostAddress();

      // init security manager
      if (System.getSecurityManager() == null) {
        System.setSecurityManager(new SecurityManager());
      }
      
      NameNode nameNode = new NameNode();
      RemoteFile remoteFile = new RemoteFile();
      
      /*
      log(0, "Unexporting previous nameNode object from the RMI registry\n");
      UnicastRemoteObject.unexportObject(nameNode, false);
      log(0, "Done\n");
      */
      
      //Registry reg = LocateRegistry.createRegistry(0);
      //UnicastRemoteObject.unexportObject(reg,true);
      
      // unbind NameNode first that we can use this name for our own object
      try {
        Naming.unbind("NameNode");
      } catch (NotBoundException e) {
        log(0, "NameNode is not bounded to any RMI object.\n");
      }
      Naming.bind("NameNode", nameNode);
      log(0, "NameNode has being succesfully bounded.\n");
      
      try {
        Naming.unbind("RemoteFile");
      } catch (NotBoundException e) {
        log(0, "RemoteFile is not bounded to any RMI object.\n");
      }
      Naming.bind("RemoteFile", remoteFile);
      log(0, "RemoteFile has being succesfully bounded.\n");
      
      log(0, "Listening to incoming requests...\n");
      
    } catch (IncorrectDataNodesFileNameFileException e) {
      log(0, "File with list of data nodes doesn't exist or inaccessible.");
    } catch (EmptyDataNodesFileException e) {
      log(0, "File with list of data nodes is empty.");
    } catch (IncorrectLogFileException e) {
      log(0, "File to store log files can't be created or inaccessible.");
    } catch (IncompleteArgumentListException e) {
      log(0, "Use: NameNode logFileName basePath hddQuota dataNodesFileName");
    } catch (AlreadyBoundException e) {
      log(0, "NameNode is already bounded to some RMI object");
    } catch (NumberFormatException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NotBoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public static void initLogger(int logLevel, String logFilename) throws IncorrectLogFileException {
    logger = new Logger(logLevel, logFilename);
  }
  
  public static void log(int logLevel, String message) {
    if (logger == null) {
      throw new RuntimeException(new UninitializedLoggerException());
    }
    logger.log(logLevel, message);
  }
  
  @Override
  public boolean isFileExists(String path) throws RemoteException {
    File f = new File(basePath + path);
    return f.exists() && !f.isDirectory();
  }

  @Override
  public boolean isDirectoryExists(String path) throws RemoteException {
    File f = new File(basePath + path);
    return f.exists() && f.isDirectory();
  }

  @Override
  public synchronized String getBasePath() throws RemoteException {
    log(0, "String getBasePath()\n");
    return basePath;
  }

  @Override
  public synchronized void setBasePath(String basePath) {
    log(0, "void setBasePath(" + basePath + ")\n");
    NameNode.basePath = basePath;
  }

  @Override
  public synchronized long getHddQuota() {
    log(0, "long getHddQuota()\n");
    return hddQuota;
  }

  @Override
  public synchronized void setHddQuota(long hddQuota) {
    log(0, "void setHddQuota(" + hddQuota + ")\n");
    NameNode.hddQuota = hddQuota;
  }

  @Override
  public RemoteFile getFile(String path) throws IOException {
    log(0, "RemoteFile getFile(" + path + ")\n");
    return new RemoteFile(path);
  }
  
  public int numberOfDataNodes() {
    return dataNodesAddresses.size();
  }

  public synchronized HashSet<String> getDataNodesAddresses() {
    return dataNodesAddresses;
  }

  public synchronized void setDataNodesAddresses(HashSet<String> dataNodesAddresses) {
    NameNode.dataNodesAddresses = dataNodesAddresses;
  }
}
