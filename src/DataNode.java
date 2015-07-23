import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;


public class DataNode extends UnicastRemoteObject implements DataNodeInterface, Serializable {
  // required for RMI
  protected DataNode() throws RemoteException {
    //super(); // super() is called by default even if it's not here
    // TODO Auto-generated constructor stub
  }

  /**
   * 
   */
  private static final long serialVersionUID = -3300956478331530054L;
  // base path where all file structure is stored
  static String basePath;
  // HDD quota of space available to use by the clients
  static long hddQuota;
  
  // reference to Logger class
  static Logger logger;
  
  // database path
  static String databaseFilename;
  //reference to connection
  static Connection c = null;
  // statement to run queries to SQLite database
  static Statement stmt = null;
  // result statement variable to use in each request to SQLite database
  static ResultSet rs = null;
  // variable to store SQL statements
  static String sql = null;
  
  static int localPort;
  
  public static void main(String[] args) throws IOException, UninitializedLoggerException {
    try {
      // init everything to log messages
      if (args.length > 2) {
        initLogger(0, args[0]); // 1 - logLevel, 2 - logFilename
      } else {
        initLogger(0, null); // 1 - logLevel, 2 - logFilename
      }
      
      if (args.length < 5) {
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
      
      // Here we start up the server
      String localIP = InetAddress.getLocalHost().getHostAddress();

      // get local port from program parameters
      localPort = Integer.parseInt(args[4]);
      
      // init security manager
      if (System.getSecurityManager() == null) {
        System.setSecurityManager(new SecurityManager());
      }
      
      DataNodeInterface dataNode = new DataNode();
      //RemoteFile remoteFile = new RemoteFile();
      
      /*
      log(0, "Unexporting previous dataNode object from the RMI registry\n");
      UnicastRemoteObject.unexportObject(dataNode, false);
      log(0, "Done\n");
      */
      
      // unbind DataNode first that we can use this name for our own object
      
      String rmiURI = "//localhost:" + localPort + "/DataNode";
      try {
        Naming.unbind(rmiURI);
      } catch (NotBoundException e) {
        log(0, "DataNode is not bounded to any RMI object.\n");
      }
      Naming.bind(rmiURI, dataNode);
      log(0, "DataNode has being succesfully bounded.\n");
      
      /*
      try {
        Naming.unbind("RemoteFile");
      } catch (NotBoundException e) {
        log(0, "RemoteFile is not bounded to any RMI object.\n");
      }
      Naming.bind("RemoteFile", remoteFile);
      log(0, "RemoteFile has being succesfully bounded.\n");
      */
      
      // read databaseFilename from program parameters
      databaseFilename = args[3];
      log(0, "Database filename is: " + databaseFilename + "\n");
              
      Class.forName("org.sqlite.JDBC");
      String sqlURI = "jdbc:sqlite:" + databaseFilename;
      log(0, "SQLite connection URI: \"" + sqlURI + "\"\n");
      c = DriverManager.getConnection(sqlURI);
      c.setAutoCommit(false);
      
      log(0, "Opened database successfully\n");
      
      stmt = c.createStatement();
      sql = "CREATE TABLE CHUNKS (HASH INT PRIMARY KEY NOT NULL, SIZE INT NOT NULL, COPIES INT NOT NULL)";
      try {
        stmt.executeUpdate(sql);
        c.commit();
      } catch (SQLException e) {
        // table already exists, move forward
      }
      stmt.close();
      
      /*
      byte[] test = "Однажды в студеную зимнюю пору я из лесу вышел, был сильный мороз!".getBytes();
      dataNode.saveChunk(test);
      byte[] chunk = dataNode.getChunk(Arrays.hashCode(test));
      log(0, "byte[] chunk: " + new String(chunk) + "\n");
      */
      
      log(0, "Listening to incoming requests...\n");
      
    } catch (IncorrectLogFileException e) {
      log(0, "File to store log files can't be created or inaccessible.");
    } catch (IncompleteArgumentListException e) {
      log(0, "Use: DataNode logFileName basePath hddQuota databaseFileName");
    } catch (AlreadyBoundException e) {
      log(0, "DataNode is already bounded to some RMI object");
    } catch (ClassNotFoundException e) {
      log(0, "Can't find necessary class");
      e.printStackTrace();
    } catch (SQLException e) {
      log(0, "Can't get connection to SQLite database");
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
    DataNode.basePath = basePath;
  }

  @Override
  public synchronized long getHddQuota() {
    log(0, "long getHddQuota()\n");
    return hddQuota;
  }

  @Override
  public synchronized void setHddQuota(long hddQuota) {
    log(0, "void setHddQuota(" + hddQuota + ")\n");
    DataNode.hddQuota = hddQuota;
  }

  @Override
  public RemoteFile getFile(String path) throws IOException {
    log(0, "RemoteFile getFile(" + path + ")\n");
    return new RemoteFile(path);
  }
  
  public boolean saveChunk(Integer hash) throws SQLException {
    log(0, "saveChunk(" + hash + ")");
    // check, if we already have this chunk we only need to increase number of copies
    sql = "SELECT * FROM CHUNKS WHERE hash = " + hash;
    log(0, sql + "\n");
    rs = stmt.executeQuery(sql);
    if (rs.next()) {
      // we already have this chunk saved
      log(0, "Chunk with hash: " + Integer.toHexString(hash) + " has " + rs.getInt("copies") + " copies\n");
      sql = "UPDATE CHUNKS SET copies = copies + 1 WHERE hash = " + hash;
      log(0, sql + "\n");
      stmt.executeUpdate(sql);
      c.commit();
      return true;
    }
    rs.close(); 
    
    // we don't have chunk saved and need data to store them
    return false;
  }
  
  public boolean saveChunk(byte[] data) throws SQLException, IOException {
    // hash code of data to store in database 
    int hash = Arrays.hashCode(data);
    
    // check, if we already have this chunk we only need to increase number of copies
    sql = "SELECT * FROM CHUNKS WHERE hash = " + hash;
    log(0, sql + "\n");
    rs = stmt.executeQuery(sql);
    if (rs.next()) {
      // we already have this chunk saved
      log(0, "Increase copies for chunk " + Integer.toHexString(hash) + " from " + rs.getInt("copies") + " to " + (rs.getInt("copies") + 1) + "\n");
      sql = "UPDATE CHUNKS SET copies = copies + 1 WHERE hash = " + hash;
      log(0, sql + "\n");
      stmt.executeUpdate(sql);
    } else {
      // write chunk to the storage
      FileOutputStream fos = new FileOutputStream(basePath + "\\" + Integer.toHexString(hash));
      fos.write(data);
      fos.close();
      
      // save hash of new chunk to database
      log(0, "It's a first copy of chunk " + Integer.toHexString(hash) + "\n");
      sql = "INSERT INTO CHUNKS (HASH, SIZE, COPIES) VALUES (" + hash + ", " + data.length + ", 1)";
      log(0, sql + "\n");
      stmt.executeUpdate(sql);      
    }
    c.commit();
    rs.close();
    
    return true;
  }
  
  public boolean deleteChunk(Integer hash) throws SQLException {
    log(0, "deleteChunk(" + hash + ")\n");
    /*
     *  check, if we have this chunk we need to increase number of copies
     *  if no copies left we may delete it safely
     */
    sql = "SELECT * FROM CHUNKS WHERE hash = " + hash;
    log(0, sql + "\n");
    rs = stmt.executeQuery(sql);
    if (rs.next()) {
      log(0, "Chunk with hash: " + Integer.toHexString(hash) + " had " + rs.getInt("copies") + " copies\n");
      if (rs.getInt("copies") > 1) {
        // if we more than 1 copy of the cnunk than just decrease copies counter
        log(0, "Decrease copies for chunk " + Integer.toHexString(hash) + " from " + rs.getInt("copies") + " to " + (rs.getInt("copies") - 1) + "\n");
        sql = "UPDATE CHUNKS SET copies = copies - 1 WHERE hash = " + hash;
        log(0, sql + "\n");
        stmt.executeUpdate(sql);
      } else {
        log(0, "Remove chunk " + Integer.toHexString(hash) + " completely\n");
        
        // and remove it from the storage
        File file = new File(basePath + "\\" + Integer.toHexString(hash));
        file.delete();
        
        // otherwise delete record about this chunk from database
        sql = "DELETE FROM CHUNKS WHERE hash = " + hash;
        log(0, sql + "\n");
        stmt.executeUpdate(sql);
      }
      c.commit();
      return true;
    }
    rs.close(); 
    
    // we don't have chunk saved and need data to store them
    return false;
  } 
  
  public byte[] getChunk(Integer hash) throws SQLException, IOException {
    log(0, "getChunk(" + hash + ")\n");
    /*
     *  check, if we have this chunk we will read it and return byte array
     *  if no copies exists we will return null
     */
    byte[] data;
    sql = "SELECT * FROM CHUNKS WHERE hash = " + hash;
    log(0, sql + "\n");
    rs = stmt.executeQuery(sql);
    if (rs.next()) {
      log(0, "Chunk with hash: " + Integer.toHexString(hash) + " has " + rs.getInt("copies") + " copies\n");
      data = new byte[rs.getInt("size")];
      // read chunk from the storage
      FileInputStream fis = new FileInputStream(basePath + "\\" + Integer.toHexString(hash));
      fis.read(data);
      fis.close();
      return data;
    }
    rs.close(); 
    
    // we don't have chunk saved
    return null;
  }  
}