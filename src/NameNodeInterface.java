import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashSet;

public interface NameNodeInterface extends Remote {
  // check whether remote file exists
  boolean isFileExists(String path) throws RemoteException;
  
  // check whether remote directory exists
  boolean isDirectoryExists(String path) throws RemoteException;
  
  // get handle to file within the DFS
  RemoteFileInterface getFile(String path) throws RemoteException, IOException;
  
  // getters and setters
  public String getBasePath() throws RemoteException;
  public void setBasePath(String basePath) throws RemoteException;
  public long getHddQuota() throws RemoteException;
  public void setHddQuota(long hddQuota) throws RemoteException;
  
  // data nodes related functions
  public int numberOfDataNodes() throws RemoteException;
  public HashSet<String> getDataNodesAddresses() throws RemoteException;
  public void setDataNodesAddresses(HashSet<String> dataNodesAddresses) throws RemoteException;
}
