import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;


public interface DataNodeInterface extends Remote {
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
  
  // the most important functions
  public boolean saveChunk(Integer hash) throws RemoteException, SQLException;
  public boolean saveChunk(byte[] data) throws RemoteException, SQLException, IOException;
  public boolean deleteChunk(Integer hash) throws RemoteException, SQLException;
  public byte[] getChunk(Integer hash) throws RemoteException, SQLException, IOException;
}
