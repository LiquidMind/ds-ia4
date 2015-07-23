import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;


public interface RemoteFileInterface extends Remote {
  // return list of files in the current folder
  public RemoteFileInterface[] listFiles() throws RemoteException, IOException;
  
  // string representation of the file (essentially file path)
  public String remoteToString() throws RemoteException, IOException;
  
  // different useful file and directory functions
  public boolean exists() throws RemoteException;
  public boolean isDirectory() throws RemoteException;
  public boolean isFile() throws RemoteException;
  public boolean mkdir() throws RemoteException;
  public boolean mkdirs() throws RemoteException;
  public boolean delete(boolean recursive) throws RemoteException, IOException, SQLException;
  public int isEmpty() throws RemoteException, IOException;
  
  // functions for working with chunks
  public boolean saveChunksList(List<int[]> list) throws RemoteException, IOException;
  public List<int[]> getChunksList() throws RemoteException, IOException;
  
  public String getAbsolutePath() throws RemoteException, IOException;
  public String getVirtualPath() throws RemoteException, IOException;
}
