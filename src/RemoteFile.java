import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;


public class RemoteFile extends UnicastRemoteObject implements RemoteFileInterface {
  //base path where all file structure is stored
  static String basePath = NameNode.basePath;
  
  // Local file handle
  private File file;
  
  /**
   * 
   */
  private static final long serialVersionUID = -4741859242281246177L;

  public RemoteFile() throws RemoteException {
    NameNode.log(0, "RemoteFile()\n");
  }
  
  public RemoteFile(String path) throws IOException {
    NameNode.log(0, "RemoteFile(" + virtualToAbsolutePath(path) + ")\n");
    file = new File(virtualToAbsolutePath(path));
  }
  
  public RemoteFile(File file) throws IOException {
    NameNode.log(0, "RemoteFile(" + virtualToAbsolutePath(file.getPath()) + ")\n");
    this.file = new File(virtualToAbsolutePath(file.getPath()));
  }
  
  public static RemoteFile fromFile(File file) throws IOException {
    NameNode.log(0, "RemoteFile fromFile(" + file + ")\n");
    return new RemoteFile(file);
  }
  
  //return list of files in the current folder
  public RemoteFileInterface[] listFiles() throws IOException {
    NameNode.log(0, "RemoteFileInterface[] listFiles()\n");
    File[] files = file.listFiles();
    RemoteFileInterface[] remoteFiles = new RemoteFileInterface[files.length]; 
    for (int i = 0; i < files.length; i++) {
      remoteFiles[i] = new RemoteFile(absoluteToVirtualPath(files[i].getCanonicalPath()));
    }
    return remoteFiles;
  }
  
  // convert into String
  public String remoteToString() throws IOException {
    NameNode.log(0, "String remoteToString()\n");
    return absoluteToVirtualPath(toString());
  }
  
  //convert into String
  @Override
  public String toString() {
    return file.toString();
  }
  
  public String getAbsolutePath() throws IOException {
    return file.getCanonicalPath();
  }
  
  public String getVirtualPath() throws IOException {
    return absoluteToVirtualPath(file.getCanonicalPath());
  }
  
  public String absoluteToVirtualPath(String path) throws IOException {
    NameNode.log(0, "absoluteToVirtualPath(" + path + ")\n");
    File f = new File(path);
    String canonicalPath = f.getCanonicalPath();
    String absolutePath = canonicalPath.replaceFirst(Pattern.quote(NameNode.basePath), "");
    return absolutePath.equals("") ? "\\" : absolutePath; 
  }
  
  public String virtualToAbsolutePath(String path) throws IOException {
    NameNode.log(0, "virtualToAbsolutePath(" + path + ")\n");    
    File f = new File(NameNode.basePath + "/" + path);
    String canonicalPath = f.getCanonicalPath();
    return canonicalPath.indexOf(basePath) == -1 ? basePath : canonicalPath;
  }
  
  // same functions as File class has, but remote
  public boolean exists() {
    return file.exists();
  }
  
  public boolean isDirectory() {
    return file.isDirectory();
  }
  
  public boolean isFile() {
    return file.isFile();
  }
  
  public boolean mkdir() {
    return file.mkdir();
  }
  
  public boolean mkdirs() {
    return file.mkdirs();
  }
  
  public boolean delete(boolean recursive) throws IOException, SQLException {
    NameNode.log(0, "rmdir(" + recursive + ")\n");
    
    File[] files = file.listFiles();
    RemoteFileInterface remoteFile;
    
    if (files == null) {
      // it's not a folder
    } else if (files.length > 0) {
      // folder is not empty
      if (recursive) {
        for (int i = 0; i < files.length; i++) {
          remoteFile = new RemoteFile(absoluteToVirtualPath(files[i].getCanonicalPath()));
          if (!remoteFile.delete(recursive)) {
            return false;
          }
        }
      } else {
        NameNode.log(0, "Folder \"" + file.getCanonicalPath() + "\" is not empty.");
        return false;
      }
    }

    // if it's a file and not a directory we need to request chunk deletion also 
    if (isFile()) {
      byte[] buffer = null;
      List<int[]> chunks = getChunksList(); 
    
      for (int[] chunk : chunks) {
        NameNode.dataNodes[chunk[1]].deleteChunk(chunk[0]);
      }
    }
     
    if (file.getCanonicalPath().equals(basePath)) {
      return false;
    }

    return file.delete();
  }
  
  /*
   *  check if directory is empty
   *  1 - empty
   *  0 - not empty
   *  -1 - not a directory
   */
  public int isEmpty() throws IOException {
    NameNode.log(0, "isEmpty()\n");
    File[] files = file.listFiles();
    
    if (files == null) {
      return -1;
    } else if (files.length > 0) {
      // folder is not empty
      return 0;
    } else {
      return 1;
    }
  }
  
  public List<int[]> getChunksList() throws IOException {
    if (exists()) {
      FileInputStream fis = new FileInputStream(file);
      byte[] data = new byte[(int) file.length()];
      fis.read(data);
      fis.close();
      return (List<int[]>) deserializeObject(data);
    } else {
      return null;
    }
  }
  
  public boolean saveChunksList(List<int[]> list) throws IOException {
    if (list != null) {
      FileOutputStream fos = new FileOutputStream(file);
      byte[] data = serializeObject(list);
      fos.write(data);
      fos.close();
      return true;
    } else {
      return false;
    }
  }  
  
  static byte[] serializeObject(Object obj) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(obj);
      out.flush();
      byte[] bytes = bos.toByteArray();
      //log("Size of serialized object is: " + bytes.length + " bytes\n");
      //log(Node.dateFormatter.format(Node.localTimeMillis()) + " >> ");
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      //System.out.println("Finally");
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException e) {
        new RuntimeException(e);
      }
      try {
        bos.close();
      } catch (IOException e) {
        new RuntimeException(e);
      }
    }
  }

  static Object deserializeObject(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInput in = null;
    try {
      in = new ObjectInputStream(bis);
      Object obj = in.readObject();
      return obj;
    } catch (IOException e) {
      NameNode.log(0, "bytes.length = " + bytes.length);
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        bis.close();
      } catch (IOException e) {
        new RuntimeException(e);
      }
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        new RuntimeException(e);
      }
    }
  }  
}
