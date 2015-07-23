import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;


public class Client {
  //flag to notify that client should keep working
  static boolean keepWorking = true;
  
  // scanner to read user input
  private static Scanner userInput = new Scanner(System.in);
  // message that was read from the user
  private static String message = "";
  // processed message 
  private static String msg = "";
  
  // reference to Logger class
  private static Logger logger;
  
  // current directory within the DFS
  private static String currentFolder = "\\";
  
  static HashSet<String> dataNodesAddresses;
   
  public static void main(String[] args) throws UninitializedLoggerException, IOException, SQLException {

    try {
      // init everything to log messages
      if (args.length > 2) {
        initLogger(0, args[0]); // 1 - logLevel, 2 - logFilename
      } else {
        initLogger(0, null); // 1 - logLevel, 2 - logFilename
      }
      
      if (args.length < 2) {
        throw new IncompleteArgumentListException();
      }
    
      // init security manager
      if (System.getSecurityManager() == null) {
        System.setSecurityManager(new SecurityManager());
      }
  
      //Registry registry = LocateRegistry.getRegistry(args[1]);
      //NameNodeInterface nameNode = (NameNodeInterface) registry.lookup("NameNode");
      
      NameNodeInterface nameNode = (NameNodeInterface) Naming.lookup("NameNode");
      RemoteFileInterface remoteFile = (RemoteFileInterface) Naming.lookup("RemoteFile");
      //DataNodeInterface dataNode = (DataNodeInterface) Naming.lookup("DataNode");
      
      DataNodeInterface[] dataNodes = null; 
      
      dataNodesAddresses = nameNode.getDataNodesAddresses();
      log(0, "There are " + dataNodesAddresses.size() + " data nodes:\n");
      dataNodes = new DataNodeInterface[dataNodesAddresses.size()];
      for (String s : dataNodesAddresses) {
        String[] parts = s.split(":");
        log(0, "ID: " + parts[0] + ", host: " + parts[1] + ", port: " + parts[2] + "\n");
        dataNodes[Integer.parseInt(parts[0]) - 1] = (DataNodeInterface) Naming.lookup("//" + parts[1] + ":" + parts[2] + "/DataNode"); // "//host:port/name"
      }
      
      userInput  = new Scanner(System.in);
      
      log(0, "Type your message and press <Enter>\n");
      log(0, "Type \"exit\" and press <Enter> for exit\n");
      
      while(keepWorking){
        System.out.print(Logger.dateFormatter.format(System.currentTimeMillis()) + " >> ");
        if (userInput.hasNextLine()) {
          message = userInput.nextLine();
        } else {
          message = "exit";
        }
        
        String[] splitArray = message.trim().split("\\s+");
        msg = splitArray[0].toLowerCase();
  
        if (msg.equals("exit")) {
          // notify to stop thread and close connection
          keepWorking = false;
          
        } else if (msg.equals("quota")) {
          // print DFS HDD quota
          log(0, "HDD Quota: " + nameNode.getHddQuota() + " bytes\n");
          
        } else if (msg.equals("ls")) {
          RemoteFileInterface folder = null;
          
          // print content of the current directory
          if (splitArray.length == 1) {
            folder = nameNode.getFile(currentFolder);
          } else if (splitArray.length == 2) {
            // print content of the directory in parameter
            String path = null;
            if (splitArray[1].startsWith("\\")) {
              path = splitArray[1];
            } else {
              path = currentFolder + "\\" + splitArray[1];
            }
            folder = nameNode.getFile(path);
            if (!folder.exists()) {
              log(0, "Directory \"" + folder.remoteToString() + "\" doesn't exist\n");
              folder = null;
            } else if (!folder.isDirectory()) {
              log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
              folder = null;
            }
          } else {
            log(0, "Use: ls [directory_name]");
          }
          
          if (folder != null) {
            RemoteFileInterface[] listOfFiles = folder.listFiles();
            String pattern = folder.remoteToString() + (folder.remoteToString().equals("\\") ? "" : "\\"); 
            if (listOfFiles.length == 0) {
              log(0, "Folder \"" + folder.remoteToString() + "\" is empty\n");
            } else {
              log(0, "Folder \"" + folder.remoteToString() + "\" contains:\n");
              for (int i = 0; i < listOfFiles.length; i++) {
                log(0, "  " + listOfFiles[i].remoteToString().replaceFirst(Pattern.quote(pattern), "") + "\n");
              }
            }
          }
          
        } else if (msg.equals("pwd")) {
          // print working directory
          RemoteFileInterface folder = nameNode.getFile(currentFolder);
          log(0, "Current working directory is: \"" + folder.remoteToString() + "\"\n");
          
        } else if (msg.equals("cd")) {
          // change working directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: cd <directory name>");
          } else {
            String path = null;
            if (splitArray[1].startsWith("\\")) {
              path = splitArray[1];
            } else {
              path = currentFolder + "\\" + splitArray[1];
            }
            RemoteFileInterface folder = nameNode.getFile(path);
            if (!folder.exists()) {
              log(0, "Directory \"" + folder.remoteToString() + "\" doesn't exist\n");
            } else if (!folder.isDirectory()) {
              log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
            } else {
              log(0, "Working directory has being changed to: \"" + folder.remoteToString() + "\"\n");
              currentFolder = folder.remoteToString();
            }
          }
          
        } else if (msg.equals("mkdir")) {
          // create new directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: mkdir <directory name>");
          } else {
            String path = null;
            if (splitArray[1].startsWith("\\")) {
              path = splitArray[1];
            } else {
              path = currentFolder + "\\" + splitArray[1];
            }
            RemoteFileInterface folder = nameNode.getFile(path);
            if (folder.exists()) {
              log(0, "Name \"" + folder.remoteToString() + "\" already exists\n");
            } else {
              // create directory
              if (folder.mkdir()) {
                log(0, "Directory \"" + folder.remoteToString() + "\" has being created\n");
              } else {
                log(0, "Directory \"" + folder.remoteToString() + "\" can't be created\n");
              }
            }
          }
          
        } else if (msg.equals("rmdir")) {
          // change working directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: rmdir <directory name>");
          } else {
            String path = null;
            if (splitArray[1].startsWith("\\")) {
              path = splitArray[1];
            } else {
              path = currentFolder + "\\" + splitArray[1];
            }
            
            RemoteFileInterface folder = nameNode.getFile(path);
            
            if (folder.getVirtualPath().equals("\\")) {
              log(0, "You can't remove root of the DFS.\n");
              log(0, "To clear everythin use: init\n");
            } else if (!folder.exists()) {
              log(0, "Directory \"" + folder.remoteToString() + "\" doesn't exist\n");
            } else if (!folder.isDirectory()) {
              log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
            } else {
              switch (folder.isEmpty()) {
                case -1:
                  // this is not possible, because we've filtered it earlier
                  log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
                  break;
                case 0:
                  // directory is not empty, ask about recursive delete
                  log(0, "Directory \"" + folder.remoteToString() + "\" is not empty. Delete recursively?\n");
                  System.out.print(Logger.dateFormatter.format(System.currentTimeMillis()) + " >> Yes or No >> ");
                  if (userInput.hasNextLine()) {
                    message = userInput.nextLine();
                  } else {
                    message = "n";
                  }
                  if (message.trim().toLowerCase().equals("y") || message.trim().toLowerCase().equals("yes")) {
                    if (folder.delete(true)) {
                      log(0, "Directory \"" + folder.remoteToString() + "\" and its content was removed\n");
                    } else {
                      log(0, "Directory \"" + folder.remoteToString() + "\" can't be removed\n");
                    }
                  } else {
                    log(0, "Directory \"" + folder.remoteToString() + "\" was not deleted\n");
                  }
                  break;
                case 1:
                  if (folder.delete(false)) {
                    log(0, "Directory \"" + folder.remoteToString() + "\" was removed\n");
                  } else {
                    log(0, "Directory \"" + folder.remoteToString() + "\" can't be removed\n");
                  }
                  break;
              }
            }
          }
          
        } else if (msg.equals("upload")) {
          // upload file from local fs to dfs
          if (splitArray.length < 3 || splitArray.length > 3) {
            log(0, "Use: upload <local name> <remote name>\n");
          } else {
            String local = splitArray[1];
            String remote = null;
            if (splitArray[2].startsWith("\\")) {
              remote = splitArray[2];
            } else {
              remote = currentFolder + "\\" + splitArray[2];
            }
            RemoteFileInterface remoteFI = nameNode.getFile(remote);
            
            boolean error = false;
            if (remoteFI.exists()) {
              log(0, "Name \"" + remoteFI.remoteToString() + "\" is already used\n");
              error = true;
            } 
            File localF = new File(local);
            if (!localF.exists()) {
              log(0, "File \"" + localF + "\" doesn't exist\n");
              error = true;
            }
            if (!localF.isFile()) {
              log(0, "\"" + localF + "\" is not a file\n");
              error = true;
            }
            
            byte[] buffer = new byte[65536];
            byte[] chunk = null;
            List<int[]> chunks = new ArrayList<int[]>(); 
            int hash;
            int dataNodeId;
            int chunkLength;
            if (!error) {
              FileInputStream fis = new FileInputStream(localF);
              for (int i = 0; (chunkLength = fis.read(buffer)) != -1; i++) {
                log(0, "chunkLength: " + chunkLength + ", buffer.length: " + buffer.length + "\n");
                chunk = Arrays.copyOfRange(buffer, 0, chunkLength);
                hash = Arrays.hashCode(chunk);
                // trick that not to get negative values here
                dataNodeId = (hash % nameNode.numberOfDataNodes() + nameNode.numberOfDataNodes()) % nameNode.numberOfDataNodes();
                if (dataNodes[dataNodeId].saveChunk(chunk)) {
                  int[] chunkData = {hash, dataNodeId};
                  chunks.add(chunkData);
                } else {
                  log(0, "Can't save chunk " + hash + " at node " + (dataNodeId + 1));
                }
              }
              fis.close();
              
              // save chunk list to name node
              remoteFI.saveChunksList(chunks);
              log(0, "File was uploaded from \"" + localF.getCanonicalPath() + "\" to " + remoteFI.remoteToString() + "\n");
            }
          }
          
        } else if (msg.equals("download")) {
          // download file from dfs to local fs
          if (splitArray.length < 3 || splitArray.length > 3) {
            log(0, "Use: download <remote name> <local name>\n");
          } else {
            String local = splitArray[2];
            String remote = null;
            if (splitArray[1].startsWith("\\")) {
              remote = splitArray[1];
            } else {
              remote = currentFolder + "\\" + splitArray[1];
            }
            RemoteFileInterface remoteFI = nameNode.getFile(remote);
            
            File localF = new File(local);
            
            boolean error = false;
            if (localF.exists()) {
              log(0, "Name \"" + localF.getCanonicalPath() + "\" is already used\n");
              error = true;
            } 
            
            if (!remoteFI.exists()) {
              log(0, "File \"" + remoteFI.remoteToString() + "\" doesn't exist\n");
              error = true;
            }
            if (!remoteFI.isFile()) {
              log(0, "\"" + remoteFI.remoteToString() + "\" is not a file\n");
              error = true;
            }
            
            byte[] buffer = null;
            List<int[]> chunks = remoteFI.getChunksList(); 
            
            if (!error) {
              FileOutputStream fos = new FileOutputStream(localF);
              
              for (int[] chunk : chunks) {
                buffer = dataNodes[chunk[1]].getChunk(chunk[0]);
                if (buffer != null) {
                  log(0, "buffer.length: " + buffer.length + "\n");
                } else {
                  log(0, "Can't read chunk " + chunk[0] + " from node " + chunk[1]);
                }
                fos.write(buffer);
              }
              fos.close();
              
              log(0, "File was saved from \"" + remoteFI.remoteToString() + "\" to " + localF.getCanonicalPath() + "\n");
            }
          }
          
        } else if (msg.equals("delete")) {
          // delete file from dfs
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: delete <file name>");
          } else {
            String path = null;
            if (splitArray[1].startsWith("\\")) {
              path = splitArray[1];
            } else {
              path = currentFolder + "\\" + splitArray[1];
            }
            RemoteFileInterface file = nameNode.getFile(path);
            if (!file.exists()) {
              log(0, "File \"" + file.remoteToString() + "\" doesn't exist\n");
            } else if (!file.isFile()) {
              log(0, "\"" + file.remoteToString() + "\" is not a file\n");
            } else {
              if (file.delete(true)) {
                log(0, "File \"" + file.remoteToString() + "\" was deleted\n");
              } else {
                log(0, "File \"" + file.remoteToString() + "\" can't be deleted\n");
              }
            }
          }
          
        } else if (msg.equals("init")) {
            RemoteFileInterface folder = nameNode.getFile("\\");

            // this will clear everything
            log(0, "This will clear everything within DFS. Are you sure?\n");
            System.out.print(Logger.dateFormatter.format(System.currentTimeMillis()) + " >> Yes or No >> ");
            if (userInput.hasNextLine()) {
              message = userInput.nextLine();
            } else {
              message = "n";
            }
            if (message.trim().toLowerCase().equals("y") || message.trim().toLowerCase().equals("yes")) {
              folder.delete(true);
              log(0, "Everything was removed from DFS\n");
              // print DFS HDD quota
              log(0, "HDD Quota: " + nameNode.getHddQuota() + " bytes\n");
            } else {
              log(0, "Init was not performed\n");
            }
        } else if (msg.equals("")) {
          // just do nothing
          
        } else {
          // print program usage
          log(0, "Usage: \n");
          log(0, "  exit - exit the program\n");
          log(0, "  help - shows possible program parameters\n");
          log(0, "  quota - shows HDD quota for the DFS\n");
          log(0, "  ls [directory_name] - list directory contents\n");
          log(0, "  cd <directory_name> - change directory\n");
          log(0, "  mkdir <directory_name> - create directory\n");
          log(0, "  rmdir <directory_name> - remove directory\n");
          log(0, "  upload <local name> <remote name> - upload file from local to remote fs\n");
          log(0, "  download <remote name> <local name> - download file from remote to local fs\n");
          log(0, "  delete <file name> - delete file from remote fs\n");
          log(0, "  init - clear everything within dfs\n");
          log(0, "  \n");
          log(0, "  to be done...\n");
        }
      }
  
      log(0, "Client was shutted down");
      
    } catch (IncompleteArgumentListException e) {
      log(0, "Use: Client logFileName NameNodeAddress");
    } catch (IncorrectLogFileException e) {
      log(0, "File to store log files can't be created or inaccessible.");
    } catch (RemoteException e) {
      log(0, "RemoteException was thrown.");
      e.printStackTrace();
    } catch (NotBoundException e) {
      log(0, "Some RMI object is not bound.");
      e.printStackTrace();
    } catch (MalformedURLException e) {
      log(0, "Malformed URI was used to lookup RMI object.");
      e.printStackTrace();
    }      
  }
  
  public static void initLogger(int logLevel, String logFilename) throws IncorrectLogFileException {
    logger = new Logger(logLevel, logFilename);
  }
  
  public static void log(int logLevel, String message) throws UninitializedLoggerException {
    if (logger == null) {
      throw new UninitializedLoggerException();
    }
    logger.log(logLevel, message);
  }  
}
