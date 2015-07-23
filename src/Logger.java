import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;


public class Logger {
  // log filename
  String logFileName = null;
  // to write log data
  PrintWriter logWriter = null;
  FileOutputStream log = null;
  
  // to select density of log messages
  int logLevel = 0;

  // variable to make log synchronized on System.out
  final boolean synchronizedLog = true;

  // Input date format
  //static SimpleDateFormat dateFormatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
  public static SimpleDateFormat dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS");

  public Logger(int logLevel, String logFilename) throws IncorrectLogFileException {
    //System.out.println("logLevel: " + logLevel + "; logFilename: " + logFilename);
    initLogger(logLevel, logFilename);
  }
  
  void initLogger(int logLevel, String logFileName) throws IncorrectLogFileException {
    this.logLevel = logLevel;
    if (logFileName == null) {
      return; // log may be used without log file specified
    }
    try {
      FileWriter fw = new FileWriter(logFileName, true);
      BufferedWriter bw = new BufferedWriter(fw);
      logWriter = new PrintWriter(bw);
    } catch (IOException e) {
      throw new IncorrectLogFileException();
    }      
  }
  
  void log (int logLevel, String message) {
    if (synchronizedLog) {
      // synchronize to ensure log consistency
      synchronized (System.out) {
        _log (logLevel, message);
      }
    } else {
      _log (logLevel, message);
    }
  }

  void _log (int logLevel, String message) {
    // log only messages with specific log level
    if (logLevel > this.logLevel)
      return;
    
    message = dateFormatter.format(System.currentTimeMillis()) + " >> " + message;
            
    if (logWriter != null) {
      logWriter.write(message);
      logWriter.flush();
    }
    System.out.print(message);
  } 

}
