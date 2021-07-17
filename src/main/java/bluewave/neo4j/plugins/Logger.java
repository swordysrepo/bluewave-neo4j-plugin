package bluewave.neo4j.plugins;
import java.util.List;
import java.util.LinkedList;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import java.sql.Clob;
import java.sql.PreparedStatement;

import javaxt.sql.*;
import javaxt.json.JSONObject;
import static javaxt.utils.Console.console;


//******************************************************************************
//**  Logger Class
//******************************************************************************
/**
 *   Used to log server activity to a text file. A new text file is created
 *   for each day.
 *
 ******************************************************************************/

public class Logger implements Runnable {

    private static List pool = new LinkedList();
    private File logDir;
    private FileChannel outChannel;
    private FileOutputStream outputFile;
    private int date;
    private java.util.TimeZone tz;
    private Long maxFileSize;

    private JSONObject webconfig;
    private Database database;
    private final static long  jvm_diff;
    static {
        jvm_diff = System.currentTimeMillis()*1000_000-System.nanoTime();
    }


  //**************************************************************************
  //** Constructor
  //**************************************************************************
    public Logger() {
        this.date = -1;
        this.tz = javaxt.utils.Date.getTimeZone("UTC");
    }


  //**************************************************************************
  //** setDirectory
  //**************************************************************************
    public void setDirectory(javaxt.io.Directory dir){
        if (dir==null){
            logDir = null;
        }
        else{
            dir.create();
            if (dir.exists()){
                logDir = dir.toFile();
            }
            else{
                logDir = null;
            }
        }
    }


  //**************************************************************************
  //** setMaxFileSize
  //**************************************************************************
    public void setMaxFileSize(Long maxFileSize){
        this.maxFileSize = maxFileSize;
    }


  //**************************************************************************
  //** setDatabase
  //**************************************************************************
    public void setDatabase(Database database) throws Exception {
        this.database = database;
        initDatabase();
    }


  //**************************************************************************
  //** setWebServer
  //**************************************************************************
    public void setWebServer(JSONObject webconfig){
        this.webconfig = webconfig;
    }


  //**************************************************************************
  //** log
  //**************************************************************************
    public void log(String action, String type, String data, String user){
        long t = getCurrentTime();

        synchronized (pool) {
            pool.add(new Object[]{
                t, action, type, data, user
            });
            pool.notifyAll();
        }
    }


  //**************************************************************************
  //** run
  //**************************************************************************
    public void run() {
        while (true) {

            Object obj;
            synchronized (pool) {
                while (pool.isEmpty()) {
                  try {
                    pool.wait();
                  }
                  catch (InterruptedException e) {
                      break;
                  }
                }
                obj = pool.remove(0);
            }

            if (obj==null) return;

            Object[] arr = (Object[]) obj;
            Long t = (Long) arr[0];
            String action = (String) arr[1];
            String type = (String) arr[2];
            String data = (String) arr[3];
            String user = (String) arr[4];


            String msg = t + "," + action + "," + type + "," + data + "," + user;
            console.log(msg);


          //Log info to a file
            if (logDir!=null){
                try{
                    String str = msg + "\r\n";
                    byte[] b = str.getBytes();
                    ByteBuffer output = ByteBuffer.allocateDirect(b.length);
                    output.put(b);
                    output.flip();
                    getFileChannel().write(output);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
            }


          //Post info to a webserver
            if (webconfig!=null){
                String url = webconfig.get("url").toString();
                if (url!=null){
                    javaxt.http.Request request = new javaxt.http.Request(url);
                    String username = webconfig.get("username").toString();
                    String password = webconfig.get("password").toString();
                    if (username!=null && password!=null){
                        request.setCredentials(username, password);
                    }
                    request.setRequestMethod("POST");
                    request.setNumRedirects(0);
                    request.write(msg);
                }
            }


          //Update database
            if (database!=null){
                Connection conn = null;
                try{
                    conn = database.getConnection();
                    java.sql.Connection c = conn.getConnection();

                    PreparedStatement preparedStatement = c.prepareStatement(
                    "INSERT INTO TRANSACTION (action, type, data, username, timestamp) " +
                    "VALUES (?, ?, ?, ?, ?)");

                    Clob clob = c.createClob();
                    clob.setString(1, data);

                    preparedStatement.setString(1, action);
                    preparedStatement.setString(2, type);
                    preparedStatement.setClob(3, clob);
                    preparedStatement.setString(4, user);
                    preparedStatement.setLong(5, t);
                    preparedStatement.executeUpdate();
                    preparedStatement.close();

                    conn.close();
                }
                catch(Exception e){
                    if (conn!=null) conn.close();
                }
            }
        }
    }


  //**************************************************************************
  //** getFileChannel
  //**************************************************************************
    private FileChannel getFileChannel() throws Exception {

        int date = Integer.parseInt(new SimpleDateFormat("yyyyMMdd").format(new java.util.Date()));
        if (date>this.date){
            this.date = date;

            if (outputFile!=null) outputFile.close();
            if (outChannel!=null) outChannel.close();

            File file = new File(logDir, this.date + ".log");
            outputFile = new FileOutputStream(file, true);
            outChannel = outputFile.getChannel();
        }
        else{
            File file = new File(logDir, this.date + ".log");
            if (maxFileSize!=null){
                if (file.length()>maxFileSize){
                    if (outputFile!=null) outputFile.close();
                    if (outChannel!=null) outChannel.close();
                    throw new Exception("Log file too big: " + file.length() + " vs " + maxFileSize);
                }
            }
        }
        return outChannel;
    }


  //**************************************************************************
  //** initDatabase
  //**************************************************************************
    private void initDatabase() throws Exception {


      //Create tables as needed
        Connection conn = null;
        try{
            conn = database.getConnection();

            boolean initSchema;
            javaxt.io.File db = new javaxt.io.File(database.getHost() + ".mv.db");
            if (!db.exists()){
                initSchema = true;
            }
            else{
                Table[] tables = Database.getTables(conn);
                initSchema = tables.length==0;
            }


            if (initSchema){
                db.getDirectory().create();

                String cmd =
                "CREATE TABLE IF NOT EXISTS TRANSACTION( " +
                "id bigint auto_increment, " +
                "action varchar(10), "+
                "type varchar(25), " +
                "data clob, " +
                "username varchar(35), " +
                "timestamp LONG);";

                java.sql.Statement stmt = conn.getConnection().createStatement();
                stmt.execute(cmd);
                stmt.close();
            }


            conn.close();
        }
        catch(Exception e){
            if (conn!=null) conn.close();
            throw e;
        }


      //Inititalize connection pool
        database.initConnectionPool();
    }


  //**************************************************************************
  //** getDate
  //**************************************************************************
    public javaxt.utils.Date getDate(){
        javaxt.utils.Date d = new javaxt.utils.Date();
        d.setTimeZone(tz);
        return d;
    }


  //**************************************************************************
  //** getCurrentTime
  //**************************************************************************
  /** Returns current time in nanoseconds
   */
    private static long getCurrentTime(){
        return System.nanoTime()+jvm_diff;
    }
}