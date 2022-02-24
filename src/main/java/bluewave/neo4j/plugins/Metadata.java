package bluewave.neo4j.plugins;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;

import javaxt.json.*;
import static javaxt.utils.Console.console;


//******************************************************************************
//**  Metadata Class
//******************************************************************************
/**
 *   Used to create and update metadata for the graph. Metadata is stored in a
 *   metadata node. Optionally, metadata can be cached to a local file for
 *   faster startup.
 *
 ******************************************************************************/

public class Metadata implements Runnable {

    private static List pool = new LinkedList();
    private ConcurrentHashMap<String, Object> cache;
    private javaxt.io.Directory cacheDir;

    public static String META_NODE_LABEL = "bluewave_metadata";
    public static final String KEY_COUNTS = "counts";
    public static final String KEY_PROPERTIES = "nodes";
    
    private GraphDatabaseService db;

    private java.util.Timer syncTimer;
    private java.util.Timer refreshTimer;

    // TEST
    // private long refreshInterval = 24 * 60 * 60 * 1000; // 24 hours
    // private long refreshDelay = 24 * 60 * 60 * 1000; // 24 hours
    // private long syncInterval = 60 * 1000; // 60s
    // private long synchDelay = 10 * 60 * 1000; // 10m    

    private long refreshInterval = 3 * 60 * 1000; // 3m
    private long refreshDelay = 3 * 60 * 1000; // 3m

    private long syncInterval = 30 * 1000; // 30s
    private long synchDelay = 10 * 1000; // 10s

    private CountsHandler countsHandler;
    private PropertiesHandler propertiesHandler;

    public static long lastUpdate = 0;

  //**************************************************************************
  //** Constructor
  //**************************************************************************
    public Metadata(GraphDatabaseService databaseService, boolean recordCounts, boolean recordProperties) {
        cache = new ConcurrentHashMap<>();
        db = databaseService;

        syncTimer = new java.util.Timer();
        refreshTimer = new java.util.Timer();

        if(recordCounts) countsHandler = new CountsHandler(db);
        if(recordProperties) propertiesHandler = new PropertiesHandler(db);
    }


  //**************************************************************************
  //** init
  //**************************************************************************
  /** Used to initialize the Metadata class and populate the metadata node
   */
    public void init() {
        long startTime = System.currentTimeMillis();
        
        try {
            if(propertiesHandler != null) propertiesHandler.init();
        } catch(Exception e) {
            e.printStackTrace();
        }

        try{
            getNodes();
            getProperties();
        }
        catch(Exception e){
        }
        long ellapsedTime = System.currentTimeMillis()-startTime;
        console.log("Fetched nodes and properties in " + ellapsedTime + "ms");
        //TODO: unsure the timer task interval is slower than the ellapsedTime


        startTimers();
    }


  //**************************************************************************
  //** setNodeName
  //**************************************************************************
    public void setNodeName(String nodeName){
        if (nodeName==null || nodeName.isBlank()) return;
        META_NODE_LABEL = nodeName;
    }


  //**************************************************************************
  //** setCacheDirectory
  //**************************************************************************
    public void setCacheDirectory(javaxt.io.Directory dir){
        cacheDir = dir;
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

            //TODO: updateCache()
        }
    }


  //**************************************************************************
  //** stop
  //**************************************************************************
    public void stop(){
        cancelTimers();

        synchronized (pool) {
            pool.clear();
            pool.add(0, null);
            pool.notifyAll();
        }
    }


  //**************************************************************************
  //** start timer tasks
  //**************************************************************************
    private void startTimers() {
        if (syncTimer == null) {
            syncTimer = new java.util.Timer();
        }
        syncTimer.scheduleAtFixedRate(new java.util.TimerTask() {
            public void run() {
                if(propertiesHandler != null) propertiesHandler.sync(false);
            }
        }, synchDelay, syncInterval);

        if (refreshTimer == null) {
            refreshTimer = new java.util.Timer();
        }
        refreshTimer.scheduleAtFixedRate(new java.util.TimerTask() {
            public void run() {
                try {
                    try {
                        if(syncTimer != null) {
                            syncTimer.cancel();
                            syncTimer = null;
                        }
                    }catch(Exception e){}
                    if(propertiesHandler != null) propertiesHandler.refresh();
                }finally {
                    syncTimer = new java.util.Timer();
                    syncTimer.scheduleAtFixedRate(new java.util.TimerTask() {
                        public void run() {
                            if(propertiesHandler != null) propertiesHandler.sync(false);
                        }
                    }, synchDelay, syncInterval);
                }
            }
        }, refreshDelay, refreshInterval);
    }


  //**************************************************************************
  //** cancel Sync Timer
  //**************************************************************************
    private void cancelTimers() {
        if (syncTimer != null) {
            syncTimer.cancel();
            syncTimer = null;
        }
    
        if (refreshTimer != null) {
            refreshTimer.cancel();
            refreshTimer = null;
        }
    }

  //**************************************************************************
  //** handleEventBeforeCommit
  //**************************************************************************
    public void handleEventBeforeCommit(final TransactionData data) throws Exception {
    }


  //**************************************************************************
  //** handleEventAfterCommit
  //**************************************************************************
    public synchronized void handleEventAfterCommit(final TransactionData data) throws Exception {
    }

    public void handleEvent(TransactionData data) {


        try {
            if(countsHandler != null) countsHandler.handleEvent(data);
            if(propertiesHandler != null) propertiesHandler.handleEvent(data);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    private void e(Object message) {
        console.log("*** ------- ERROR ------- *** " + message.toString());
    }

  //**************************************************************************
  //** isSafeToSync
  //**************************************************************************    
    public static boolean isSafeToSync() {
        if(lastUpdate == 0) return true;

        if(System.currentTimeMillis() - lastUpdate >= (60 * 1000)) return true;

        return false;
    }

  //**************************************************************************
  //** log
  //**************************************************************************
  /** Used to add an event to the queue
   */
    public void log(String action, String type, JSONArray data, String username){
        lastUpdate = System.currentTimeMillis();
        //TODO: add event to pool
    }


  //**************************************************************************
  //** updateCache
  //**************************************************************************
  /** Used to update the cached nodes and properties
   */
    private void updateCache(String action, String type, JSONArray data, String username){

        if ((action.equals("create") || action.equals("delete")) && (type.equals("nodes") || type.equals("relationships")))
        synchronized(cache){
            try{

              //Update nodes
                JSONArray nodes = getNodes();
                boolean updateFile = false;
                for (int i=0; i<data.length(); i++){
                    JSONArray entry = data.get(i).toJSONArray();
                    if (entry.isEmpty()) continue;


                    HashSet<String> labels = new HashSet<>();
                    for (int j=1; j<entry.length(); j++){
                        String label = entry.get(j).toString();
                        if (label!=null) label = label.toLowerCase();
                        labels.add(label);
                    }

                    boolean foundMatch = false;
                    for (int j=0; j<nodes.length(); j++){
                        JSONObject node = nodes.get(j).toJSONObject();

                        String label = node.get("node").toString();
                        if (label!=null && labels.contains(label.toLowerCase())){
                            if (type.equals("nodes")){
                                AtomicLong count = (AtomicLong) node.get("count").toObject();
                                Long a = count.get();
                                if (action.equals("create")){
                                    count.incrementAndGet();
                                }
                                else{
                                    Long n = count.decrementAndGet();
                                    if (n==0){
                                        //TODO: Remove node
                                    }
                                }
                                Long b = count.get();
                                console.log((b>a ? "increased " : "decreased ") + label + " to " + b);
                                updateFile = true;
                            }
                            else if (type.equals("relationships")){
                                AtomicLong relations = (AtomicLong) node.get("relations").toObject();
                            }

                            foundMatch = true;
                            break;
                        }
                    }


                    if (!foundMatch){
                        if (action.equals("create") && (type.equals("nodes"))){
                            console.log("add node!");
                            String label = entry.get(1).toString();
                            AtomicLong count = new AtomicLong(1);
                            AtomicLong relations = new AtomicLong(0);
                            JSONObject json = new JSONObject();
                            json.set("node", label);
                            json.set("count", count);
                            json.set("relations", relations);
                            json.set("id", label);
                            nodes.add(json);
                            updateFile = true;
                        }
                    }
                }






              //TODO: Update properties



              //TODO: Update network


                cache.notifyAll();

            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }


    private static final String nodesQuery =
    "MATCH (n) RETURN\n" +
    "distinct labels(n) as labels,\n" +
    "count(labels(n)) as count,\n" +
    "sum(size((n) <--())) as relations;";



  //**************************************************************************
  //** getNodes
  //**************************************************************************
    private JSONArray getNodes() throws Exception {

        synchronized(cache){
            Object obj = cache.get("nodes");
            if (obj!=null){
                return (JSONArray) obj;
            }
            else{
                JSONArray arr = new JSONArray();

                javaxt.io.File f = null;
                if (cacheDir!=null){
                    f = new javaxt.io.File(cacheDir, "nodes.json");
                }

                if (f!=null && f.exists()){
                    arr = new JSONArray(f.getText());
                    for (int i=0; i<arr.length(); i++){
                        JSONObject node = arr.get(i).toJSONObject();
                        Long c = node.get("count").toLong();
                        Long r = node.get("relations").toLong();
                        AtomicLong count = new AtomicLong(c==null ? 0 : c);
                        AtomicLong relations = new AtomicLong(r==null ? 0 : r);
                        node.set("count", count);
                        node.set("relations", relations);
                    }
                }
                else{

                    try (Transaction tx = db.beginTx()) {

                      //Execute query
                        Result rs = tx.execute(nodesQuery);
                        while (rs.hasNext()){
                            Map<String, Object> r = rs.next();
                            JSONArray labels = new JSONArray(r.get("labels").toString());
                            String label = labels.isEmpty()? "" : labels.get(0).toString();

                            Long _count = Long.parseLong(r.get("count").toString());
                            AtomicLong count = new AtomicLong(_count);

                            Long _relations = Long.parseLong(r.get("relations").toString());
                            AtomicLong relations = new AtomicLong(_relations);

                            JSONObject json = new JSONObject();
                            json.set("node", label);
                            json.set("count", count);
                            json.set("relations", relations);
                            json.set("id", label);
                            arr.add(json);
                        }


                      //Write file
                        if (f!=null){
                            f.create();
                            f.write(arr.toString());
                        }
                    }
                    catch (Exception e) {
                        e("executeNodesQuery: " + e);
                        throw e;
                    }
                }



              //Update cache
                cache.put("nodes", arr);
                cache.notify();


                return arr;

            }
        }
    }

    private String propertiesQuery =
    "MATCH(n)\n" +
    "WITH LABELS(n) AS labels , KEYS(n) AS keys\n" +
    "UNWIND labels AS label\n" +
    "UNWIND keys AS key\n" +
    "RETURN DISTINCT label as node, COLLECT(DISTINCT key) AS properties\n" +
    "ORDER BY label";


  //**************************************************************************
  //** getProperties
  //**************************************************************************
    private JSONArray getProperties() throws Exception {
        synchronized(cache){
            Object obj = cache.get("properties");
            if (obj!=null){
                return (JSONArray) obj;
            }
            else{
                JSONArray arr = new JSONArray();

                javaxt.io.File f = null;
                if (cacheDir!=null){
                    f = new javaxt.io.File(cacheDir, "properties.json");
                }

                if (f!=null && f.exists()){
                    arr = new JSONArray(f.getText());
                }
                else{

                    try (Transaction tx = db.beginTx()) {

                      //Execute query
                        Result rs = tx.execute(propertiesQuery);
                        while (rs.hasNext()){
                            Map<String, Object> r = rs.next();
                            Object node = r.get("node");
                            Object props = r.get("properties");
                            if (node==null) continue;


                            JSONObject json = new JSONObject();
                            json.set("node", node);
                            JSONArray properties = new JSONArray();
                            json.set("properties", properties);
                            if (props!=null){
                                for (Object p : (List) props){
                                    properties.add(p);
                                }
                            }
                            arr.add(json);
                        }



                      //Write file
                        if (f!=null){
                            f.create();
                            f.write(arr.toString());
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        e("executePropertiesQuery: " + e);
                        throw e;
                    }
                }



              //Update cache
                cache.put("properties", arr);
                cache.notify();


                return arr;

            }
        }
    }

}