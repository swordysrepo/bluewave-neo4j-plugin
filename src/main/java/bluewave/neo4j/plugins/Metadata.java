package bluewave.neo4j.plugins;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.TransactionData;

import javaxt.json.*;
import static javaxt.utils.Console.console;


//******************************************************************************
//**  Metadata Class
//******************************************************************************
/**
 *   Used to create and update metadata for the graph. Metadata is stored in a
 *   metadata node.
 *
 ******************************************************************************/

public class Metadata implements Runnable {


    private ConcurrentHashMap<String, NodeMetadata> nodes;
    public static String META_NODE_LABEL = "bluewave_metadata";
    private GraphDatabaseService db;
    private AtomicLong lastUpdate;

    private static final String nodesQuery =
    "MATCH (n) RETURN\n" +
    "distinct labels(n) as labels,\n" +
    "count(labels(n)) as count,\n" +
    "sum(size((n) <--())) as relations;";

    private String propertiesQuery =
    "MATCH(n)\n" +
    "WITH LABELS(n) AS labels , KEYS(n) AS keys\n" +
    "UNWIND labels AS label\n" +
    "UNWIND keys AS key\n" +
    "RETURN DISTINCT label as node, COLLECT(DISTINCT key) AS properties\n" +
    "ORDER BY label";



  //**************************************************************************
  //** Constructor
  //**************************************************************************
    public Metadata(GraphDatabaseService databaseService, boolean recordCounts, boolean recordProperties) {

        nodes = new ConcurrentHashMap<>();
        db = databaseService;
        lastUpdate = new AtomicLong(0L);


      //Sync recent updates made to the nodes map every 30 seconds
        {
            long interval = 30*1000; //30 seconds
            java.util.Timer timer = new java.util.Timer();
            timer.scheduleAtFixedRate( new java.util.TimerTask(){
                public void run(){
                    synchronized (lastUpdate){
                        if (lastUpdate.get()==0) return;
                        try{
                            if ((System.currentTimeMillis()-lastUpdate.get())>interval){
                                saveNodes();
                                lastUpdate.set(0);
                            }
                        }
                        catch(Exception e){}
                    }
                }
            }, 30*1000, interval);
        }



      //Sync all nodes every 24 hours
        {
            javaxt.utils.Date startDate = new javaxt.utils.Date();
            startDate.removeTimeStamp(); startDate.add(26, "hours"); //2AM
            long interval = 24*60*60*1000; //24 hours
            java.util.Timer timer = new java.util.Timer();
            timer.scheduleAtFixedRate( new java.util.TimerTask(){
                public void run(){
                    synchronized (lastUpdate){
                        if (lastUpdate.get()==0){
                            init();
                        }
                    }
                }
            }, startDate.getDate(), interval);
        }
    }


  //**************************************************************************
  //** init
  //**************************************************************************
  /** Used to initialize the Metadata class and populate the metadata node
   */
    public void init() {
        long startTime = System.currentTimeMillis();
        try{
            updateNodes();
            saveNodes();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        long ellapsedTime = System.currentTimeMillis()-startTime;
        console.log("Fetched nodes and properties in " + ellapsedTime + "ms");


        lastUpdate.set(0);
    }


  //**************************************************************************
  //** setNodeName
  //**************************************************************************
    public void setNodeName(String nodeName){
        if (nodeName==null || nodeName.isBlank()) return;
        META_NODE_LABEL = nodeName;
    }


  //**************************************************************************
  //** run
  //**************************************************************************
    public void run() {

    }


  //**************************************************************************
  //** stop
  //**************************************************************************
    public void stop(){
        //cancelTimers();
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



  //**************************************************************************
  //** isSafeToSync
  //**************************************************************************
    public static boolean isSafeToSync() {
//        if(lastUpdate == 0) return true;
//
//        if(System.currentTimeMillis() - lastUpdate >= (60 * 1000)) return true;

        return false;
    }

  //**************************************************************************
  //** log
  //**************************************************************************
  /** Used to add an event to the queue
   */
    public void log(String action, String type, JSONArray data, String username){

        if ((action.equals("create") || action.equals("delete")) && (type.equals("nodes") || type.equals("relationships")))
        synchronized(nodes){
            try{

                for (int i=0; i<data.length(); i++){
                    JSONArray entry = data.get(i).toJSONArray();
                    if (entry.isEmpty()) continue;


                    HashSet<String> labels = new HashSet<>();
                    for (int j=1; j<entry.length(); j++){
                        String label = entry.get(j).toString();
                        if (label!=null) label = label.toLowerCase();
                        labels.add(label);
                    }



                    if (type.equals("nodes")){
                        if (labels.contains(META_NODE_LABEL)) continue;

                        NodeMetadata nodeMetadata = null;
                        for (String label : labels){
                            nodeMetadata = nodes.get(label);
                            if (nodeMetadata!=null) break;
                        }


                        if (nodeMetadata==null){
                            nodeMetadata = new NodeMetadata();
                            nodeMetadata.count.incrementAndGet();
                            nodes.put(labels.iterator().next(), nodeMetadata);
                        }
                        else{
                            if (action.equals("create")){
                                nodeMetadata.count.incrementAndGet();
                            }
                            else{
                                Long n = nodeMetadata.count.decrementAndGet();
                                if (n==0){
                                    //TODO: Remove node
                                }
                            }
                        }


                        lastUpdate.set(System.currentTimeMillis());

                    }
                    else if (type.equals("relationships")){
                        //AtomicLong relations = (AtomicLong) node.get("relations").toObject();
                    }

                }
            }
            catch(Exception e){
                e.printStackTrace();
            }


            nodes.notifyAll();

        }
    }


  //**************************************************************************
  //** updateNodes
  //**************************************************************************
    private void updateNodes() throws Exception {

        HashMap<String, NodeMetadata> nodes = new HashMap<>();

        try (Transaction tx = db.beginTx()) {


          //Get nodes and properties from the database
            Result rs = tx.execute(propertiesQuery);
            while (rs.hasNext()){
                Map<String, Object> r = rs.next();
                Object node = r.get("node");
                Object props = r.get("properties");
                if (node==null) continue;

                String nodeName = node.toString().toLowerCase();
                if (nodeName.equals(META_NODE_LABEL)) continue;

                NodeMetadata nodeMetadata = new NodeMetadata();


                if (props!=null){
                    for (Object p : (List) props){
                        String propertyName = p.toString();
                        nodeMetadata.properties.put(propertyName, new PropertyMetadata());
                    }
                }

                nodes.put(nodeName, nodeMetadata);
            }


          //Update nodeMetadata.properties with attributes stored in the bluewave_metadata node
            String metadata = null;
            rs = tx.execute("match (n:bluewave_metadata) return n.nodes");
            if (rs.hasNext()) metadata = rs.next().get("n.nodes").toString();
            if (metadata!=null){
                JSONObject json = new JSONObject(metadata);
                Iterator<String> it = json.keySet().iterator();
                while (it.hasNext()){
                    String nodeName = it.next();
                    JSONObject nodeMetadata = json.get(nodeName).toJSONObject();
                    JSONObject properties = nodeMetadata.get("properties").toJSONObject();
                    Iterator<String> i2 = properties.keySet().iterator();
                    while (i2.hasNext()){
                        String propertyName = i2.next();
                        JSONObject propertyMetadata = properties.get(propertyName).toJSONObject();
                        String type = propertyMetadata.get("type").toString();
                        PropertyMetadata pm = nodes.get(nodeName).properties.get(propertyName);
                        if (type!=null) pm.type = type;
                    }
                }
            }


          //Update counts
            rs = tx.execute(nodesQuery);
            while (rs.hasNext()){
                Map<String, Object> r = rs.next();
                JSONArray labels = new JSONArray(r.get("labels").toString());
                String nodeName = labels.isEmpty()? "" : labels.get(0).toString();
                if (nodeName.equals(META_NODE_LABEL)) continue;

                Long count = Long.parseLong(r.get("count").toString());
                Long relations = Long.parseLong(r.get("relations").toString());

                NodeMetadata nodeMetadata = nodes.get(nodeName.toLowerCase());
                if (nodeMetadata!=null){
                    nodeMetadata.count.set(count);
                    nodeMetadata.relations.set(relations);
                }
            }
        }
        catch (Exception e) {
            throw e;
        }



        synchronized(this.nodes){
            this.nodes.clear();
            Iterator<String> it = nodes.keySet().iterator();
            while (it.hasNext()){
                String nodeName = it.next();
                NodeMetadata nodeMetadata = nodes.get(nodeName);
                this.nodes.put(nodeName, nodeMetadata);
            }

            this.nodes.notify();
        }
    }


  //**************************************************************************
  //** saveNodes
  //**************************************************************************
    private void saveNodes() throws Exception {

        JSONObject json = new JSONObject();
        synchronized(nodes){
            Iterator<String> it = nodes.keySet().iterator();
            while (it.hasNext()){
                String nodeName = it.next();
                NodeMetadata nodeMetadata = nodes.get(nodeName);
                json.set(nodeName, nodeMetadata.toJSON());
            }
        }
        //System.out.println(json.toString(4));


        Label label = Label.label(META_NODE_LABEL);
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodesIterator = tx.findNodes(label);
            Node metadataNode;
            if (nodesIterator.hasNext()) {
                metadataNode = nodesIterator.next();
            }
            else{
                metadataNode = tx.createNode(label);
            }

            metadataNode.setProperty("nodes", json.toString());
            tx.commit();
        }
        catch (Exception e) {
            throw e;
        }
    }


  //**************************************************************************
  //** NodeMetadata
  //**************************************************************************
    private class NodeMetadata {
        private ConcurrentHashMap<String, PropertyMetadata> properties;
        private AtomicLong count;
        private AtomicLong relations;
        public NodeMetadata(){
            properties = new ConcurrentHashMap<>();
            count = new AtomicLong(0L);
            relations = new AtomicLong(0L);
        }
        public JSONObject toJSON(){
            JSONObject json = new JSONObject();
            json.set("count", count.get());
            json.set("relations", relations.get());

            JSONObject props = new JSONObject();
            Iterator<String> it = properties.keySet().iterator();
            while (it.hasNext()){
                String propertyName = it.next();
                PropertyMetadata propertyMetadata = properties.get(propertyName);
                props.set(propertyName, propertyMetadata.toJSON());
            }
            json.set("properties", props);

            return json;
        }
    }


  //**************************************************************************
  //** PropertyMetadata
  //**************************************************************************
    private class PropertyMetadata {
        private String type = "Object";
        private boolean isIndexed = false;
        public JSONObject toJSON(){
            JSONObject json = new JSONObject();
            json.set("type", type);
            json.set("isIndexed", isIndexed);
            return json;
        }
    }
}