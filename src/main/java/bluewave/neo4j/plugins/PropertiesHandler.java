package bluewave.neo4j.plugins;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;

import javaxt.json.JSONArray;
import javaxt.json.JSONObject;

import static javaxt.utils.Console.console;

public class PropertiesHandler implements IHandleEvent {

    ConcurrentHashMap<String, ConcurrentHashMap<String, String>> metaPropertiesMap;
    GraphDatabaseService db;

    public static final String KEY_PROPERTIES = "nodes";

    public PropertiesHandler(GraphDatabaseService db) {
        this.db = db;
        metaPropertiesMap = new ConcurrentHashMap<>();
    }

    public void init() {
        refresh();
    }

    @Override
    public void handleEvent(TransactionData data) throws Exception {
        // TODO Auto-generated method stub
    }


    //***********************************************
    //** Adds a new node, replaces if exists
    //***********************************************
    public void add(JSONObject json) {
        if(json == null) return;

        ConcurrentHashMap<String, String> tempProp = new ConcurrentHashMap<>();
        JSONArray properties = json.get("properties").toJSONArray();
        Iterator itProps = properties.iterator();
        while(itProps.hasNext()) {
            tempProp.put(itProps.next().toString(), "Object");
        }
        metaPropertiesMap.put(json.get("node").toString(), tempProp);
    }


    //***********************************************
    //** Adds an array of new nodes
    //***********************************************
    public void addAll(JSONArray arr) {
        if(arr.isEmpty()) return;
        Iterator it = arr.iterator();
        JSONObject json = null;
        while(it.hasNext()) {
            json = (JSONObject) it.next();
            ConcurrentHashMap<String, String> tempProp = new ConcurrentHashMap<>();
            JSONArray properties =  json.get("properties").toJSONArray();
            Iterator itProps = properties.iterator();
            while(itProps.hasNext()) {
                tempProp.put(itProps.next().toString(), "Object");
            }
            metaPropertiesMap.put(json.get("node").toString(), tempProp);
        }
    }

    //***********************************************
    //** Appends properties to a node if exists, otherwise add as new
    //***********************************************
    // public void append(JSONObject json) {
    //     if(json == null) return;

    //     ConcurrentHashMap<String, String>existingNodePropertiesValue = metaPropertiesMap.get(json.get("node").toString());
    //     if(existingNodePropertiesValue != null) {
    //         for(String prop : (List<String>)json.get("properties")) {
    //             existingNodePropertiesValue.put(prop, "Object");
    //         }
    //     } else {
    //         existingNodePropertiesValue = new ConcurrentHashMap<>();
    //         List<String> properties = (List<String>) json.get("properties");
    //         for(String prop: properties) {
    //             existingNodePropertiesValue.put(prop, "Object");
    //         }
    //         metaPropertiesMap.put(json.get("node").toString(), existingNodePropertiesValue);
    //     }
    // }

    //***********************************************
    //** Updates the db node from the nodes in our map
    //***********************************************
    public void sync(boolean refresh) {

      //Create json format from map
        JSONObject json = new JSONObject();
        for(String nodeKey : metaPropertiesMap.keySet()) {
            ConcurrentHashMap<String, String>nodePropertiesValue = metaPropertiesMap.get(nodeKey);
            JSONArray propertyList = new JSONArray();
            JSONObject propSet = null;
            for(String propKey: nodePropertiesValue.keySet()) {
                propSet = new JSONObject();
                propSet.set(propKey, nodePropertiesValue.get(propKey));
                propertyList.add(propSet);
            }
            json.set(nodeKey, propertyList);
        }

        if(Metadata.isSafeToSync() || refresh) save(json);

    }

    //***********************************************
    //** Saves the json to the db node
    //***********************************************
    public void save(JSONObject json) {
      //Save to db
        Label label = Label.label(Metadata.META_NODE_LABEL);
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodesIterator = tx.findNodes(label);
            Node metadataNode = null;
            if (nodesIterator.hasNext()) {
                metadataNode = nodesIterator.next();
            } else {
                metadataNode = tx.createNode(label);
            }

            metadataNode.setProperty(KEY_PROPERTIES, json.toString());
            tx.commit();
        } catch (Exception e) {
            e(e);
        }
    }


    //***********************************************
    //** Runs the query and saves the output in our map
    //***********************************************
    public void refresh() {
        String propertiesQuery =
        "MATCH(n)\n" +
        "WITH LABELS(n) AS labels , KEYS(n) AS keys\n" +
        "UNWIND labels AS label\n" +
        "UNWIND keys AS key\n" +
        "RETURN DISTINCT label as node, COLLECT(DISTINCT key) AS properties\n" +
        "ORDER BY label";

        try (Transaction tx = db.beginTx()) {

            //Execute query
              Result rs = tx.execute(propertiesQuery);
              metaPropertiesMap = new ConcurrentHashMap<>();
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
                  add(json);
              }

            //Save to db
              sync(true);

        }
        catch (Exception e) {
            e(e);
            e.printStackTrace();
        }finally {
            p("PropertiesHandler.refresh() finished");
        }
    }

    private void p(Object message) {
        console.log(message.toString());
    }

    private void e(Object message) {
        console.log("*** ------- ERROR ------- *** " + message.toString());
    }


}
