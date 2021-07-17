package bluewave.neo4j.plugins;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.logging.internal.LogService;

import java.util.*;
import javaxt.json.JSONObject;
import static javaxt.utils.Console.console;


public class Neo4JTransactionEventListener implements TransactionEventListener<Object> {

    private Logger logger;
    private javaxt.io.File configFile;


  //**************************************************************************
  //** Constructor
  //**************************************************************************
    public Neo4JTransactionEventListener(final GraphDatabaseService graphDatabaseService, final LogService logsvc){

      //Find the config file
        javaxt.io.Jar jar = new javaxt.io.Jar(this);
        java.io.File pluginDir = jar.getFile().getParentFile();
        configFile = new javaxt.io.File(pluginDir, "config.json");


      //Parse the config file
        JSONObject config = null;
        try{
            config = new JSONObject(configFile.getText());
        }
        catch(Exception e){
            console.log(e.getMessage());
            return;
        }


      //Instantiate logger
        logger = new Logger();


      //Set path to the log file directory
        try{
            javaxt.io.Directory logDir = new javaxt.io.Directory(config.get("logger").get("path").toString());
            logger.setDirectory(logDir);
        }
        catch(Exception e){
        }


      //Initialize database
        try{
            JSONObject json = config.get("database").toJSONObject();
            String path = json.get("path").toString().replace("\\", "/");
            javaxt.io.Directory dbDir = new javaxt.io.Directory(path);
            dbDir.create();
            path = new java.io.File(dbDir.toString()+"database").getCanonicalPath();

            javaxt.sql.Database database = new javaxt.sql.Database();
            database.setDriver("H2");
            database.setHost(path);
            database.setConnectionPoolSize(25);
            logger.setDatabase(database);
        }
        catch(Exception e){

        }


      //Get webserver config
        try{
            logger.setWebServer(config.get("webserver").toJSONObject());
        }
        catch(Exception e){
        }


        console.log("starting logger...");
        new Thread(logger).start();

    }


  //**************************************************************************
  //** beforeCommit
  //**************************************************************************
    public Object beforeCommit(final TransactionData data, final Transaction transaction,
        final GraphDatabaseService databaseService) throws Exception {
        if (logger==null) return null;
        String user = data.username();

        Iterable<Node> createdNodes = data.createdNodes();
        if (createdNodes!=null) {
            Iterator<Node> it = createdNodes.iterator();
            if (it.hasNext()) logger.log("create","nodes",getNodeInfo(it),user);
        }


        Iterable<Node> deletedNodes = data.deletedNodes();
        if (data.deletedNodes()!=null) {
            Iterator<Node> it = deletedNodes.iterator();
            if (it.hasNext()) logger.log("delete","nodes",getNodeInfo(it),user);
        }


        Iterable<Relationship> createdRelationships = data.createdRelationships();
        if (createdRelationships!=null) {
            Iterator<Relationship> it = createdRelationships.iterator();
            if (it.hasNext()) logger.log("create","relationships",getRelationshipInfo(it),user);
        }


        Iterable<Relationship> deletedRelationships = data.deletedRelationships();
        if (data.deletedRelationships()!=null) {
            Iterator<Relationship> it = deletedRelationships.iterator();
            if (it.hasNext()) logger.log("delete","relationships",getRelationshipInfo(it),user);
        }


        Iterable<LabelEntry> assignedLabels = data.assignedLabels();
        if (assignedLabels!=null) {
            Iterator<LabelEntry> it = assignedLabels.iterator();
            if (it.hasNext()) logger.log("create","labels","",user);
        }


        Iterable<LabelEntry> removedLabels = data.assignedLabels();
        if (removedLabels!=null) {
            Iterator<LabelEntry> it = removedLabels.iterator();
            if (it.hasNext()) logger.log("delete","labels","",user);
        }


        Iterable<PropertyEntry<Node>> assignedNodeProperties = data.assignedNodeProperties();
        if (assignedNodeProperties!=null) {
            Iterator<PropertyEntry<Node>> it = assignedNodeProperties.iterator();
            if (it.hasNext()) logger.log("create","properties","",user);
        }


        Iterable<PropertyEntry<Node>> removedNodeProperties = data.removedNodeProperties();
        if (removedNodeProperties!=null) {
            Iterator<PropertyEntry<Node>> it = removedNodeProperties.iterator();
            if (it.hasNext()) logger.log("delete","properties","",user);
        }


        Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties = data.assignedRelationshipProperties();
        if (assignedRelationshipProperties!=null) {
            Iterator<PropertyEntry<Relationship>> it = assignedRelationshipProperties.iterator();
            if (it.hasNext()) logger.log("create","relationship_property","",user);
        }


        Iterable<PropertyEntry<Relationship>> removedRelationshipProperties = data.removedRelationshipProperties();
        if (removedRelationshipProperties!=null) {
            Iterator<PropertyEntry<Relationship>> it = removedRelationshipProperties.iterator();
            if (it.hasNext()) logger.log("delete","relationship_property","",user);
        }

        return null;
    }


  //**************************************************************************
  //** afterCommit
  //**************************************************************************
    public void afterCommit(final TransactionData data, final Object state,
        final GraphDatabaseService databaseService){
    }


  //**************************************************************************
  //** afterRollback
  //**************************************************************************
    public void afterRollback(final TransactionData data, final Object state,
        final GraphDatabaseService databaseService){
    }


  //**************************************************************************
  //** getNodeInfo
  //**************************************************************************
    private String getNodeInfo(Iterator<Node> it){
        StringBuilder str = new StringBuilder("[");

        try{
            while(it.hasNext()){
                str.append("[");
                Node node = it.next();
                Long nodeID = node.getId();
                str.append(nodeID);

                Iterable<Label> labels = node.getLabels();
                if (labels!=null){
                    Iterator<Label> i2 = labels.iterator();
                    while (i2.hasNext()){
                        String label = i2.next().name();
                        if (label!=null){
                            str.append(",");
                            str.append(label);
                        }
                    }
                }
                str.append("]");
                if (it.hasNext()) str.append(",");
            }
        }
        catch(Exception e){
            console.log(e.getMessage());
        }


        str.append("]");
        return str.toString();
    }


  //**************************************************************************
  //** getRelationshipInfo
  //**************************************************************************
    private String getRelationshipInfo(Iterator<Relationship> it){
        StringBuilder str = new StringBuilder();
        return str.toString();
    }


  //**************************************************************************
  //** getRelationshipInfo
  //**************************************************************************
    private String getPropertyInfo(Iterator<Relationship> it){
        StringBuilder str = new StringBuilder();
        return str.toString();
    }
}