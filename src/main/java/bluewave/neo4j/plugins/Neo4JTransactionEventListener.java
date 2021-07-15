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
import java.sql.*;

import static javaxt.utils.Console.console;
import javaxt.json.*;


public class Neo4JTransactionEventListener implements TransactionEventListener<Object> {

    private Logger logger;
    private java.io.File pluginDir;
    private final static long  jvm_diff;
    static {
        jvm_diff = System.currentTimeMillis()*1000_000-System.nanoTime();
    }


  //**************************************************************************
  //** Constructor
  //**************************************************************************
    public Neo4JTransactionEventListener(final GraphDatabaseService graphDatabaseService, final LogService logsvc){


        javaxt.io.Jar jar = new javaxt.io.Jar(this);
        pluginDir = jar.getFile().getParentFile();
        //console.log(pluginDir);


        javaxt.io.File configFile = new javaxt.io.File(pluginDir, "config.json");
        try{
            JSONObject config = new JSONObject(configFile.getText());
            javaxt.io.Directory logDir = new javaxt.io.Directory(config.get("logger").get("path").toString());
            logDir.create();
            if (logDir.exists()){
                logger = new Logger(logDir.toFile());
                console.log("starting logger: " + logDir);
                new Thread(logger).start();
            }


//        String createQuery = "CREATE TABLE IF NOT EXISTS transaction( " +
//                "id bigint auto_increment, " +
//                "data clob, " +
//                "commitTime LONG, " +
//                "transactionID LONG);";
//
//        try (Connection testCon = DriverManager.getConnection(url, user, passwd)) {
//            Statement st = testCon.createStatement();
//            st.executeUpdate(createQuery);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }


        }
        catch(Exception e){
            console.log(e.getMessage());
        }
    }


  //**************************************************************************
  //** beforeCommit
  //**************************************************************************
    public Object beforeCommit(final TransactionData data, final Transaction transaction,
        final GraphDatabaseService databaseService) throws Exception {


        Iterable<Node> createdNodes = data.createdNodes();
        if (createdNodes!=null) {
            Iterator<Node> it = createdNodes.iterator();
            if (it.hasNext()) notify("create","nodes",getNodeInfo(it));
        }


        Iterable<Node> deletedNodes = data.deletedNodes();
        if (data.deletedNodes()!=null) {
            Iterator<Node> it = deletedNodes.iterator();
            if (it.hasNext()) notify("delete","nodes",getNodeInfo(it));
        }


        Iterable<Relationship> createdRelationships = data.createdRelationships();
        if (createdRelationships!=null) {
            Iterator<Relationship> it = createdRelationships.iterator();
            if (it.hasNext()) notify("create","relationships",getRelationshipInfo(it));
        }


        Iterable<Relationship> deletedRelationships = data.deletedRelationships();
        if (data.deletedRelationships()!=null) {
            Iterator<Relationship> it = deletedRelationships.iterator();
            if (it.hasNext()) notify("delete","relationships",getRelationshipInfo(it));
        }


        Iterable<LabelEntry> assignedLabels = data.assignedLabels();
        if (assignedLabels!=null) {
            Iterator<LabelEntry> it = assignedLabels.iterator();
            if (it.hasNext()) notify("create","labels","");
        }


        Iterable<LabelEntry> removedLabels = data.assignedLabels();
        if (removedLabels!=null) {
            Iterator<LabelEntry> it = removedLabels.iterator();
            if (it.hasNext()) notify("delete","labels","");
        }


        Iterable<PropertyEntry<Node>> assignedNodeProperties = data.assignedNodeProperties();
        if (assignedNodeProperties!=null) {
            Iterator<PropertyEntry<Node>> it = assignedNodeProperties.iterator();
            if (it.hasNext()) notify("create","properties","");
        }


        Iterable<PropertyEntry<Node>> removedNodeProperties = data.removedNodeProperties();
        if (removedNodeProperties!=null) {
            Iterator<PropertyEntry<Node>> it = removedNodeProperties.iterator();
            if (it.hasNext()) notify("delete","properties","");
        }


        Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties = data.assignedRelationshipProperties();
        if (assignedRelationshipProperties!=null) {
            Iterator<PropertyEntry<Relationship>> it = assignedRelationshipProperties.iterator();
            if (it.hasNext()) notify("create","relationship_property","");
        }


        Iterable<PropertyEntry<Relationship>> removedRelationshipProperties = data.removedRelationshipProperties();
        if (removedRelationshipProperties!=null) {
            Iterator<PropertyEntry<Relationship>> it = removedRelationshipProperties.iterator();
            if (it.hasNext()) notify("delete","relationship_property","");
        }

        return null;
    }


  //**************************************************************************
  //** afterCommit
  //**************************************************************************
    public void afterCommit(final TransactionData data, final Object state,
        final GraphDatabaseService databaseService){


//        String createQuery = "CREATE TABLE IF NOT EXISTS transaction( " +
//                "id NOT NULL PRIMARY KEY, " +
//                "data clob, " +
//                "commitTime LONG, " +
//                "transactionID LONG);";
//
//
//        try {
//            commitTime = data.getCommitTime();
//            dataString += "Commit Time | " + data.getCommitTime() + " | ";
//
//            transactionId = data.getTransactionId();
//
//            dataString += "Transaction ID | " + data.getTransactionId() + " | ";
//
//
//            // Need to allow for multiple connections simultaneously
//            try (Connection testCon = DriverManager.getConnection(url, user, passwd)) {
//
//                PreparedStatement preparedStatement = testCon.prepareStatement("INSERT INTO TRANSACTION ( data, commitTime, transactionId) VALUES (?, ?, ?)");
//
//                Clob clob = testCon.createClob();
//
//                clob.setString(1, dataString);
//                preparedStatement.setClob(1, clob);
//                preparedStatement.setLong(2, data.getCommitTime());
//                preparedStatement.setLong(3, data.getTransactionId());
//                preparedStatement.executeUpdate();
//                preparedStatement.close();
//
//            }
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }


  //**************************************************************************
  //** afterRollback
  //**************************************************************************
    public void afterRollback(final TransactionData data, final Object state,
        final GraphDatabaseService databaseService){
    }


  //**************************************************************************
  //** notify
  //**************************************************************************
    private void notify(String action, String type, String data){

        long t = getCurrentTime();
        String msg = t + "," + action + "," + type + "," + data;
        console.log(msg);
        if (logger!=null) logger.log(msg);

//        javaxt.http.Request request = new javaxt.http.Request("http://localhost:9080/graph/update");
//        request.setCredentials("neo4j", "password");
//        request.write(msg);
//        if (logger!=null){
//            logger.log(request.toString());
//            logger.log(request.getResponse().toString());
//        }

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


  //**************************************************************************
  //** getCurrentTime
  //**************************************************************************
  /** Returns current time in nanoseconds
   */
    private static long getCurrentTime(){
        return System.nanoTime()+jvm_diff;
    }
}