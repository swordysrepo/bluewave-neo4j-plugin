package bluewave.neo4j.plugins;

import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.Log;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Neo4JTransactionEventListener implements TransactionEventListener<Object> {
    GraphDatabaseService db;
    LogService log;
    Path logFilePath;
    Neo4j neo4j;

    long transactionId;
    long commitTime;
    String type;
    TransactionType transactionType;
    String tableName;
    String insertQuery;
    String nodeName;
    String labelName;
    String relName;
    String propName;
    String propValue;


    public Neo4JTransactionEventListener(final GraphDatabaseService graphDatabaseService, final LogService logsvc)
    {
        this.db = graphDatabaseService;
        this.log = logsvc;
        this.tableName = "transaction";
        this.insertQuery = "INSERT INTO ";
        // can be changed in the future
        logFilePath = Paths.get("C:/", "testing", "test.txt");


    }
    public Object beforeCommit(final TransactionData data, final Transaction transaction, final GraphDatabaseService databaseService) throws Exception
    {


        String url = "jdbc:h2:mem";
        String createQuery = "CREATE TABLE transaction ( " +
                "id INT NOT NULL, " +
                "type VARCHAR(50), " +
                "transactionType VARCHAR(50), " +
                "timestamp LONG, " +
                "node VARCHAR(50), " +
                "relationship VARCHAR(50), " +
                "label VARCHAR(50), " +
                "propertyname VARCHAR(50), " +
                "propertyvalue VARCHAR(50) "
                ;

        try (Connection testCon = DriverManager.getConnection(url)) {
            Statement st = testCon.createStatement();
            st.executeQuery("");

        }


        //ENUM for type?
        //think about how to work with json data type for h2
        //think about how to deal with transactions that affect multiple nodes





//
//        String query = "CREATE (friend:Person {name: 'tester'}) RETURN friend";
//        session.run(query);
//        session.close();

//        String insertQuery = "INSERT INTO " + tableName + " VALUES(";



        if (data.createdNodes() != null) {


            Files.writeString(logFilePath, "\n///Created Nodes: " + data.createdNodes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            nodeName = data.createdNodes().toString();
            type = "NODE";
            transactionType = TransactionType.CREATION;



        }
        if (data.deletedNodes() != null) {
            Files.writeString(logFilePath, "\n///Deleted Nodes: " + data.deletedNodes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            nodeName = data.deletedNodes().toString();
            type = "NODE";
            transactionType = TransactionType.DELETION;
        }
        if (data.createdRelationships() != null) {
            Files.writeString(logFilePath, "\n///Created Relationships: " + data.createdRelationships(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            relName = data.createdRelationships().toString();

            type = "RELATIONSHIP";
            transactionType = TransactionType.CREATION;
        }
        if (data.deletedRelationships() != null) {
            Files.writeString(logFilePath, "\n///Deleted Relationships: " + data.deletedRelationships(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            relName = data.deletedRelationships().toString();

            type = "RELATIONSHIP";
            transactionType = TransactionType.DELETION;

        }
        if (data.assignedLabels() != null) {
            Files.writeString(logFilePath, "\n///Assigned Labels:  " + data.assignedLabels(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            labelName = data.assignedLabels().toString();

            type = "LABEL";
            transactionType = TransactionType.ASSIGNMENT;
        }
        if (data.removedLabels() != null) {
            Files.writeString(logFilePath, "\n///Removed Labels: " + data.removedLabels(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            labelName = data.removedLabels().toString();
            type = "LABEL";
            transactionType = TransactionType.REMOVAL;
        }
        if (data.assignedNodeProperties() != null) {

            List<String> keys = new ArrayList<String>();
            List<String> values = new ArrayList<String>();


            for (PropertyEntry<Node> assignedNodeProperty : data.assignedNodeProperties()) {
                keys.add(assignedNodeProperty.key());
                values.add(assignedNodeProperty.value().toString());

            }

            transactionType = TransactionType.ASSIGNMENT;
            type = "NODE";
            propName = keys.stream().collect(Collectors.joining(","));
            propValue = values.stream().collect(Collectors.joining(","));

        }
        if (data.removedNodeProperties() != null) {

            List<String> keys = new ArrayList<String>();
            List<String> values = new ArrayList<String>();


            for (PropertyEntry<Node> removedNodeProperty : data.removedNodeProperties()) {
                keys.add(removedNodeProperty.key());
                values.add(removedNodeProperty.value().toString());

            }

            transactionType = TransactionType.REMOVAL;
            type = "NODE";
            propName = keys.stream().collect(Collectors.joining(","));
            propValue = values.stream().collect(Collectors.joining(","));

        }
        if (data.assignedRelationshipProperties() != null) {

            List<String> keys = new ArrayList<String>();
            List<String> values = new ArrayList<String>();

            for (PropertyEntry<Relationship> assignedRelationshipProperty : data.assignedRelationshipProperties()) {
                keys.add(assignedRelationshipProperty.key());
                values.add(assignedRelationshipProperty.value().toString());
            }

            transactionType = TransactionType.ASSIGNMENT;
            type = "RELATIONSHIP";
            propName = keys.stream().collect(Collectors.joining(","));
            propValue = values.stream().collect(Collectors.joining(","));

        }
        if (data.removedRelationshipProperties() != null) {

            List<String> keys = new ArrayList<String>();
            List<String> values = new ArrayList<String>();

            for (PropertyEntry<Relationship> removedRelationshipProperty : data.removedRelationshipProperties()) {
                keys.add(removedRelationshipProperty.key());
                values.add(removedRelationshipProperty.value().toString());
            }

            transactionType = TransactionType.REMOVAL;
            type = "RELATIONSHIP";
            propName = keys.stream().collect(Collectors.joining(","));
            propValue = values.stream().collect(Collectors.joining(","));


        }




        return null;

    }
    public void afterCommit(final TransactionData data, final Object state, final GraphDatabaseService databaseService)

    {
        try {
            Files.writeString(logFilePath, "\n///Commit time: " + data.getCommitTime(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            Files.writeString(logFilePath, "\n///Transaction Id: " + data.getTransactionId(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void afterRollback(final TransactionData data, final Object state, final GraphDatabaseService databaseService)
    {
    }
}
