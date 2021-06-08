package bluewave.neo4j.plugins;

import com.google.gson.Gson;
import org.h2.jdbc.JdbcClob;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.logging.internal.LogService;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.*;
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
    TransactionType transactionType = TransactionType.NONE;
    String tableName;
    String insertQuery;
    String nodeName = "";
    String labelName = "";
    String relName = "";
    String propName = "";
    String propValue = "";
    String url = "jdbc:h2:~/h2test";
    String user = "sa";
    String passwd = "password";
    int index =0;
    String dataString = "";



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


        String createQuery = "CREATE TABLE transaction IF NOT EXISTS( " +
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




        //ENUM for type?
        //think about how to work with json data type for h2
        //think about how to deal with transactions that affect multiple nodes





//
//        String query = "CREATE (friend:Person {name: 'tester'}) RETURN friend";
//        session.run(query);
//        session.close();

//        String insertQuery = "INSERT INTO " + tableName + " VALUES(";



        if (data.createdNodes() != null) {
            type = "NODE";
            transactionType = TransactionType.CREATION;

            dataString += "Created Nodes | " + data.createdNodes() + " | ";



        }
        if (data.deletedNodes() != null) {
            type = "NODE";
            transactionType = TransactionType.DELETION;

            dataString += "Deleted Nodes | " + data.deletedNodes() + " | ";
        }
        if (data.createdRelationships() != null) {

            type = "RELATIONSHIP";
            transactionType = TransactionType.CREATION;

            dataString += "Created Relationships | " + data.createdRelationships() + " | ";
        }
        if (data.deletedRelationships() != null) {

            type = "RELATIONSHIP";
            transactionType = TransactionType.DELETION;

            dataString += "Deleted Relationships | " + data.deletedRelationships() + " | ";

        }
        if (data.assignedLabels() != null) {

            type = "LABEL";
            transactionType = TransactionType.LABEL_ASSIGNMENT;

            dataString += "Assigned Labels | " + data.assignedLabels() + " | ";
        }
        if (data.removedLabels() != null) {
            type = "LABEL";
            transactionType = TransactionType.LABEL_REMOVAL;

            dataString += "Removed Labels | " + data.removedLabels() + " | ";
        }
        if (data.assignedNodeProperties() != null) {

            transactionType = TransactionType.PROPERTY_ASSIGNMENT;
            type = "NODE";

            dataString += "Assigned Node Properties | " + data.assignedNodeProperties().toString() + " | ";

        }
        if (data.removedNodeProperties() != null) {

            transactionType = TransactionType.PROPERTY_REMOVAL;
            type = "NODE";

            dataString += "Removed Node Properties | " + data.removedNodeProperties().toString() + " | ";

        }
        if (data.assignedRelationshipProperties() != null) {

            transactionType = TransactionType.PROPERTY_ASSIGNMENT;
            type = "RELATIONSHIP";

            dataString += "Assigned Relationship Properties | " + data.assignedRelationshipProperties().toString() + " | ";

        }
        if (data.removedRelationshipProperties() != null) {

            transactionType = TransactionType.PROPERTY_REMOVAL;
            type = "RELATIONSHIP";

            dataString += "Removed Relationship Properties | " + data.removedRelationshipProperties().toString() + " | ";


        }





        return null;

    }
    public void afterCommit(final TransactionData data, final Object state, final GraphDatabaseService databaseService)

    {
        try {
            commitTime = data.getCommitTime();
            dataString += "Commit Time | " + data.getCommitTime() + " | ";

            transactionId = data.getTransactionId();

            dataString += "Transaction ID | " + data.getTransactionId() + " | ";


            // Need to allow for multiple connections simultaneously
            try (Connection testCon = DriverManager.getConnection(url, user, passwd)) {
                Statement st = testCon.createStatement();
                PreparedStatement preparedStatement = testCon.prepareStatement("INSERT INTO TRANSACTION VALUES (?, ?)");
                preparedStatement.setInt(1, index++);

                Clob clob = testCon.createClob();


                clob.setString(1, dataString);
                preparedStatement.setClob(2, clob);
                preparedStatement.executeUpdate();
                preparedStatement.close();
//                ResultSet query = st.executeQuery("INSERT INTO transaction VALUES(?, ?)");



            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void afterRollback(final TransactionData data, final Object state, final GraphDatabaseService databaseService)
    {
    }
}
