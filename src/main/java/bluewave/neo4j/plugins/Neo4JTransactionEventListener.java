package bluewave.neo4j.plugins;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
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

public class Neo4JTransactionEventListener implements TransactionEventListener<Object> {
    GraphDatabaseService db;
    LogService log;
    Path logFilePath;

    public Neo4JTransactionEventListener(final GraphDatabaseService graphDatabaseService, final LogService logsvc)
    {
        this.db = graphDatabaseService;
        this.log = logsvc;
        // can be changed in the future
        logFilePath = Paths.get("C:/", "testing", "test.txt");


    }
    public Object beforeCommit(final TransactionData data, final Transaction transaction, final GraphDatabaseService databaseService) throws Exception
    {



        if (data.createdNodes() != null) {
            Files.writeString(logFilePath, "\n///Created Nodes: " + data.createdNodes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (data.deletedNodes() != null) {
            Files.writeString(logFilePath, "\n///Deleted Nodes: " + data.deletedNodes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (data.createdRelationships() != null) {
            Files.writeString(logFilePath, "\n///Created Relationships: " + data.createdRelationships(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (data.deletedRelationships() != null) {
            Files.writeString(logFilePath, "\n///Deleted Relationships: " + data.deletedRelationships(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (data.assignedLabels() != null) {
            Files.writeString(logFilePath, "\n///Assigned Labels:  " + data.assignedLabels(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        if (data.removedLabels() != null) {
            Files.writeString(logFilePath, "\n///Removed Labels: " + data.removedLabels(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
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
