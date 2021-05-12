package bluewave.neo4j.plugins;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.logging.internal.LogService;

public class Neo4JTransactionEventListener implements TransactionEventListener<Object> {
    GraphDatabaseService db;
    LogService log;
    public Neo4JTransactionEventListener(final GraphDatabaseService graphDatabaseService, final LogService logsvc)
    {
        this.db = graphDatabaseService;
        this.log = logsvc;
    }
    public Object beforeCommit(final TransactionData data, final Transaction transaction, final GraphDatabaseService databaseService) throws Exception
    {
// TODO Auto-generated method stub
        log.getUserLog(getClass()).debug("PATRICK" );
        log.getUserLog(getClass()).info("ITSTESTING");



        //data. all the info


        return null;
    }
    public void afterCommit(final TransactionData data, final Object state, final GraphDatabaseService databaseService)
    {
// TODO Auto-generated method stub
    }
    public void afterRollback(final TransactionData data, final Object state, final GraphDatabaseService databaseService)
    {
// TODO Auto-generated method stub
    }
}
