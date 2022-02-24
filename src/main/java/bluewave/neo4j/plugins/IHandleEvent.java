package bluewave.neo4j.plugins;

import org.neo4j.graphdb.event.TransactionData;

public interface IHandleEvent {
    void handleEvent(final TransactionData data) throws Exception;
}
