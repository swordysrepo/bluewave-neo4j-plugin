package bluewave.neo4j.plugins;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;

import javaxt.json.*;
import javaxt.utils.Date;
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

    private static final int BEFORE_COMMIT = 1;
    private static final int AFTER_COMMIT = 2;

    private String META_NODE_LABEL = "bluewave_metadata";
    public static final String KEY_COUNTS = "counts";

    private GraphDatabaseService db;

    public static final int INDEX_LABELS = 0;
    private static final int INDEX_COUNT = 1;
    public static final int INDEX_RELATIONS = 2;
    private static final int INDEX_TRANSACTION_ID = 3;
    private static final int INDEX_NODE_ID = 4;

    private java.util.Timer timer;

    private long interval = 24 * 60 * 60 * 1000; // 24 hours
    private long delay = 24 * 60 * 60 * 1000;
    private boolean isAvailable = false;


  //**************************************************************************
  //** Constructor
  //**************************************************************************
    public Metadata(GraphDatabaseService databaseService) {
        cache = new ConcurrentHashMap<>();
        db = databaseService;
        timer = new java.util.Timer();
    }


  //**************************************************************************
  //** init
  //**************************************************************************
  /** Used to initialize the Metadata class and populate the metadata node
   */
    public void init() {
        while (!db.isAvailable(500)) {
            e("db.isAvailable == false");
        }
        isAvailable = false;



        long startTime = System.currentTimeMillis();
        JSONObject nodes = executeNodesAndCountsQuery();
        try{
            getNodes();
            getProperties();
        }
        catch(Exception e){
        }
        long ellapsedTime = System.currentTimeMillis()-startTime;
        console.log("Fetched nodes and properties in " + ellapsedTime + "ms");
        //TODO: unsure the timer task interval is slower than the ellapsedTime


        Label label = Label.label(META_NODE_LABEL);
        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodesIterator = tx.findNodes(label);
            Node metadataNode = null;
            if (nodesIterator.hasNext()) {
                metadataNode = nodesIterator.next();
            } else {
                metadataNode = tx.createNode(label);
            }
            metadataNode.setProperty(KEY_COUNTS, nodes.toString());
            tx.commit();
            isAvailable = true;
        } catch (Exception e) {
            e("init: " + e);
        }
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
        cancelTimer();

        synchronized (pool) {
            pool.clear();
            pool.add(0, null);
            pool.notifyAll();
        }
    }


  //**************************************************************************
  //** startTimer
  //**************************************************************************
    private void startTimer() {
        if (timer == null) {
            timer = new java.util.Timer();
        }
        timer.scheduleAtFixedRate(new java.util.TimerTask() {
            public void run() {
                p("Timer task started: " + new Date().toString("EEE MMM dd HH:mm:ss z yyyy"));
                init();
                p("Timer task complete: " + new Date().toString("EEE MMM dd HH:mm:ss z yyyy"));
            }
        }, delay, interval);
    }


  //**************************************************************************
  //** cancelTimer
  //**************************************************************************
    private void cancelTimer() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }


  //**************************************************************************
  //** executeNodesAndCountsQuery
  //**************************************************************************
    private JSONObject executeNodesAndCountsQuery() {
        String query =
                "MATCH (n) RETURN distinct labels(n) as labels, count(labels(n)) as count, sum(size((n) <--())) as relations";

        JSONObject containerOfCounts = new JSONObject();
        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute(query);
            while (rs.hasNext()) {
                Map<String, Object> r = rs.next();
                JSONArray labelValue = new JSONArray(r.get("labels").toString());
                Set<String> labelSet = new HashSet<String>();
                labelValue.forEach(l -> labelSet.add(l.toString()));
                String nodeKey = String.valueOf(labelSet.hashCode());
                Long countValue = Long.parseLong(r.get("count").toString());
                Long relationsValue = Long.parseLong(r.get("relations").toString());
                containerOfCounts.set(nodeKey,
                        newNodesAndCountsObject(labelValue, countValue, relationsValue, 0, -1));
            }
            return containerOfCounts;
        } catch (Exception e) {
            e("executeNodesAndCountsQuery: " + e);
        }
        return null;
    }


  //**************************************************************************
  //** getMetadataNodeData
  //**************************************************************************
  /** Returns the metadata node
   */
    private Node getMetadataNodeData(Transaction tx) throws NotInTransactionException {
        try {
            Label metadataNodeLabel = Label.label(META_NODE_LABEL);
            ResourceIterator<Node> result = tx.findNodes(metadataNodeLabel);
            if (result.hasNext()) {
                Node metaNode = result.next();
                return metaNode;
            }
        } catch (Exception e) {
            e("getMetadataNodeData: " + e);
        }
        return null;
    }

    /**
     * Save the json value object for the 'counts' property
     * @param value
     */
    private void saveBluewaveMeta_NodesAndCounts(JSONObject value) {
        try (Transaction tx = db.beginTx()) {
            Label label = Label.label(META_NODE_LABEL);
            List<Node> returnedNodes = new ArrayList<>();

            tx.findNodes(label).forEachRemaining(n -> returnedNodes.add(n));
            if (!returnedNodes.isEmpty()) {
                Node node = returnedNodes.get(0);
                node.setProperty(KEY_COUNTS, value.toString());
            }
            tx.commit();
        } catch (Throwable e) {
            e("saveBluewaveMeta_NodesAndCounts: " + e);
        }
    }


  //**************************************************************************
  //** handleEventBeforeCommit
  //**************************************************************************
    public void handleEventBeforeCommit(final TransactionData data) throws Exception {
        if (!isAvailable) return;

      //Deleted Nodes
        try {
            if (data.deletedNodes() != null) {
                data.deletedNodes().forEach(n -> {
                    deletedNodesEventNew(n);
                });
            }
        }
        catch (Exception e) {
            e("deletedNodesEvent: calling forEach(): " + e);
        }


      //Deleted Relationships
        try {
            if (data.deletedRelationships() != null) {
                data.deletedRelationships().forEach(n -> deletedRelationshipsEvent(n));
            }
        } catch (Exception e) {
            e("deletedRelationshipsEvent: " + e);
        }
    }


  //**************************************************************************
  //** handleEventAfterCommit
  //**************************************************************************
    public synchronized void handleEventAfterCommit(final TransactionData data) throws Exception {
        if (!isAvailable) return;

      //***********************************************
      //** Created Nodes
      //***********************************************
        try {
            Map<Long, Set<String>> nodeLabels = new HashMap<Long, Set<String>>();

            if (data.createdNodes() != null) {
                /**
                 * Process each new node
                 */
                data.createdNodes().forEach(n -> {
                    Set<String> newNodesLabels = new HashSet<String>();
                    Long nodeId = null;
                    try (Transaction tx = db.beginTx()) {
                        nodeId = n.getId();
                        Node thisNode = tx.getNodeById(nodeId);
                        thisNode.getLabels().forEach(l -> newNodesLabels.add(String.valueOf(l)));
                        nodeLabels.put(nodeId, newNodesLabels);

                    } catch (Exception te) {
                        e("createNodesEventNew: calling getNodeById(): " + te);
                    }
                });
                nodeLabels.entrySet().forEach(e -> createdNodesEventNew(e.getKey(), e.getValue(),
                        data.getTransactionId()));
            }
        } catch (Exception e) {
            e("createNodesEventNew: calling forEach(): " + e);
        }

      //***********************************************
      //** Assigned Labels
      //***********************************************
        try {
            if (data.assignedLabels() != null) {
                data.assignedLabels()
                        .forEach(n -> assignedLabelEventNew(n, data.getTransactionId()));
            }
        } catch (Exception e) {
            e("assignedLabelsEvent: " + e);
        }

      //***********************************************
      //** Removed Labels
      //***********************************************
        try {
            if (data.removedLabels() != null) {
                data.removedLabels().forEach(n -> removedLabelEventNew(n, data.getTransactionId()));
            }
        } catch (Exception e) {
            e("removedLabelEvent: " + e);
        }

      //***********************************************
      //** Created Relationships
      //***********************************************
        try {
            if (data.createdRelationships() != null) {
                data.createdRelationships()
                        .forEach(n -> createdRelationshipsEvent(n, data.getTransactionId()));
            }
        } catch (Exception e) {
            e("createdRelationshipsEvent: " + e);
        }

    }

    /**
     * Convenience method to determine if current node is the metadata node
     *
     * @param tx {@link Transaction}
     * @param nodeId current node id
     * @return true if this id matches the id of the bluewave_metadata node
     */
    private boolean isBluewaveMetadataNode(Transaction tx, long nodeId) {
        Node metaNode = getMetadataNodeData(tx);
        return metaNode.getId() == nodeId;
    }

  //**************************************************************************
  //** createdNodesEvent NEW
  //**************************************************************************
    private void createdNodesEventNew(Long nodeId, Set<String> newNodesLabels, Long txId) {

        Long metaNodeId = null;
        JSONObject metaCountsNode = null;
        Node metaNode = null;
        try (Transaction tx = db.beginTx()) {
            metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            metaCountsNode = new JSONObject(metaNode.getProperty(KEY_COUNTS).toString());
        } catch (Exception e) {
            e("createdNodesEventNew:  foreach2: " + e);
        }

        /**
         * Check for bluewave_metadata node
         */
        if (metaNodeId.longValue() == nodeId.longValue()) {
            return;
        }

        /**
         * Find entry in meta for this label set
         */
        JSONValue entry = null;
        if (!(entry = metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()))).isNull()) {
            /**
             * Found entry in metadata, increment count
             */
            JSONArray entryValueArray = entry.toJSONArray();
            Long counts = entryValueArray.get(INDEX_COUNT).toLong();
            counts++;
            entryValueArray.set(INDEX_TRANSACTION_ID, txId);
            entryValueArray.set(INDEX_NODE_ID, nodeId);
            metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()), entryValueArray);
        } else {
            /**
             * Create new node for counts
             */
            JSONArray labelsValue = new JSONArray();
            newNodesLabels.iterator().forEachRemaining(l -> labelsValue.add(l));
            metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()),
                    newNodesAndCountsObject(labelsValue, 1, 0, txId, nodeId));
        }

        saveBluewaveMeta_NodesAndCounts(metaCountsNode);

    }

  //**************************************************************************
  //** deletedNodesEvent New
  //**************************************************************************
    private void deletedNodesEventNew(Node node) {

        Long metaNodeId = null;
        Long nodeId = node.getId();
        JSONObject metaCountsNode = null;
        try (Transaction tx = db.beginTx()) {
            Node metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            metaCountsNode = new JSONObject(metaNode.getProperty(KEY_COUNTS).toString());
        } catch (Exception e) {
            e("deletedNodesEventNew getMetadataNodeData: " + e);
            return;
        }

        boolean somethingToSave = false;

        /**
         * Check for bluewave_metadata node
         */
        if (metaNodeId.longValue() != nodeId.longValue()) {
            try {
                /**
                 * Gather all labels from this node
                 */
                Set<String> newNodesLabels = new HashSet<String>();
                try (Transaction tx = db.beginTx()) {
                    tx.getNodeById(nodeId).getLabels()
                            .forEach(l -> newNodesLabels.add(String.valueOf(l)));
                } catch (Exception e) {
                    e("deletedNodesEventNew get all labels: " + e);
                    return;
                }
                JSONValue jsonArrayNodeCountsValue = null;
                if ((jsonArrayNodeCountsValue =
                        metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()))) != null) {
                    /**
                     * Found node in metadata, decrement count
                     */
                    JSONArray jsonArrayNodeCounts = jsonArrayNodeCountsValue.toJSONArray();
                    Long counts = jsonArrayNodeCounts.get(INDEX_COUNT).toLong();
                    counts--;
                    jsonArrayNodeCounts.set(INDEX_COUNT, counts);
                    jsonArrayNodeCounts.set(INDEX_NODE_ID, node.getId());
                    jsonArrayNodeCounts.set(INDEX_TRANSACTION_ID, -1);
                    metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()),
                            jsonArrayNodeCounts);
                    if (counts == 0) {
                        metaCountsNode.remove(String.valueOf(newNodesLabels.hashCode()));
                    }
                    somethingToSave = true;
                }
            } catch (Exception e) {
                e("deletedNodesEventNew: " + e);
            }
        }

        if (somethingToSave) {
              saveBluewaveMeta_NodesAndCounts(metaCountsNode);
        }
    }

  //**************************************************************************
  //** removedLabelEvent NEW
  //**************************************************************************
    private void removedLabelEventNew(LabelEntry labelEntry, Long txId) {

        String labelName = labelEntry.label().name();
        if (labelName.equals(META_NODE_LABEL)) {
            return;
        }

        Long metaNodeId = null;
        Long nodeId = labelEntry.node().getId();
        JSONObject metaCountsNode = null;
        /**
         * Gather all labels from this node
         */
        Set<String> newNodesLabels = new HashSet<String>();
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(nodeId).getLabels().forEach(l -> newNodesLabels.add(String.valueOf(l)));
        } catch (org.neo4j.graphdb.NotFoundException e) {
            return;
        }

        try (Transaction tx = db.beginTx()) {
            Node metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            metaCountsNode = new JSONObject(metaNode.getProperty(KEY_COUNTS).toString());
        } catch (Exception e) {
            e("removedLabelEventNew getMetadataNodeData: " + e);
            return;
        }

        /**
         * Discard bluewave_metadata transactions
         */
        if (metaNodeId == nodeId) {
            return;
        }

        /**
         * Add the newLabel to the labelset
         */
        newNodesLabels.add(labelName);

        /**
         * Check for existence of the entry using the label set
         */
        JSONValue entry = null;
        entry = metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
        if (!entry.isNull()) {
            /**
             ** Found entry in metadata
             */
            JSONArray entryValue = entry.toJSONArray();
            long lastTxId = entryValue.get(INDEX_TRANSACTION_ID).toLong();
            long lastNodeId = entryValue.get(INDEX_NODE_ID).toLong();
            /**
             * Check if we updated this node count already
             */
            if (lastTxId == txId && lastNodeId == nodeId) {
                return;
            }
            /**
             * Finally, decrement count
             */
            Long counts = entryValue.get(INDEX_COUNT).toLong();
            counts--;
            if (counts == 0) {
                /**
                 * Remove entry
                 */
                metaCountsNode.remove(String.valueOf(newNodesLabels.hashCode()));
            } else {
                entryValue.set(INDEX_COUNT, counts);
                entryValue.set(INDEX_TRANSACTION_ID, txId);
                entryValue.set(INDEX_NODE_ID, nodeId);
            }

            if (newNodesLabels.size() > 1) {
                /**
                 * Confirm entry that contains the new label set exists
                 */
                if (newNodesLabels.remove(labelName)) {
                    JSONValue newEntryValue =
                            metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
                    if (newEntryValue == null || newEntryValue.isNull()) {
                        /**
                         * Create new entry
                         */
                        JSONArray labelsValue = new JSONArray();
                        newNodesLabels.iterator().forEachRemaining(l -> labelsValue.add(l));
                        metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()),
                                newNodesAndCountsObject(labelsValue, 1, 0, txId, nodeId));
                    }
                }
            }
        }

        saveBluewaveMeta_NodesAndCounts(metaCountsNode);
    }

  //**************************************************************************
  //** assignedLabelEvent NEW
  //**************************************************************************
    private void assignedLabelEventNew(LabelEntry labelEntry, Long txId) {
        String labelName = labelEntry.label().name();
        if (labelName.equals(META_NODE_LABEL)) {
            return;
        }

        Long metaNodeId = null;
        Long nodeId = labelEntry.node().getId();
        JSONObject metaCountsNode = null;

        try (Transaction tx = db.beginTx()) {
            Node metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            metaCountsNode = new JSONObject(metaNode.getProperty(KEY_COUNTS).toString());
        } catch (Exception e) {
            e("assignedLabelEventNew getMetadataNodeData: " + e);
            return;
        }

        /**
         * Discard bluewave_metadata transactions
         */
        if (metaNodeId == nodeId) {
            return;
        }

        /**
         * Gather all labels from this node
         */
        Set<String> newNodesLabels = new HashSet<String>();
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(nodeId).getLabels().forEach(l -> newNodesLabels.add(String.valueOf(l)));
        } catch (Exception e) {
            e("assignedLabelEventNew get all labels: " + e);
            return;
        }

        /**
         * Check for existence of the entry using the label set
         */
        JSONValue entry = null;
        entry = metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
        if (!entry.isNull()) {
            /**
             ** Found entry in metadata
             */
            JSONArray entryValue = entry.toJSONArray();
            long lastTxId = entryValue.get(INDEX_TRANSACTION_ID).toLong();
            long lastNodeId = entryValue.get(INDEX_NODE_ID).toLong();
            /**
             * Check if we updated this node count already
             */
            if (lastTxId == txId && lastNodeId == nodeId) {
                return;
            }
            /**
             * Finally, increment count
             */
            Long counts = entryValue.get(INDEX_COUNT).toLong();
            counts++;
            entryValue.set(INDEX_COUNT, counts);
            entryValue.set(INDEX_TRANSACTION_ID, txId);
            entryValue.set(INDEX_NODE_ID, nodeId);

        } else {
            /**
             * Create new entry
             */
            JSONArray labelsArray = new JSONArray();
            for (String string : newNodesLabels) {
                labelsArray.add(string);
            }
            JSONArray newEntryValue = newNodesAndCountsObject(labelsArray, 1L, 0L, txId, nodeId);
            metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()), newEntryValue);

            /**
             * Find entry that contained this node prior to adding this label
             */
            if (newNodesLabels.size() > 1) {
                if (newNodesLabels.remove(labelName)) {
                    JSONValue originalEntryValue =
                            metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
                    if (originalEntryValue != null && !originalEntryValue.isNull()) {
                        JSONArray originalEntryValueArray = originalEntryValue.toJSONArray();
                        long originalNodeId = originalEntryValueArray.get(INDEX_NODE_ID).toLong();
                        if (originalNodeId == nodeId) {
                            /**
                             * Decrement count in original entry Also found PRIOR entry in metadata
                             * w/same nodeId for labels, decrement count
                             */
                            long originalNodeCount =
                                    originalEntryValueArray.get(INDEX_COUNT).toLong();
                            if (originalNodeCount > 0) {
                                originalNodeCount--;
                                if (originalNodeCount == 0) {
                                    /**
                                     * No more nodes containing this label set exist so remove from
                                     * index Remove PRIOR entry w/same nodeId for labels
                                     */
                                    metaCountsNode
                                            .remove(String.valueOf(newNodesLabels.hashCode()));
                                } else {
                                    originalEntryValueArray.set(INDEX_COUNT, originalNodeCount);
                                    metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()),
                                            originalEntryValueArray);
                                }
                            }
                        }
                    }
                }
            }
        }
        saveBluewaveMeta_NodesAndCounts(metaCountsNode);
    }

  //**************************************************************************
  //** createdRelationshipsEvent
  //**************************************************************************
    private void createdRelationshipsEvent(Relationship createdRelationship, Long txId) {
        /**
         * Collect r where (n) <--() These are where the nodes are the end nodes of the relationship
         */
        Node metaNode = null;
        Long endNodeId = createdRelationship.getEndNodeId();
        Long metaNodeId = null;
        JSONObject metaCountsNode = null;

        /**
         * Get metadata node
         */
        try (Transaction tx = db.beginTx()) {
            metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            metaCountsNode = new JSONObject(metaNode.getProperty(KEY_COUNTS).toString());
        } catch (Exception e) {
            e("createdRelationshipsEvent getMetadataNodeData: " + e);
            return;
        }

        /**
         * Check if node is bluewave_metadata
         */
        if (metaNodeId == endNodeId) {
            return;
        }

        /**
         * Get endNode label set
         */
        Set<String> newNodesLabels = new HashSet<String>();
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(endNodeId).getLabels()
                    .forEach(l -> newNodesLabels.add(String.valueOf(l)));
        } catch (Exception e) {
            e("createdRelationshipsEvent getMetadataNodeData: " + e);
            return;
        }

        /**
         * Check for existence of the entry using the label set
         */
        JSONValue entry = null;
        entry = metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
        if (!entry.isNull()) {
            /**
             ** Found entry in metadata
             */
            JSONArray entryValue = entry.toJSONArray();

            /**
             * Finally, increment relations
             */
            Long relations = entryValue.get(INDEX_RELATIONS).toLong();
            relations++;
            entryValue.set(INDEX_RELATIONS, relations);
            entryValue.set(INDEX_TRANSACTION_ID, txId);
            entryValue.set(INDEX_NODE_ID, endNodeId);
            metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()), entryValue);
            saveBluewaveMeta_NodesAndCounts(metaCountsNode);
        } else {
            /**
             * Create new entry
             */
            JSONArray labelsArray = new JSONArray();
            for (String string : newNodesLabels) {
                labelsArray.add(string);
            }
            JSONArray newEntryValue = newNodesAndCountsObject(labelsArray, 1L, 1L, txId, endNodeId);
            metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()), newEntryValue);
            saveBluewaveMeta_NodesAndCounts(metaCountsNode);
        }

    }

  //**************************************************************************
  //** deletedRelationshipsEvent
  //**************************************************************************
    private void deletedRelationshipsEvent(Relationship deletedRelationship) {
        Node metaNode = null;
        Long endNodeId = deletedRelationship.getEndNodeId();
        Long metaNodeId = null;
        JSONObject metaCountsNode = null;

        /**
         * Get metadata node
         */
        try (Transaction tx = db.beginTx()) {
            metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            metaCountsNode = new JSONObject(metaNode.getProperty(KEY_COUNTS).toString());
        } catch (Exception e) {
            e("deletedRelationshipsEvent getMetadataNodeData: " + e);
            return;
        }

        /**
         * Check if node is bluewave_metadata
         */
        if (metaNodeId == endNodeId) {
            return;
        }

        /**
         * Get endNode label set
         */
        Set<String> newNodesLabels = new HashSet<String>();
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(endNodeId).getLabels()
                    .forEach(l -> newNodesLabels.add(String.valueOf(l)));
        } catch (Exception e) {
            e("deletedRelationshipsEvent getMetadataNodeData: " + e);
            return;
        }

        /**
         * Check for existence of the entry using the label set
         */
        JSONValue entry = null;
        entry = metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
        if (!entry.isNull()) {
            /**
             ** Found entry in metadata
             */
            JSONArray entryValue = entry.toJSONArray();

            /**
             * Finally, increment relations
             */
            Long relations = entryValue.get(INDEX_RELATIONS).toLong();
            relations--;
            entryValue.set(INDEX_RELATIONS, relations);
            entryValue.set(INDEX_TRANSACTION_ID, -1);
            entryValue.set(INDEX_NODE_ID, endNodeId);
            metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()), entryValue);
            saveBluewaveMeta_NodesAndCounts(metaCountsNode);
        }
    }

    /**
     * Holds the nodes and counts data
     * @param labels
     * @param count
     * @param relations
     * @param txId
     * @param nodeId
     * @return
     */
    private JSONArray newNodesAndCountsObject(JSONArray labels, long count, long relations,
            long txId, long nodeId) {
        JSONArray data = new JSONArray();
        data.add(labels);
        data.add(count);
        data.add(relations);
        data.add(txId);
        data.add(nodeId);
        return data;
    }

    private void p(Object message) {
        console.log(message.toString());
    }

    private void e(Object message) {
        console.log("*** ------- ERROR ------- *** " + message.toString());
    }

    private void setInterval(Long interval) {
        this.interval = interval;
    }

    private void setDelay(Long delay) {
        this.delay = delay;
    }


  //**************************************************************************
  //** log
  //**************************************************************************
  /** Used to add an event to the queue
   */
    public void log(String action, String type, JSONArray data, String username){
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