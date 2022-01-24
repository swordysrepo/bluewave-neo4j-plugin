package bluewave.neo4j.plugins;

import static javaxt.utils.Console.console;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import javaxt.json.JSONArray;
import javaxt.json.JSONObject;
import javaxt.json.JSONValue;

public class Metadata {

    public static final int BEFORE_COMMIT = 1;
    public static final int AFTER_COMMIT = 2;

    public static final String META_NODE_LABEL = "bluewave_metadata";
    public static final String KEY_COUNTS = "counts";
    public static final String KEY_PROPERTIES = "properties";

    public static boolean metaNodeExist = false;
    private GraphDatabaseService db;

    public static final int INDEX_LABELS = 0;
    public static final int INDEX_COUNT = 1;
    public static final int INDEX_RELATIONS = 2;
    public static final int INDEX_TRANSACTION_ID = 3;
    public static final int INDEX_NODE_ID = 4;

    List<DeletedNode> deletedNodes = new ArrayList<DeletedNode>();

    public Metadata(GraphDatabaseService databaseService) {
        db = databaseService;
    }

    /**
     * Create the metadata node if it doesn't exist
     * 
     */
    public synchronized void init() {

        if (metaNodeExist)
            return;

        metaNodeExist = true;
        Label label = Label.label(META_NODE_LABEL);
        List<Node> returnedNodes = new ArrayList<>();

        /**
         * Check if metadata node exists
         */
        while (!db.isAvailable(500)) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (Throwable t) {
            }
        }

        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodesIterator = tx.findNodes(label);
            if (nodesIterator.hasNext()) {
                returnedNodes.add(nodesIterator.next());
            }
        } catch (Exception e) {
            e("init findNodes: " + e);
        }

        if (returnedNodes.isEmpty()) {
            /**
             * Create the metadata node
             */
            try (Transaction tx = db.beginTx()) {
                Node metadataNode = tx.createNode(label);
                /**
                 * Example structure of counts in metadata
                 * (n:bluewave_metadata{"counts":{"534657143":[["label1","labeln"],15,
                 * 4],"7989553324":[["label1","labeln"],13, 2]}})
                 */
                metadataNode.setProperty(KEY_COUNTS, executeNodesAndCountsQuery().toString());
                /**
                 * Example structure of properties in metadata
                 * (n:bluewave_metadata{"properties":{"SOME_LABEL":["prop1","propn"],"SOME_LABEL2":[["prop1","propn"]}})
                 */
                metadataNode.setProperty(KEY_PROPERTIES, executeNodesAndPropertiesQuery().toString());
                tx.commit();
                metaNodeExist = true;
            } catch (Exception e) {
                e("init createNode:" + e);
            }
        }
    }

    /**
     * Example format of Nodes and Counts
     * {"534657143":[["label1","labeln"],15, 4],"7989553324":[["label1","labeln"],13, 2]}
     * 
     * @return JSONObject of NodesAndCounts results
     */
    private JSONObject executeNodesAndCountsQuery() {
        String query = "MATCH (n) RETURN distinct labels(n) as labels, count(labels(n)) as count, sum(size((n) <--())) as relations";
        /**
         * Execute query
         */
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
                containerOfCounts.set(nodeKey, newNodesAndCountsObject(labelValue, countValue, relationsValue, 0, -1));
            }
            return containerOfCounts;
        } catch (Exception e) {
            e("executeNodesAndCountsQuery: " + e);
        }
        return null;
    }

    private JSONObject executeNodesAndPropertiesQuery() {
        String query = "MATCH(n) WITH LABELS(n) AS labels , KEYS(n) AS keys UNWIND labels AS label UNWIND keys AS key RETURN DISTINCT label as nodes, COLLECT(DISTINCT key) AS properties ORDER BY label";
        /**
         * Execute query
         */
        JSONObject containerOfNodesAndProperties = new JSONObject();
        try (Transaction tx = db.beginTx()) {
            Result rs = tx.execute(query);
            while (rs.hasNext()) {
                // Get record
                Map<String, Object> row = rs.next();
                String labelValue = row.get("nodes").toString();
                List<String> properties = (List<String>) row.get("properties");
                containerOfNodesAndProperties.set(labelValue, properties);
            }
            return containerOfNodesAndProperties;
        } catch (Exception e) {
            e("executeNodesAndPropertiesQuery: " + e);
        }
        return null;
    }

    /**
     * Retrieve the bluewave_metadata node
     * 
     * @param Transaction tx
     * @return {@link Node}
     * @throws NotInTransactionException
     */
    public Node getMetadataNodeData(Transaction tx) throws NotInTransactionException {
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
     * Saves and overwrites the value for the property 'data' of the
     * bluewave_metadata node
     * 
     * @param dataValue The bluewave_metadata node has a property named 'data'. This
     *                  property contains the json
     *                  representation of the db
     */
    private void saveBluewaveMeta_DataProperty(String dataValue) {
        try (Transaction tx = db.beginTx()) {
            Label label = Label.label(META_NODE_LABEL);
            List<Node> returnedNodes = new ArrayList<>();
            /**
             * Check if metadata node exists
             */
            tx.findNodes(label).forEachRemaining(n -> returnedNodes.add(n));
            if (!returnedNodes.isEmpty()) {
                /**
                 * Metadata node does not exist.
                 * Retrieve all nodes and properties
                 */
                Node node = returnedNodes.get(0);
                node.setProperty("data", dataValue.toString());
                // tx.commit();
            }
            tx.commit();
        } catch (Throwable e) {
            e("saveBluewaveMeta_DataProperty: " + e);
        }
    }

    private void saveBluewaveMeta_NodesAndCounts(JSONObject value) {
        try (Transaction tx = db.beginTx()) {
            Label label = Label.label(META_NODE_LABEL);
            List<Node> returnedNodes = new ArrayList<>();
            /**
             * Get metadata node
             */
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

    // private void saveBluewaveMeta_NodesAndProperties(String value) {
    //     try (Transaction tx = db.beginTx()) {
    //         Label label = Label.label(META_NODE_LABEL);
    //         List<Node> returnedNodes = new ArrayList<>();
    //         /**
    //          * Get metadata node
    //          */
    //         tx.findNodes(label).forEachRemaining(n -> returnedNodes.add(n));
    //         if (!returnedNodes.isEmpty()) {
    //             Node node = returnedNodes.get(0);
    //             node.setProperty(KEY_PROPERTIES, value.toString());
    //         }
    //         tx.commit();
    //     } catch (Throwable e) {
    //         e("saveBluewaveMeta_NodesAndProperties: " + e);
    //     }
    // }

    // **************************************************************************
    // ** handleEventBeforeCommit
    // **************************************************************************
    public void handleEventBeforeCommit(final TransactionData data) throws Exception {
        // ***********************************************
        // ** Deleted Nodes New
        // ***********************************************
        try {

            if (data.deletedNodes() != null) {
                data.deletedNodes().forEach(n -> {
                    deletedNodesEventNew(n);
                });
            }
        } catch (Exception e) {
            e("deletedNodesEvent: calling forEach(): " + e);
        }

        // ***********************************************
        // ** Deleted Relationships
        // ***********************************************
        try {
            if (data.deletedRelationships() != null) {
                data.deletedRelationships().forEach(n -> deletedRelationshipsEvent(n));
            }
        } catch (Exception e) {
            e("deletedRelationshipsEvent: " + e);
        }
    }

    // **************************************************************************
    // ** handleEventAfterCommit
    // **************************************************************************
    public synchronized void handleEventAfterCommit(final TransactionData data) throws Exception {

        // ***********************************************
        // ** Created Nodes
        // ***********************************************
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
                nodeLabels.entrySet().forEach(e -> createdNodesEventNew(e.getKey(),
                        e.getValue(), data.getTransactionId()));
            }
        } catch (Exception e) {
            e("createNodesEventNew: calling forEach(): " + e);
        }

        // ***********************************************
        // ** Assigned Labels
        // ***********************************************
        try {
            if (data.assignedLabels() != null) {
                data.assignedLabels().forEach(n -> assignedLabelEventNew(n, data.getTransactionId()));
            }
        } catch (Exception e) {
            e("assignedLabelsEvent: " + e);
        }

        // ***********************************************
        // ** Removed Labels
        // ***********************************************
        try {
            if (data.removedLabels() != null) {
                data.removedLabels().forEach(n -> removedLabelEventNew(n, data.getTransactionId()));
            }
        } catch (Exception e) {
            e("removedLabelEvent: " + e);
        }

        // ***********************************************
        // ** Created Relationships
        // ***********************************************
        try {
            if (data.createdRelationships() != null) {
                data.createdRelationships().forEach(n -> createdRelationshipsEvent(n, data.getTransactionId()));
            }
        } catch (Exception e) {
            e("createdRelationshipsEvent: " + e);
        }

        // ***********************************************
        // ** Assigned Properties
        // ***********************************************
        // try {
        // JSONArray assignedNodeProperties = new JSONArray();
        // if (data.assignedNodeProperties() != null) {
        // try {
        // data.assignedNodeProperties().forEach(n -> assignedNodeProperties.add(n));
        // } catch (NullPointerException npe) {
        // e("assignedNodeProperties: NPE: calling foreach(): " + npe);
        // }
        // if (!assignedNodeProperties.isEmpty()) {
        // assignedNodeProperties.iterator()
        // .forEachRemaining(n -> assignedNodePropertiesEvent((PropertyEntry<Node>) n));
        // }
        // }
        // } catch (Exception e) {
        // e("assignedNodePropertiesEvent: " + e);
        // }

        // ***********************************************
        // ** Removed Properties
        // ***********************************************
        // try {
        // JSONArray removedNodeProperties = new JSONArray();
        // if (data.removedNodeProperties() != null) {
        // try {
        // data.removedNodeProperties().forEach(n -> removedNodeProperties.add(n));
        // } catch (NullPointerException npe) {
        // e("removedNodeProperties: NPE: calling foreach(): " + npe);
        // }
        // if (!removedNodeProperties.isEmpty()) {
        // removedNodeProperties.iterator()
        // .forEachRemaining(n -> removedNodePropertiesEvent((PropertyEntry<Node>) n));
        // }
        // }
        // } catch (Exception e) {
        // e("removedNodePropertiesEvent: " + e);
        // }

    }

    /**
     * Convenience method to determine is current node is the metadata node
     * 
     * @param tx     {@link Transaction}
     * @param nodeId current node id
     * @return true if this id matches the id of the bluewave_metadata node
     */
    public boolean isBluewaveMetadataNode(Transaction tx, long nodeId) {
        Node metaNode = getMetadataNodeData(tx);
        return metaNode.getId() == nodeId;
    }

    // **************************************************************************
    // ** createdNodesEvent NEW
    // **************************************************************************
    private void createdNodesEventNew(Long nodeId, Set<String> newNodesLabels, Long txId) {
        /**
         * Get bluewave_metadata
         */
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
        if (!(entry = metaCountsNode
                .get(String.valueOf(newNodesLabels.hashCode()))).isNull()) {
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

        /**
         * Save changes
         */
        saveBluewaveMeta_NodesAndCounts(metaCountsNode);

    }

    // **************************************************************************
    // ** deletedNodesEvent New
    // **************************************************************************
    private void deletedNodesEventNew(Node node) {
        /**
         * Get bluewave_metadata node data
         * 
         * Process:
         * Get label(s) for node
         * 
         */
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
                    tx.getNodeById(nodeId).getLabels().forEach(l -> newNodesLabels.add(String.valueOf(l)));
                } catch (Exception e) {
                    e("deletedNodesEventNew get all labels: " + e);
                    return;
                }
                JSONValue jsonArrayNodeCountsValue = null;
                if ((jsonArrayNodeCountsValue = metaCountsNode
                        .get(String.valueOf(newNodesLabels.hashCode()))) != null) {
                    /**
                     * Found node in metadata, decrement count
                     */
                    JSONArray jsonArrayNodeCounts = jsonArrayNodeCountsValue.toJSONArray();
                    Long counts = jsonArrayNodeCounts.get(INDEX_COUNT).toLong();
                    counts--;
                    jsonArrayNodeCounts.set(INDEX_COUNT, counts);
                    jsonArrayNodeCounts.set(INDEX_NODE_ID, node.getId());
                    jsonArrayNodeCounts.set(INDEX_TRANSACTION_ID, -1);
                    metaCountsNode.set(String.valueOf(newNodesLabels.hashCode()), jsonArrayNodeCounts);
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
            /**
             * Replace the counts property
             */
            saveBluewaveMeta_NodesAndCounts(metaCountsNode);
        }
    }

    // **************************************************************************
    // ** removedLabelEvent NEW
    // **************************************************************************
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
         * 
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
                    JSONValue newEntryValue = metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
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

    // **************************************************************************
    // ** assignedLabelEvent NEW
    // **************************************************************************
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
                    JSONValue originalEntryValue = metaCountsNode.get(String.valueOf(newNodesLabels.hashCode()));
                    if (originalEntryValue != null && !originalEntryValue.isNull()) {
                        JSONArray originalEntryValueArray = originalEntryValue.toJSONArray();
                        long originalNodeId = originalEntryValueArray.get(INDEX_NODE_ID).toLong();
                        if (originalNodeId == nodeId) {
                            /**
                             * Decrement count in original entry
                             * Also found PRIOR entry in metadata w/same nodeId for labels, decrement count
                             */
                            long originalNodeCount = originalEntryValueArray.get(INDEX_COUNT).toLong();
                            if (originalNodeCount > 0) {
                                originalNodeCount--;
                                if (originalNodeCount == 0) {
                                    /**
                                     * No more nodes containing this label set exist so remove from index
                                     * Remove PRIOR entry w/same nodeId for labels
                                     */
                                    metaCountsNode.remove(String.valueOf(newNodesLabels.hashCode()));
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

    // **************************************************************************
    // ** createdRelationshipsEvent
    // **************************************************************************
    private void createdRelationshipsEvent(Relationship createdRelationship, Long txId) {
        /**
         * Collect r where (n) <--() These are where the nodes are the end nodes of the
         * relationship
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
            tx.getNodeById(endNodeId).getLabels().forEach(l -> newNodesLabels.add(String.valueOf(l)));
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
            long lastTxId = entryValue.get(INDEX_TRANSACTION_ID).toLong();
            long lastNodeId = entryValue.get(INDEX_NODE_ID).toLong();
            /**
             * Check if we updated this node count already
             */
            // if (lastTxId == txId && lastNodeId == endNodeId) {
            // p("Found matching transaction & node ids, discarding.");
            // return;
            // }
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

    // **************************************************************************
    // ** deletedRelationshipsEvent
    // **************************************************************************
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
            tx.getNodeById(endNodeId).getLabels().forEach(l -> newNodesLabels.add(String.valueOf(l)));
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
            long lastTxId = entryValue.get(INDEX_TRANSACTION_ID).toLong();
            long lastNodeId = entryValue.get(INDEX_NODE_ID).toLong();
            /**
             * Check if we updated this node count already
             */
            // if (lastTxId == txId && lastNodeId == endNodeId) {
            // p("Found matching transaction & node ids, discarding.");
            // return;
            // }
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

    // **************************************************************************
    // ** removedNodePropertiesEvent
    // **************************************************************************
    public void removedNodePropertiesEvent(PropertyEntry<Node> propertyEntryNode) {
        /**
         * Grab new incoming property and node Id
         */
        String newProperty = null;
        Long nodeID = null;
        newProperty = propertyEntryNode.key();
        nodeID = propertyEntryNode.entity().getId();

        if (newProperty == null) {
            return;
        }

        /**
         * Get bluewave_metadata node data
         */
        Long metaNodeId = null;
        JSONObject jsonWrapperObject;
        JSONObject metadataNodesJSONObject;
        try (Transaction tx = db.beginTx()) {
            Node metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            jsonWrapperObject = new JSONObject(metaNode.getProperty("data").toString());
            metadataNodesJSONObject = jsonWrapperObject.get("nodes").toJSONObject();
        } catch (Exception e) {
            e("removedNodePropertiesEvent getMetadataNodeData: " + e);
            return;
        }

        /**
         * Check if this is a bluewave_metadata transaction
         */
        if (metaNodeId == nodeID) {
            return;
        }

        /**
         * Check for existence of the node this label is attached to
         */
        JSONObject nodeObject = null;
        if (metadataNodesJSONObject.get(nodeID.toString()) != null) {
            nodeObject = metadataNodesJSONObject.get(nodeID.toString()).toJSONObject();
        }

        /**
         * If node is found - remove property
         * If node is not found - do nothing, node may have been deleted already
         */
        if (nodeObject != null) {

            JSONArray nodeProps = nodeObject.get("properties").toJSONArray();
            boolean somethingToSave = false;
            if (nodeProps.isEmpty()) {
                /**
                 * Do nothing
                 */
            } else {
                /**
                 * If property is present, remove it
                 */
                int index = -1;
                for (int i = 0; i < nodeProps.length(); i++) {
                    String temp = nodeProps.get(i).toString();
                    if (temp.equals(newProperty)) {
                        index = i;
                        break;
                    }
                }
                if (index != -1) {
                    nodeProps.remove(index);
                    somethingToSave = true;
                }

            }
            if (somethingToSave) {
                nodeObject.set("properties", nodeProps);
                metadataNodesJSONObject.set(nodeID.toString(), nodeObject);

                if (somethingToSave) {
                    jsonWrapperObject.set("nodes", metadataNodesJSONObject);
                    saveBluewaveMeta_DataProperty(jsonWrapperObject.toString());
                }

            }
        }
    }

    // **************************************************************************
    // ** assignedNodePropertiesEvent
    // **************************************************************************
    private void assignedNodePropertiesEvent(PropertyEntry<Node> propertyEntryNode) {

        if (propertyEntryNode == null)
            return;

        /**
         * Grab new incoming property and node Id
         */
        String newProperty = null;
        Long nodeID = null;
        newProperty = propertyEntryNode.key();
        nodeID = propertyEntryNode.entity().getId();

        if (newProperty == null) {
            return;
        }

        /**
         * Get bluewave_metadata node data
         */
        Long metaNodeId = null;
        JSONObject jsonWrapperObject;
        JSONObject metadataNodesJSONObject;
        try (Transaction tx = db.beginTx()) {
            Node metaNode = getMetadataNodeData(tx);
            metaNodeId = metaNode.getId();
            jsonWrapperObject = new JSONObject(metaNode.getProperty("data").toString());
            metadataNodesJSONObject = jsonWrapperObject.get("nodes").toJSONObject();
        } catch (Exception e) {
            e("assignedNodePropertiesEvent getMetadataNodeData: " + e);
            return;
        }

        /**
         * Check if this is a bluewave_metadata transaction
         */
        if (metaNodeId == nodeID) {
            return;
        }

        JSONObject nodeObject = findOrCreateNodeFromMetadata(nodeID, metadataNodesJSONObject);
        JSONArray nodeProps = nodeObject.get("properties").toJSONArray();
        boolean somethingToSave = false;
        if (nodeProps.isEmpty()) {
            /**
             * This node has zero properties so just add the new one
             */
            nodeProps.add(newProperty);
            somethingToSave = true;
        } else {
            boolean found = false;
            for (Object object : nodeProps) {
                if (String.valueOf(object).equals(newProperty)) {
                    found = true;
                }
            }
            if (!found) {
                /**
                 * New property is not present in list so add
                 */
                nodeProps.add(newProperty);
                somethingToSave = true;
            }
        }

        if (somethingToSave) {
            nodeObject.set("properties", nodeProps);
            metadataNodesJSONObject.set(nodeID.toString(), nodeObject);

            try (Transaction tx = db.beginTx()) {
                if (somethingToSave) {
                    jsonWrapperObject.set("nodes", metadataNodesJSONObject);
                    saveBluewaveMeta_DataProperty(jsonWrapperObject.toString());
                }
            } catch (Exception e) {
                e(">: assignedNodePropertiesEvent: " + e.getMessage());
            }
        }
    }

    // **************************************************************************
    // ** findOrCreateNodeFromMetadata
    // ** Convenience method for creating a node if it does not exist
    // **************************************************************************
    private JSONObject findOrCreateNodeFromMetadata(Long nodeId, JSONObject metadataNodesJSONObject) {

        /**
         * Check for existence of the node this label is attached to
         */
        JSONObject nodeObject = null;
        if (metadataNodesJSONObject.get(nodeId.toString()) != null) {
            nodeObject = metadataNodesJSONObject.get(nodeId.toString()).toJSONObject();
        }

        if (nodeObject == null) {
            /**
             * If we do not have a reference to this node then we create it
             */
            metadataNodesJSONObject.set(nodeId.toString(), newNode(nodeId));
        }

        return nodeObject;
    }

    /**
     * Convenience method to check existence of a relationship
     * 
     * @param relationships JSONArray of ids representing a node's relationships
     * @param id            the relationship id to compare
     * @return index of matching relationship
     */
    private int relationshipExist(JSONArray relationships, Long id) {

        for (int i = 0; i < relationships.length(); i++) {
            if (relationships.get(i).toLong().longValue() == id) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns a new json node representation
     * 
     * @param id
     * @return node {@link JSONObject}
     */
    private JSONObject newNode(Long id) {
        JSONObject nodeObject = new JSONObject();
        nodeObject.set("id", id);
        nodeObject.set("labels", new JSONArray());
        nodeObject.set("properties", new JSONArray());
        nodeObject.set("relationships", new JSONArray());
        return nodeObject;
    }

    /**
     * Holds the nodes and counts data
     * 
     * @param hashKey
     * @return
     */
    private JSONArray newNodesAndCountsObject(JSONArray labels, long count, long relations, long txId, long nodeId) {
        JSONArray data = new JSONArray();
        data.add(labels);
        data.add(count);
        data.add(relations);
        data.add(txId);
        data.add(nodeId);
        return data;
    }

    /**
     * Holds the nodes and properties data
     * 
     * @param label
     * @param properties
     * @return
     */
    private JSONArray newNodesAndPropertiesObject(String label, JSONArray properties) {
        JSONArray data = new JSONArray();
        data.set(0, label);
        data.set(1, properties);
        return data;
    }

    // private void p(Object message) {
    // console.log(message.toString());
    // }

    public void e(Object message) {
        console.log("*** ------- ERROR ------- *** " + message.toString());
    }

}

class DeletedNode {
    Node node;
    Set<String> labels;

    public DeletedNode(Node node, Set<String> labels) {
        this.node = node;
        this.labels = labels;
    }
}