package bluewave.neo4j.plugins;

import static javaxt.utils.Console.console;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import javaxt.json.JSONArray;
import javaxt.json.JSONObject;

public class Metadata {

    public static final String META_NODE_LABEL = "bluewave_metadata";
    public static final String GET_BLUEWAVE_METADATA_NODE_SQL = "MATCH (n:bluewave_metadata) return n";
    public static boolean metaNodeExist = false;
    private GraphDatabaseService db;

    public Metadata(GraphDatabaseService databaseService, TransactionData data) {
        db = databaseService;
        /**
         * Must wait for an event before we create the metadata node because we do not
         * know when the database will be ready for transactions.
         */
        if (!metaNodeExist) {
            createMetadataNodeIfNotExist();
        }
        /**
         * Process events
         */
        if (db.isAvailable(500)) {

            try {
                handleEvent(data);
            } catch (Exception e) {
                e("process event: " + e);
            }

        }
    }

    public synchronized void createMetadataNodeIfNotExist() {

        if (metaNodeExist)
            return;

        Label label = Label.label(META_NODE_LABEL);
        List<Node> returnedNodes = new ArrayList<>();
        /**
         * Check if metadata node exists
         */
        try (Transaction tx = db.beginTx()) {
            tx.findNodes(label).forEachRemaining(n -> returnedNodes.add(n));
        } catch (Exception e) {
            e("createMetadataNodeIfNotExist findNodes: " + e);
        }
        if (returnedNodes.isEmpty()) {
            /**
             * Metadata node does not exist.
             * Retrieve all nodes and properties
             */
            String allNodesAndProperties = getAllNodesAndProperties();
            /**
             * Create the metadata node
             */
            try (Transaction tx = db.beginTx()) {
                Node metadataNode = tx.createNode(label);
                metadataNode.setProperty("data", allNodesAndProperties);
                tx.commit();
                metaNodeExist = true;
            } catch (Exception e) {
                e("createMetadataNodeIfNotExist createNode:" + e);
            }
        }
    }

    private String getAllNodesAndProperties() {
        /**
         * Create json wrapper object
         */
        JSONObject rootJSONObject = new JSONObject();
        /**
         * Create NODES array object
         */
        JSONObject nodesJSONObject = new JSONObject();
        rootJSONObject.set("nodes", nodesJSONObject);

        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodesIterator = tx.getAllNodes().iterator();
            while (nodesIterator.hasNext()) {
                Node tempNode = nodesIterator.next();
                /**
                 * Get Properties of each node
                 */
                JSONArray propertiesJSONArray = new JSONArray();
                tempNode.getPropertyKeys().forEach((k) -> {
                    propertiesJSONArray.add(k);
                });

                /**
                 * Get Labels of each node
                 */
                JSONArray labelsJSONArray = new JSONArray();
                tempNode.getLabels().forEach((l) -> {
                    labelsJSONArray.add(l.name());
                });

                /**
                 * Create each node
                 */
                JSONObject aNodeJSONObject = new JSONObject();
                aNodeJSONObject.set("id", tempNode.getId());
                aNodeJSONObject.set("labels", labelsJSONArray);
                aNodeJSONObject.set("properties", propertiesJSONArray);
                nodesJSONObject.set(tempNode.getId() + "", aNodeJSONObject);
            }
            tx.commit();
        } catch (Exception e) {
            e("getAllNodesAndProperties : " + e);
        }
        return rootJSONObject.toString();
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
            p("saveBluewaveMeta_DataProperty: " + e);
        }
    }

    // **************************************************************************
    // ** handleEvent
    // **************************************************************************
    public synchronized void handleEvent(final TransactionData data) throws Exception {

        // ***********************************************
        // ** Created Nodes
        // ***********************************************
        try {
            JSONArray newNodeIdsArray = new JSONArray();
            try {
                try (Transaction tx = db.beginTx()) {

                    if (data.createdNodes() != null) {
                        data.createdNodes().forEach(n -> newNodeIdsArray.add(n.getId()));
                    }
                }
            } catch (NullPointerException npe) {
                e("createNodesEvent: NPE: calling forEach(): " + npe);
            }
            if (!newNodeIdsArray.isEmpty()) {
                p("createNodesEvent: " + newNodeIdsArray.toString());
                createdNodesEvent(newNodeIdsArray);
            }
        } catch (Exception e) {
            e("createNodesEvent: " + e);
        }

        // ***********************************************
        // ** Deleted Nodes
        // ***********************************************
        try {
            JSONArray deletedNodeIdsArray = new JSONArray();
            try {
                if (data.deletedNodes() != null) {
                    data.deletedNodes().forEach(n -> deletedNodeIdsArray.add(n.getId()));
                }
            } catch (NullPointerException npe) {
                e("deletedNodesEvent: NPE: calling foreach(): " + npe);
            }
            if (!deletedNodeIdsArray.isEmpty()) {
                p("deletedNodesEvent: " + deletedNodeIdsArray.toString());
                deletedNodesEvent(deletedNodeIdsArray);
            }
        } catch (Exception e) {
            e("deletedNodesEvent: " + e);
        }

        // ***********************************************
        // ** Assigned Labels
        // ***********************************************
        try {
            if (data.assignedLabels() != null) {
                data.assignedLabels().forEach(n -> assignedLabelsEvent(n));
            }
        } catch (Exception e) {
            e("assignedLabelsEvent: " + e);
        }

        // ***********************************************
        // ** Removed Labels
        // ***********************************************
        try {
            if (data.removedLabels() != null) {
                data.removedLabels().forEach(n -> removedLabelEvent(n));
            }
        } catch (Exception e) {
            e("removedLabelEvent: " + e);
        }

        // ***********************************************
        // ** Assigned Properties
        // ***********************************************
        try {
            JSONArray assignedNodeProperties = new JSONArray();
            if (data.assignedNodeProperties() != null) {
                try {
                    data.assignedNodeProperties().forEach(n -> assignedNodeProperties.add(n));
                } catch (NullPointerException npe) {
                    e("assignedNodeProperties: NPE: calling foreach(): " + npe);
                }
                if (!assignedNodeProperties.isEmpty()) {
                    assignedNodeProperties.iterator()
                            .forEachRemaining(n -> assignedNodePropertiesEvent((PropertyEntry<Node>) n));
                }
            }
        } catch (Exception e) {
            e("assignedNodePropertiesEvent: " + e);
        }

        // ***********************************************
        // ** Removed Properties
        // ***********************************************
        try {
            JSONArray removedNodeProperties = new JSONArray();
            if (data.removedNodeProperties() != null) {
                try {
                    data.removedNodeProperties().forEach(n -> removedNodeProperties.add(n));
                } catch (NullPointerException npe) {
                    e("removedNodeProperties: NPE: calling foreach(): " + npe);
                }
                if (!removedNodeProperties.isEmpty()) {
                    removedNodeProperties.iterator()
                            .forEachRemaining(n -> removedNodePropertiesEvent((PropertyEntry<Node>) n));
                }
            }
        } catch (Exception e) {
            e("removedNodePropertiesEvent: " + e);
        }

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
    // ** createdNodesEvent
    // **************************************************************************
    private void createdNodesEvent(JSONArray newNodeIdsArray) {
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
            e("createdNodesEvent getMetadataNodeData: " + e);
            return;
        }

        boolean somethingToSave = false;
        final Long metaId = metaNodeId;
        /**
         * Create a new node for every id
         */
        newNodeIdsArray.forEach(n -> {
            /**
             * Skip bluewave_metadata node
             */
            if (metaId.longValue() != ((Long) n).longValue()) {
                /**
                 * Skip if node already exists
                 */
                if (!metadataNodesJSONObject.has(n.toString())) {
                    JSONObject newNode = newNode((Long) n);
                    metadataNodesJSONObject.set(n.toString(), newNode);
                }
            }
        });

        if (somethingToSave) {
            /**
             * Replace the nodes property with the new nodes json object
             */
            jsonWrapperObject.set("nodes", metadataNodesJSONObject);
            saveBluewaveMeta_DataProperty(jsonWrapperObject.toString());
        }
    }

    // **************************************************************************
    // ** deletedNodesEvent
    // **************************************************************************
    public void deletedNodesEvent(JSONArray deletedNodeIdsArray) {
        Iterator<Object> iterator = deletedNodeIdsArray.iterator();
        while (iterator.hasNext()) {
            try (Transaction tx = db.beginTx()) {
                Long nodeId = (Long) iterator.next();
                Node metaNode = getMetadataNodeData(tx);

                /**
                 * Skip if node is bluewave_metadata node
                 */
                if (metaNode.getId() == nodeId) {
                    p("deletedNodesEvent: Found bluewave_metadata node .. exiting");
                    return;
                }

                /**
                 * Get the nodes property from the bluewave_metadata data property
                 */
                JSONObject jsonWrapperObject = new JSONObject(metaNode.getProperty("data").toString());
                JSONObject metadataNodesJSONObject = jsonWrapperObject.get("nodes").toJSONObject();

                /**
                 * Delete the json object
                 */
                JSONObject nodeObject = metadataNodesJSONObject.get(Long.toString(nodeId)).toJSONObject();
                if (nodeObject != null) {
                    metadataNodesJSONObject.remove(Long.toString(nodeId));
                }

                /**
                 * Replace the nodes property with the new nodes json object
                 */
                jsonWrapperObject.set("nodes", metadataNodesJSONObject);
                saveBluewaveMeta_DataProperty(jsonWrapperObject.toString());
            } catch (Throwable t) {
                e("deletedNodesEvent: " + t.toString());
            }
        }
    }

    // **************************************************************************
    // ** removedLabelEvent
    // **************************************************************************
    private void removedLabelEvent(LabelEntry labelEntry) {
        Node metaNode = null;
        try (Transaction tx = db.beginTx()) {
            Long nodeId = labelEntry.node().getId();
            String labelName = labelEntry.label().name();
            p("removedLabelEvent: " + labelName);
            /**
             * Get metadata node
             */
            metaNode = getMetadataNodeData(tx);

            /**
             * Discard bluewave_metadata transactions
             */
            if (metaNode.getId() == nodeId) {
                p("removedLabelEvent: Found bluewave_metadata for " + labelName
                        + " exiting");
                return;
            }

            /**
             * Convert the string value at the data property to JSONObject
             */
            JSONObject jsonWrapperObject = new JSONObject(metaNode.getProperty("data").toString());
            JSONObject metadataNodesJSONObject = jsonWrapperObject.get("nodes").toJSONObject();

            /**
             * Check for existence of the node this label is attached to
             */
            JSONObject nodeObject = null;
            if (metadataNodesJSONObject.get(nodeId.toString()) != null) {
                nodeObject = metadataNodesJSONObject.get(nodeId.toString()).toJSONObject();
            }

            /**
             * If node is found - remove label
             * If node is not found - do nothing, node may have been deleted already
             */
            if (nodeObject != null) {

                JSONArray nodeLabels = nodeObject.get("labels").toJSONArray();
                boolean somethingToSave = false;
                if (nodeLabels.isEmpty()) {
                    /**
                     * Do nothing
                     */
                } else {

                    /**
                     * If Label is present in list, remove it
                     */
                    int index = -1;
                    for (int i = 0; i < nodeLabels.length(); i++) {
                        String temp = nodeLabels.get(i).toString();
                        if (temp.equals(labelName)) {
                            index = i;
                            break;
                        }
                    }
                    if (index != -1) {
                        nodeLabels.remove(index);
                        somethingToSave = true;
                    }

                }
                if (somethingToSave) {
                    nodeObject.set("labels", nodeLabels);
                    metadataNodesJSONObject.set(nodeId.toString(), nodeObject);

                    if (somethingToSave) {
                        jsonWrapperObject.set("nodes", metadataNodesJSONObject);
                        saveBluewaveMeta_DataProperty(jsonWrapperObject.toString());
                    }
                }
            }
        } catch (Throwable t) {
            e("removedLabelEvent: " + t.toString());
        }
    }

    // **************************************************************************
    // ** assignedLabelEvent
    // **************************************************************************
    private void assignedLabelsEvent(LabelEntry labelEntry) {
        Node metaNode = null;
        try (Transaction tx = db.beginTx()) {
            Long nodeId = labelEntry.node().getId();
            String labelName = labelEntry.label().name();
            p("assignedLabelEvent: " + labelName);
            /**
             * Get metadata node
             */
            metaNode = getMetadataNodeData(tx);
            
            /**
             * Check if node is bluewave_metadata
             */
            if (metaNode.getId() == nodeId) {
                p(">: assignedLabelsEvent: Found bluewave_metadata for " + labelName
                        + " exiting");
                return;
            }
            
            /**
             * Discard bluewave_metadata transactions
             */
            if (metaNode.getId() == labelEntry.node().getId())
                return;

            /**
             * Convert the string value at the data property to a usuable form, JSONObject
             */
            JSONObject jsonWrapperObject = new JSONObject(metaNode.getProperty("data").toString());
            JSONObject metadataNodesJSONObject = jsonWrapperObject.get("nodes").toJSONObject();

            /**
             * Check for existence of the node this label is attached to
             */
            JSONObject nodeObject = findOrCreateNodeFromMetadata(nodeId, metadataNodesJSONObject);

            /**
             * Ensure label property exists
             */
            if (nodeObject.get("labels").isNull()) {
                nodeObject.set("labels", new JSONArray());
            }
            
            JSONArray labelsJSONArray = nodeObject.get("labels").toJSONArray();
            
            /**
             * Check for existence of this label
             */
            Optional<Object> obj = StreamSupport.stream(labelsJSONArray.spliterator(), false)
                    .filter(p -> p.toString().equals(labelName.toString())).findFirst();
             
            if (obj != null && obj.isEmpty()) {
                /**
                 * This label is not attached to this node yet, add it
                 */
                labelsJSONArray.add(labelName);
            }
            jsonWrapperObject.set("nodes", metadataNodesJSONObject);
            saveBluewaveMeta_DataProperty(jsonWrapperObject.toString());

        } catch (Throwable t) {
            e("Error -> assignedLabelEvent: " + t.toString());
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
            p("removedNodePropertiesEvent exiting .. property: " + newProperty);
            return;
        }
        p("removedNodePropertiesEvent property: " + newProperty);
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
            p("removedNodePropertiesEvent Found bluewave_metadata node .. exiting");
            return;
        }

        p("Removing property to nodeId: " + nodeID + ", prop:" + newProperty.toString());

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
            p("assignedNodePropertiesEvent exiting .. property: " + newProperty);
            return;
        }
        p("assignedNodePropertiesEvent adding property: " + newProperty + " to nodeId: " + nodeID);
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
            p("assignedNodePropertiesEvent Found bluewave_metadata node .. exiting");
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
            nodeObject = new JSONObject();
            nodeObject.set("id", nodeId);
            nodeObject.set("labels", new JSONArray());
            nodeObject.set("properties", new JSONArray());
            metadataNodesJSONObject.set(nodeId.toString(), nodeObject);
        }

        return nodeObject;
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
        return nodeObject;
    }

    private void p(Object message) {
        // console.log(message.toString());
    }

    private void e(Object message) {
        console.log("*** ------- ERROR ------- *** " + message.toString());
    }
}
