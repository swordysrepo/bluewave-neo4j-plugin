package bluewave.neo4j.plugins;

import static javaxt.utils.Console.console;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import javaxt.json.JSONArray;
import javaxt.json.JSONObject;

/**
 * Tests needed
 * *************
 * *************
 * 
 * Add nodes with labels and properties - COMPLETED -
 * -> test_Delete_All_Nodes_With_Specific_Label()
 * ------------------------------
 * Delete nodes with labels and properties - COMPLETED -
 * -> test_Delete_All_Nodes_With_Specific_Label()
 * ------------------------------
 * Add property to node with existing properties - COMPLETED -
 * -> test_Add_Property_To_All_Nodes_With_Specific_Label()
 * ------------------------------
 * Delete existing property - COMPLETED -
 * -> test_Delete_Property_From_All_Nodes_With_Specific_Label()
 * ------------------------------
 * Update property value - COMPLETED -
 * -> test_Update_Property_Value_From_All_Nodes_With_Specific_Label()
 * ------------------------------
 * Add label to existing node - COMPLETED -
 * -> test_Add_Label_To_Existing_Node()
 * ------------------------------
 * Delete existing label
 * -> test_Delete_Existing_Label() - COMPLETED -
 * ------------------------------
 * 
 * 
 * Cleanup
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MetadataTest {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private static Driver driver;
    private Neo4j embeddedDatabaseServer;

    @Test
    public void test_Delete_Existing_Label() {
        console.log("********************************************************************************");
        console.log("***************************** NEW TEST *****************************************");
        console.log("**                                                                            **");
        console.log("** test_Delete_Existing_Label()                                               **");
        console.log("**                                                                            **");
        console.log("**                                                                            **");
        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();
        /**
         * Confirm test data was added
         */
        console.log("** Test data loaded.");
        printResultsToConsole(dbService);

        final String deleteSql = "MATCH (n:TESTLABEL:TESTLABEL2) REMOVE n :TESTLABEL return n";

        console.log("** Delete Label 'TESTLABEL' From Node");
        console.log("** " + deleteSql);

        /**
         * Delete
         */
        try (Transaction tx = dbService.beginTx()) {
            tx.execute(deleteSql);
            tx.commit();
        } catch (Exception e) {

        }

        /**
         * Confirm nodes have changed.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(Metadata.GET_BLUEWAVE_METADATA_NODE_SQL);

            assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");
            JSONObject jsonObjectForDataProperty = new JSONObject(metaNode.getProperty("data").toString());

            /**
             * Let's verify the label has been removed.
             */

            Predicate<Object> isTESTLABELPresent = l -> String.valueOf(l).equals("TESTLABEL");
            Predicate<Object> isTESTLABEL2Present = l -> String.valueOf(l).equals("TESTLABEL2");

            JSONObject nodesJSONObject = jsonObjectForDataProperty.get("nodes").toJSONObject();
            jsonObjectForDataProperty.get("nodes").toJSONObject().keys().forEachRemaining(key -> {

                JSONArray labels = nodesJSONObject.get(key).get("labels").toJSONArray();
                boolean hasTestLabel2 = StreamSupport
                        .stream(labels.spliterator(), false)
                        .filter(isTESTLABEL2Present)
                        .collect(Collectors.toList())
                        .size() > 0;
                if (hasTestLabel2) {
                    boolean hasTestLabel = StreamSupport
                            .stream(labels.spliterator(), false)
                            .filter(isTESTLABELPresent)
                            .collect(Collectors.toList())
                            .size() > 0;

                    assertFalse(hasTestLabel);
                }
            });

        } catch (Exception e) {
            console.log("ERROR -> " + e.toString());
        }
    }

    @Test
    public void test_Add_Label_To_Existing_Node() {
        console.log("********************************************************************************");
        console.log("***************************** NEW TEST *****************************************");
        console.log("**                                                                            **");
        console.log("** test_Add_Label_To_Existing_Node()                                          **");
        console.log("**                                                                            **");
        console.log("**                                                                            **");

        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();
        /**
         * Confirm test data was added
         */
        console.log("** Test data loaded.");
        printResultsToConsole(dbService);

        final String updateSql = "MATCH (n:TESTNODE) SET n :NEWLABEL return n";

        console.log("** Add Label To Existing Nodes With LABEL 'TESTNODE' (No change in meta node should occur)");
        console.log("** " + updateSql);

        /**
         * Update
         */
        try (Transaction tx = dbService.beginTx()) {
            tx.execute(updateSql);
            tx.commit();
        } catch (Exception e) {

        }

        /**
         * Confirm additional label 'NEWLABEL' on nodes with :TESTNODE.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(Metadata.GET_BLUEWAVE_METADATA_NODE_SQL);

            assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");
            JSONObject jsonObjectForDataProperty = new JSONObject(metaNode.getProperty("data").toString());

            /**
             * Let's verify the new label 'NEWLABEL' is found on the node with label
             * 'TESTNODE'.
             */

            Predicate<Object> isTESTLABELPresent = l -> String.valueOf(l).equals("TESTNODE");
            Predicate<Object> isNEWLABELPresent = l -> String.valueOf(l).equals("NEWLABEL");

            JSONObject nodesJSONObject = jsonObjectForDataProperty.get("nodes").toJSONObject();
            jsonObjectForDataProperty.get("nodes").toJSONObject().keys().forEachRemaining(key -> {

                JSONArray labels = nodesJSONObject.get(key).get("labels").toJSONArray();
                boolean hasLabelTESTNODE = StreamSupport
                        .stream(labels.spliterator(), false)
                        .filter(isTESTLABELPresent)
                        .collect(Collectors.toList())
                        .size() > 0;

                boolean hasLabelNEWLABEL = StreamSupport
                        .stream(labels.spliterator(), false)
                        .filter(isNEWLABELPresent)
                        .collect(Collectors.toList())
                        .size() > 0;
                if (hasLabelTESTNODE) {
                    assertTrue(hasLabelNEWLABEL);
                }
            });
        } catch (Exception e) {
            console.log("ERROR -> " + e.toString());
        }
    }

    @Test
    public void test_Update_Property_Value_From_All_Nodes_With_Specific_Label() {
        console.log("********************************************************************************");
        console.log("***************************** NEW TEST *****************************************");
        console.log("**                                                                            **");
        console.log("** test_Update_Property_Value_From_All_Nodes_With_Specific_Label()            **");
        console.log("**                                                                            **");
        console.log("**                                                                            **");

        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();
        /**
         * Confirm test data was added
         */
        console.log("** Test data loaded.");
        printResultsToConsole(dbService);

        final String updateSql = "MATCH (n:TESTLABEL) SET n.prop1 = 'MUTABLE' return n";

        console.log("** Update Property With All Nodes With LABEL 'TESTLABEL' (No change in meta node should occur)");
        console.log("** " + updateSql);

        /**
         * Update
         */
        try (Transaction tx = dbService.beginTx()) {
            tx.execute(updateSql);
            tx.commit();
        } catch (Exception e) {

        }

        /**
         * Confirm nothing has changed.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(Metadata.GET_BLUEWAVE_METADATA_NODE_SQL);

            assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");
            JSONObject jsonObjectForDataProperty = new JSONObject(metaNode.getProperty("data").toString());

            /**
             * Let's verify the property 'prop1' is still present for the nodes containing
             * 'TESTLABEL' label.
             * This is testing for adverse effects. There should be no changes in the
             * metadata node.
             */

            Predicate<Object> isTESTLABELPresent = l -> String.valueOf(l).equals("TESTLABEL");
            Predicate<Object> isProp1Present = p -> String.valueOf(p).equals("prop1");

            JSONObject nodesJSONObject = jsonObjectForDataProperty.get("nodes").toJSONObject();
            jsonObjectForDataProperty.get("nodes").toJSONObject().keys().forEachRemaining(key -> {

                boolean hasLabel = StreamSupport
                        .stream(nodesJSONObject.get(key).get("labels").toJSONArray().spliterator(), false)
                        .filter(isTESTLABELPresent)
                        .collect(Collectors.toList())
                        .size() > 0;

                boolean hasProp1 = StreamSupport
                        .stream(nodesJSONObject.get(key).get("properties").toJSONArray().spliterator(), false)
                        .filter(isProp1Present)
                        .collect(Collectors.toList())
                        .size() > 0;
                if (hasLabel) {
                    assertTrue(hasProp1);
                }
            });
        } catch (Exception e) {
            console.log("ERROR -> " + e.toString());
        }
    }

    @Test
    public void test_Delete_Property_From_All_Nodes_With_Specific_Label() {
        console.log("********************************************************************************");
        console.log("***************************** NEW TEST *****************************************");
        console.log("**                                                                            **");
        console.log("** test_Delete_Property_From_All_Nodes_With_Specific_Label()                  **");
        console.log("**                                                                            **");
        console.log("**                                                                            **");
        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();
        /**
         * Confirm test data was added
         */
        console.log("** Test data loaded.");
        printResultsToConsole(dbService);

        final String deleteSql = "MATCH (n:TESTLABEL) REMOVE n.prop1 return n";

        console.log("** Delete Property From All Nodes With LABEL 'TESTLABEL' ");
        console.log("** " + deleteSql);

        /**
         * Delete
         */
        try (Transaction tx = dbService.beginTx()) {
            tx.execute(deleteSql);
            tx.commit();
        } catch (Exception e) {

        }

        /**
         * Confirm nodes have changed.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(Metadata.GET_BLUEWAVE_METADATA_NODE_SQL);

            assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");
            JSONObject jsonObjectForDataProperty = new JSONObject(metaNode.getProperty("data").toString());

            /**
             * Let's verify the property 'prop1' has been removed for the nodes containing
             * 'TESTLABEL' label.
             */

            Predicate<Object> isTESTLABELPresent = l -> String.valueOf(l).equals("TESTLABEL");
            Predicate<Object> isProp1Present = p -> String.valueOf(p).equals("prop1");

            JSONObject nodesJSONObject = jsonObjectForDataProperty.get("nodes").toJSONObject();
            jsonObjectForDataProperty.get("nodes").toJSONObject().keys().forEachRemaining(key -> {

                boolean hasLabel = StreamSupport
                        .stream(nodesJSONObject.get(key).get("labels").toJSONArray().spliterator(),
                                false)
                        .filter(isTESTLABELPresent)
                        .collect(Collectors.toList())
                        .size() > 0;

                boolean hasProp1 = StreamSupport
                        .stream(nodesJSONObject.get(key).get("properties").toJSONArray().spliterator(),
                                false)
                        .filter(isProp1Present)
                        .collect(Collectors.toList())
                        .size() > 0;
                if (hasLabel) {
                    assertFalse(hasProp1);
                }
            });

        } catch (Exception e) {
            console.log("ERROR -> " + e.toString());
        }
    }

    @Test
    public void test_Delete_All_Nodes_With_Specific_Label() {
        console.log("********************************************************************************");
        console.log("***************************** NEW TEST *****************************************");
        console.log("**                                                                            **");
        console.log("** test_Delete_All_Nodes_With_Specific_Label()                                **");
        console.log("**                                                                            **");
        console.log("**                                                                            **");
        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();
        /**
         * Confirm test data was added
         */
        console.log("** Test data loaded.");
        printResultsToConsole(dbService);

        final String deleteSql = "MATCH (n:TESTLABEL) DETACH DELETE n";

        console.log("** Delete All Nodes With LABEL 'TESTLABEL' ");
        console.log("** " + deleteSql);

        /**
         * Delete
         */
        try (Transaction tx = dbService.beginTx()) {
            tx.execute(deleteSql);
            tx.commit();
        } catch (Exception e) {

        }

        /**
         * Confirm nothing has changed.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(Metadata.GET_BLUEWAVE_METADATA_NODE_SQL);

            assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");
            JSONObject jsonObjectForDataProperty = new JSONObject(metaNode.getProperty("data").toString());

            /**
             * Let's verify the label 'TESTLABEL' has been removed from all nodes.
             * This is testing for adverse effects. There should be no changes in the
             * metadata node.
             */

            Predicate<Object> isTESTLABELPresent = l -> String.valueOf(l).equals("TESTLABEL");
            JSONObject nodesJSONObject = jsonObjectForDataProperty.get("nodes").toJSONObject();
            jsonObjectForDataProperty.get("nodes").toJSONObject().keys().forEachRemaining(key -> {
                assertTrue(
                        StreamSupport.stream(nodesJSONObject.get(key).get("labels").toJSONArray().spliterator(),
                                false)
                                .filter(isTESTLABELPresent)
                                .findFirst()
                                .isEmpty());
            });
        } catch (Exception e) {
            console.log("ERROR -> " + e.toString());
        }
    }

    @Test
    public void test_Add_Property_To_All_Nodes_With_Specific_Label() {
        console.log("********************************************************************************");
        console.log("***************************** NEW TEST *****************************************");
        console.log("**                                                                            **");
        console.log("** test_Add_Property_To_All_Nodes_With_Specific_Label()                       **");
        console.log("**                                                                            **");
        console.log("**                                                                            **");
        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();
        /**
         * Confirm test data was added
         */
        console.log("** Test data loaded.");
        printResultsToConsole(dbService);

        final String updateSql = "MATCH (n:TESTLABEL) SET n.color = 'blue' return n";

        console.log("** Add Property to All Nodes With LABEL 'TESTLABEL' ");
        console.log("** " + updateSql);

        /**
         * Update
         */
        try (Transaction tx = dbService.beginTx()) {
            tx.execute(updateSql);
            tx.commit();
        } catch (Exception e) {

        }

        /**
         * Confirm nodes have changed with new property.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(Metadata.GET_BLUEWAVE_METADATA_NODE_SQL);

            assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");
            JSONObject jsonObjectForDataProperty = new JSONObject(metaNode.getProperty("data").toString());

            /**
             * Let's verify the property 'color' has been added for the nodes containing
             * 'TESTLABEL' label.
             */

            Predicate<Object> isTESTLABELPresent = l -> String.valueOf(l).equals("TESTLABEL");
            Predicate<Object> isColorPropPresent = p -> String.valueOf(p).equals("color");

            JSONObject nodesJSONObject = jsonObjectForDataProperty.get("nodes").toJSONObject();
            nodesJSONObject.keySet().forEach(key -> {

                JSONArray labels = nodesJSONObject.get(key).get("labels").toJSONArray();
                boolean hasLabel = StreamSupport
                        .stream(labels.spliterator(), false)
                        .filter(isTESTLABELPresent)
                        .collect(Collectors.toList())
                        .size() > 0;

                JSONArray properties = nodesJSONObject.get(key).get("properties").toJSONArray();
                boolean hasColorProp = StreamSupport
                        .stream(properties.spliterator(), false)
                        .filter(isColorPropPresent)
                        .collect(Collectors.toList())
                        .size() > 0;

                if (hasLabel) {
                    assertTrue(hasColorProp);
                } else {
                    assertFalse(hasColorProp);
                }
            });

        } catch (Exception e) {
            console.log("ERROR: " + e);
        }
    }

    /**
     * Prints out the contents of bluewave_metadata node
     * 
     * @param dbService
     */
    private void printResultsToConsole(GraphDatabaseService dbService) {

        /**
         * Eyeball the results in the console and verify the bluewave_metadata node has
         * one node with one label.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(Metadata.GET_BLUEWAVE_METADATA_NODE_SQL);

            assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");

            console.log("** bluewave_metadata node contents: id: " + metaNode.getId()
                    + ", data property: " + metaNode.getProperties("data").toString() + "\n");

        } catch (Exception e) {
            console.log("ERROR -> " + e);
        }
    }

    @BeforeEach
    void initializeNeo4j() {

        /**
         * Load dummy data.
         * 
         */
        final String createSql = "CREATE (n1{prop1:'val1'})," // One node with one property
                + " (n2:TESTNODE{prop1:'val1', prop2:'val2'})," // One node with a label and 2 properties
                + " (n3:TESTLABEL{prop1:'val1', prop2:'val2', prop3:'val3'}), " // One node with a label and 3
                                                                                // properties
                + "(n4:TESTLABEL:TESTLABEL2{prop1:'val1', prop2:'val2', prop3:'val3', prop4:'val4'}) "; // One node with
                                                                                                        // 2 labels and
                                                                                                        // 4 properties
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .build();

        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();

        while (!dbService.isAvailable(500)) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (Throwable t) {
            }
        }

        try (Transaction tx = dbService.beginTx()) {
            tx.execute(createSql);
            tx.commit();
        } catch (Exception e) {

        }
    }

    @AfterEach
    void cleanDb() {
        /**
         * Wipe out all db data
         */
        console.log("** ");
        console.log("** ");
        console.log("** Final result:");
        printResultsToConsole(this.embeddedDatabaseServer.defaultDatabaseService());
        console.log("**                                                                            **");
        console.log("**                                                                            **");
        console.log("********************************************************************************");
        console.log("********************************************************************************");
        console.log("******** Ignore all errors below this statement, DB is in shutdown mode. *******\n\n");
        try (Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
        }

        this.driver.close();
        this.embeddedDatabaseServer.close();

        // Reset Metadata flag
        Metadata.metaNodeExist = false;
    }
}
