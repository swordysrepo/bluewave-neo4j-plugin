package bluewave.neo4j.plugins;

import static javaxt.utils.Console.console;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import javaxt.utils.Date;
/**
 * Use this cmd to run these tests:
 *  mvn -Dtest=bluewave.neo4j.plugins.PropertiesHandlerTest test
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PropertiesHandlerTest {

    public static final String GET_BLUEWAVE_METADATA_NODE_SQL = "MATCH (n:%s) return n";
    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private static Driver driver;
    private Neo4j embeddedDatabaseServer;

    @Test
    public void test_init() {
        console.log("********************************************************************************");
        console.log("***************************** NEW TEST *****************************************");
        console.log("**                                                                            **");
        console.log("** test_init()                                                                **");
        console.log("**                                                                            **");
        console.log("**                                                                            **");

        //
        // At this point, the data has been added.
        // Call init() which will call refresh() to query
        // the db and save the results to the metadata node in the db.
        //
        GraphDatabaseService dbService = embeddedDatabaseServer.defaultDatabaseService();
        // PropertiesHandler propertiesHandler = new PropertiesHandler(dbService);
        // propertiesHandler.init();

        try {
            console.log("Test sleep: " + new Date().toString("EEE MMM dd HH:mm:ss z yyyy"));
            TimeUnit.SECONDS.sleep(300);

        }catch(Exception e) {
          console.log(e);
        } finally {
            console.log("Test sleep finished. " + new Date().toString("EEE MMM dd HH:mm:ss z yyyy"));
        }

        //
        // Confirm test data was added
        //
        String formattedSql = String.format(GET_BLUEWAVE_METADATA_NODE_SQL, Metadata.META_NODE_LABEL);
        console.log("** Test data loaded.");
        console.log("** formattedSql \n" + formattedSql);
        printResultsToConsole(dbService, formattedSql);
    }


    /**
     * Prints out the contents of bluewave_metadata node
     *
     * @param dbService
     */
    private void printResultsToConsole(GraphDatabaseService dbService, String sql) {

        /**
         * Eyeball the results in the console and verify the bluewave_metadata node has
         * one node with one label.
         */
        try (Transaction tx = dbService.beginTx()) {
            Result result = tx.execute(sql);

           // assertTrue(result.hasNext());
            Map<String, Object> record = result.next();
            Node metaNode = (Node) record.get("n");

            console.log("** bluewave_metadata node contents: "
                    // + " --properties--:> " +
                    + metaNode.getProperties(PropertiesHandler.KEY_PROPERTIES).toString()
                    // + "\n --counts--:> " + metaNode.getProperties(KEY_COUNTS).toString()
                    + "\n");
            tx.close();
        } catch (Exception e) {
            fail("ERROR -> " + e);
        }
    }

    @BeforeEach
    void initializeNeo4j() {
        try {
            /**
             * Load test data.
             *
             */
            final String createSql = "CREATE (n1{prop1:'val1'})," // One node with one property
                    + " (n2:TESTNODE{prop1:'val1', prop2:'val2'})," // One node with a label and 2 properties
                    + " (n3:TESTLABEL{prop1:'val1', prop2:'val2', prop3:'val3'}), " // One node with a label and 3
                                                                                    // properties
                    + " (n4:TESTLABEL:TESTLABEL3:TESTNODE{prop1:'val33', prop2:'val2', prop3:'val3'}), "
                    + "(n5:TESTLABEL:TESTLABEL2{prop1:'val1', prop2:'val2', prop3:'val3', prop4:'val4'}) "; // One node
                                                                                                            // with
                                                                                                            // 2 labels
                                                                                                            // and
                                                                                                            // 4
                                                                                                            // properties
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
                console.log("Initial data fill success. ");
            } catch (Exception e) {
                e.printStackTrace();
                fail("Error: " + e);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            fail("Error: " + e);
        }
    }

    @AfterEach
    void cleanDb() {

        /**
         * Wipe out all db data
         */
        GraphDatabaseService db = this.embeddedDatabaseServer.defaultDatabaseService();
        console.log("** ");
        console.log("** ");
        console.log("** Final result:");
        String formattedSql = String.format(GET_BLUEWAVE_METADATA_NODE_SQL, Metadata.META_NODE_LABEL);
        printResultsToConsole(db, formattedSql);
        console.log("**                                                                            **");
        console.log("**                                                                            **");
        console.log("********************************************************************************");
        console.log("********************************************************************************");
        console.log("******** Ignore all errors below this statement, DB is in shutdown mode. *******");
        try (org.neo4j.driver.Transaction tx = driver.session().beginTransaction()) {
            tx.run("MATCH (n) DETACH DELETE n");
            tx.close();
        } catch (Exception e) {
            console.log("ERROR: " + e);
        } finally {
            console.log("*** TESTS COMPLETE ***");
            PropertiesHandlerTest.driver.close();
            this.embeddedDatabaseServer.close();
        }
    }
}
