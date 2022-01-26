package bluewave.neo4j.plugins;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public class Neo4JTransactionEventListenerExtensionFactory
        extends ExtensionFactory<Neo4JTransactionEventListenerExtensionFactory.Dependencies> {

    @Override
    public Lifecycle newInstance(final ExtensionContext extensionContext,
            final Dependencies dependencies) {
        final GraphDatabaseAPI db = dependencies.graphdatabaseAPI();
        final LogService log = dependencies.log();
        final DatabaseManagementService databaseManagementService =
                dependencies.databaseManagementService();
        final AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
        final Metadata meta = new Metadata(db);

        availabilityGuard.addListener(new AvailabilityListener() {

            @Override
            public void available() {

                if (dependencies.graphdatabaseAPI().databaseName().compareTo("system") != 0) {

                    Thread metaThread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            meta.init();
                        }
                    });

                    try {
                        metaThread.start();
                        metaThread.join();
                        meta.startTimer();
                    } catch (Exception e) {
                    }
                }
            }

            @Override
            public void unavailable() {
                if (dependencies.graphdatabaseAPI().databaseName().compareTo("system") != 0) {

                    if (meta != null) {
                        meta.cancelTimer();
                    }
                }
            }

        });
        return new CustomGraphDatabaseLifecycle(log, db, dependencies, databaseManagementService,
                meta);
    }

    interface Dependencies {
        GraphDatabaseAPI graphdatabaseAPI();

        DatabaseManagementService databaseManagementService();

        AvailabilityGuard availabilityGuard();

        LogService log();
    }

    public static class CustomGraphDatabaseLifecycle extends LifecycleAdapter {
        private final GraphDatabaseAPI db;
        private LogService log;
        private Neo4JTransactionEventListener transactionEventhandler;
        private final DatabaseManagementService databaseManagementService;
        private Metadata metadata;

        public CustomGraphDatabaseLifecycle(final LogService log, final GraphDatabaseAPI db,
                final Dependencies dependencies,
                final DatabaseManagementService databaseManagementService, Metadata metadata) {
            this.db = db;
            this.databaseManagementService = databaseManagementService;
            this.log = log;
            this.metadata = metadata;
        }

        @Override
        public void start() {
            if (this.db.databaseName().compareTo("system") != 0) {
                this.transactionEventhandler =
                        new Neo4JTransactionEventListener(this.db, log, metadata);
                this.databaseManagementService.registerTransactionEventListener(
                        this.db.databaseName(), this.transactionEventhandler);
            }
        }

        @Override
        public void shutdown() {
            this.databaseManagementService.unregisterTransactionEventListener(
                    this.db.databaseName(), this.transactionEventhandler);
        }
    }

    public Neo4JTransactionEventListenerExtensionFactory() {
        super(ExtensionType.DATABASE, "neo4JTransactionEventHandler");

    }
}
