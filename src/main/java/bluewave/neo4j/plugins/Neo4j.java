package bluewave.neo4j.plugins;
import java.util.logging.Level;
import org.neo4j.driver.*;
import static org.neo4j.driver.SessionConfig.builder;

public class Neo4j implements AutoCloseable {
    private Driver driver;
    private static int port = 7687;
    private static String host;
    private String username;
    private String password;
    private String database;


    //**************************************************************************
    //** Constructor
    //**************************************************************************
    public Neo4j(){}


    //**************************************************************************
    //** setUsername
    //**************************************************************************
    public void setUsername(String username){
        if (driver!=null) driver = null;
        this.username = username;
    }


    //**************************************************************************
    //** setPassword
    //**************************************************************************
    public void setPassword(String password){
        if (driver!=null) driver = null;
        this.password = password;
    }


    //**************************************************************************
    //** setHost
    //**************************************************************************
    public void setHost(String host){
        if (driver!=null) driver = null;
        if (host.contains(":")){
            String[] arr = host.split(":");
            this.host = arr[0];
            this.port = Integer.parseInt(arr[1]);
        }
        else{
            this.host = host;
        }
    }

    public void setDatabase(String name) {
        database = name;
    }


    //**************************************************************************
    //** getSession
    //**************************************************************************
    public Session getSession(){
        return getSession(true);
    }


    //**************************************************************************
    //** getSession
    //**************************************************************************
    public Session getSession(boolean readOnly){

        if(database ==null) {
            if (readOnly) {
                return getDriver().session();
            } else {
                return getDriver().session(builder().withDefaultAccessMode(AccessMode.WRITE).build());
            }
        }
        else {
            if (readOnly) {
                return getDriver().session(builder().withDatabase(database).build());
            }
            else {
                return getDriver().session( builder().withDatabase(database).withDefaultAccessMode( AccessMode.WRITE ).build());
            }
        }
    }


    //**************************************************************************
    //** getDriver
    //**************************************************************************
    private Driver getDriver(){
        if (driver==null){
            driver = GraphDatabase.driver(
                    "bolt://" + host + ":" + port,
                    AuthTokens.basic( username, password ),
                    Config.builder().withLogging(Logging.javaUtilLogging(Level.SEVERE)).build()
            );
        }
        return driver;
    }


    //**************************************************************************
    //** close
    //**************************************************************************
    @Override
    public void close() throws Exception{
        if (driver!=null) driver.close();
    }

}