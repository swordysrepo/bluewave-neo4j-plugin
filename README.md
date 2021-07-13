# Transaction Plugin for Project BlueWave

Custom transaction event listener for Neo4J. Implemented as a Neo4J plugin.
Logs transaction information in a local H2 database and can be configured to
send information to REST endpoint (e.g. BlueWave server).



# Config.json
The plugin requires a config.json file to start. At a minimum, the config.json
file should include connection information to the neo4j database.

```javascript
{
    "database" : {
        "driver" : "H2",
        "path" : "data/database"
    },

    "graph" : {
        "host" : "localhost:7687",
        "username" : "neo4j",
        "password" : "password"
    },

    "webserver" : {
        "host" : "localhost:8080",
        "username" : "neo4j",
        "password" : "password"
    }
}
```




