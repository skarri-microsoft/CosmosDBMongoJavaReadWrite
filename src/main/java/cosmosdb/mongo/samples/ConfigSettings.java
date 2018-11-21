package cosmosdb.mongo.samples;

import java.io.*;
import java.net.URISyntaxException;
import java.util.Properties;

public class ConfigSettings {

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCollName() {
        return collName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getNumberOfBatches() {
        return numberOfBatches;
    }

    public String getPartitionkey() {
        return partitionkey;
    }

    public int getClientThreadsCount() {
        return clientThreadsCount;
    }

    public int getClientPoolSize() {
        return clientPoolSize;
    }

    public int getRus() {
        return rus;
    }

    public String getScenario()
    {
        return scenario;
    }

    public void Init() throws IOException, URISyntaxException {
        Properties props = new Properties();
        InputStream in;

            in = new FileInputStream(new File(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent()+"\\config.properties");
            props.load(in);

            //read and populate properties
            userName = props.getProperty("userName");
            password = props.getProperty("password");
            dbName=props.getProperty("dbName");
            collName=props.getProperty("collName");
            batchSize=Integer.parseInt(props.getProperty("batchSize"));
            numberOfBatches=Integer.parseInt(props.getProperty("numberOfBatches"));
            partitionkey=props.getProperty("partitionkey");
            clientThreadsCount=Integer.parseInt(props.getProperty("clientThreadsCount"));
            clientPoolSize=Integer.parseInt(props.getProperty("clientPoolSize"));
            scenario=props.getProperty("scenario");
            rus=Integer.parseInt(props.getProperty("rus"));
            in.close();

    }

    private String userName;
    private String password;
    private String dbName;
    private String collName;
    private int batchSize;
    private int numberOfBatches;
    private String partitionkey;
    private int clientThreadsCount;
    private int clientPoolSize;
    private String scenario;
    private int rus;
}
