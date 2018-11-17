package cosmosdb.mongo.samples.sdkextensions;

import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class MongoClientExtension {

    private String userName;
    private String password;
    private int port;
    private MongoClient mongoClient;
    private Boolean isSslEnabled;
    private int clientThreadsCount;

    public MongoClientExtension(
            String userName,
            String password,
            int port,
            Boolean isSslEnabled,
            int clientThreadsCount)
    {
        this.userName=userName;
        this.password=password;
        this.port=port;
        this.isSslEnabled=isSslEnabled;
        this.clientThreadsCount=clientThreadsCount;
        InitClient();
    }

    public String GetMongoEndpoint()
    {
        return  String.format(
                "mongodb://%s:%s@%s.documents.azure.com:%d/?ssl=true&replicaSet=globaldb",
                this.userName,
                this.password,
                this.userName,
                this.port);
    }
    public String GetMongoHost()
    {
        return  String.format(
                "%s.documents.azure.com",
                this.userName);
    }


    public void InitClient()
    {
        MongoClientOptions clientOptions = MongoClientOptions.builder()
                .connectionsPerHost(this.clientThreadsCount)
                .sslEnabled(isSslEnabled)
                .build();

        MongoCredential cred =
                MongoCredential.createCredential(
                        this.userName,
                        "globaldb",
                        this.password.toCharArray());
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        credentials.add(cred);

        ServerAddress serverAddress= new ServerAddress(this.GetMongoHost(), this.port);
        this.mongoClient = new MongoClient(serverAddress, credentials, clientOptions);
    }

    public Document FindOne(String dbName,String CollectionName)
    {
        return this.mongoClient.getDatabase(dbName).getCollection(CollectionName).find().first();
    }

    public RuCharge GetLatestOperationRus(String dbName)
    {
        BsonDocument cmd = new BsonDocument();
        cmd.append("getLastRequestStatistics", new BsonInt32(1));
        Document requestChargeResponse = this.mongoClient.getDatabase(dbName).runCommand(cmd);
        if(requestChargeResponse.containsKey("RequestCharge"))
        {
            return new RuCharge(
                    requestChargeResponse.getString("CommandName"),
                    requestChargeResponse.getDouble("RequestCharge"));
        }
        return null;
    }

    public void CreatePartitionedCollection(String dbName, String collectionName, String partitionKey)
    {
        System.out.println(
                String.format(
                        "Creating partitioned collection: %s in db:%s with partition key: %s",
                       collectionName,
                        dbName,
                        partitionKey));

        BsonDocument cmd = new BsonDocument();
        String fullCollName = dbName+"."+collectionName;
        cmd.append("shardCollection", new BsonString(fullCollName));
        BsonDocument keyDoc = new BsonDocument();
        keyDoc.append(partitionKey, new BsonString("hashed"));
        cmd.append("key", keyDoc);
        this.mongoClient.getDatabase(dbName).runCommand(cmd);
    }

    public void BulkWrite(
            List<InsertOneModel<Document>> docs,
            BulkWriteOptions bulkWriteOptions,
            String dbName,
            String collectionName)
    {
        this.mongoClient.getDatabase(dbName).getCollection(collectionName).bulkWrite(docs,bulkWriteOptions);
    }

    public void InsertOne(String dbName,String collectionName,Document docToInsert)
    {
        this.mongoClient.
                getDatabase(dbName).
                getCollection(collectionName).
                insertOne(docToInsert);
    }

    public boolean IsCollectionExists(String dbName, String collectionName)
    {
        MongoIterable<String> colls= this.mongoClient.getDatabase(dbName).listCollectionNames();
        for (String s : colls) {

            if(s.equals(collectionName))
            {
                return true;
            }
        }
        return  false;
    }

    public ArrayList<Document> Find(String dbName,String collectionName)
    {
        FindIterable<Document> documents = this.mongoClient.getDatabase(dbName).getCollection(collectionName).find();

        return Lists.newArrayList(documents);

    }

    public ArrayList<Document> FindSkipLimit(String dbName,String collectionName,int skip,int limit)
    {
        FindIterable<Document> documents =
                this.mongoClient.getDatabase(dbName).getCollection(collectionName).
                        find().skip(skip).limit(limit);

        return Lists.newArrayList(documents);

    }

    public void GetDocumentsCount(String dbName,String collectionName)
    {
//        List<BsonDocument> docList = new ArrayList<BsonDocument>();
//
//        Document matchCourse = new Document("$match",
//                new Document("ci", Integer.parseInt(courseid)));
        //docList.add("Document{{$match=Document{{JE_VAT_CODE=Document{{$ne=0}}}}}}") ;

        //http://mongodb.github.io/mongo-java-driver/3.4/driver/tutorials/aggregation/
        //https://stackoverflow.com/questions/38202897/mongo-aggregation-in-java-group-with-multiple-fields
        List<Bson> filters = new ArrayList<>();
        this.mongoClient.getDatabase(dbName).getCollection(collectionName).aggregate(
                Arrays.asList(
                        Aggregates.match(new BsonDocument() {})
//                        Aggregates.group("_id",
//                                Accumulators.sum("JE_GL_ACCOUNT_NUMBER360", 1)
//
//                                )

                )
        ).allowDiskUse(true).forEach(printBlock);
    }

    Block<Document> printBlock = new Block<Document>() {
        @Override
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };
}
