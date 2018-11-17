package cosmosdb.mongo.samples.runnables;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import cosmosdb.mongo.samples.ErrorHandler;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class InsertDocumentRunnable implements Runnable  {

    private String pkey;
    private long id;
    private List<Document> sourceDocuments;
    private Document docToInsert;
    private List<String> ErrorMessages=new ArrayList<>();
    private int defaultRetriesForThrottles=3;
    private String dbName;
    private String collectionName;
    private MongoClientExtension mongoClientExtension;
    private boolean isBatch;
    private int MaxWaitInMilliSeconds=500;
    private int MinWaitInMilliSeconds=100;
    private boolean isThrottled=false;
    private boolean isSucceeded=false;
    private boolean isRunning=true;



    public InsertDocumentRunnable(
            MongoClientExtension mongoClientExtension,
            List<Document> documents,
            String dbName,
            String collectionName,
            String pkey,
            Boolean isBatch) {
        this.mongoClientExtension=mongoClientExtension;
        this.sourceDocuments=documents;
        this.dbName=dbName;
        this.collectionName=collectionName;
        this.pkey=pkey;
        this.isBatch=isBatch;
    }

    public InsertDocumentRunnable(
            MongoClientExtension mongoClientExtension,
            Document docToInsert,
            String dbName,
            String collectionName,
            String pkey) {
        this.mongoClientExtension=mongoClientExtension;
        this.docToInsert=docToInsert;
        this.dbName=dbName;
        this.collectionName=collectionName;
        this.pkey=pkey;
        this.isBatch=false;
    }

    public List<Document> GetProcessedDocument()
    {
        return this.sourceDocuments;
    }

    public Document GetDocToInsert() {

        return docToInsert;
    }

    public boolean IsRunning()
    {
        return this.isRunning;
    }

    public boolean GetIsSucceeded()
    {
        return this.isSucceeded;
    }

    public List<String> GetErrorMessages()
    {
        return  this.ErrorMessages;
    }
    private void ExecuteBulkInsert(List<Document> docsToInsert) throws InterruptedException {
        int throttleRetries=0;

        int docsCount=docsToInsert.size();
        List<InsertOneModel<Document>> docs = new ArrayList<InsertOneModel<Document>>(docsCount);
        for (int i=0;i<docsCount;i++) {
            docs.add(new InsertOneModel<Document>(docsToInsert.get(i)));
        }
        while(throttleRetries<defaultRetriesForThrottles)
        {
            try
            {
                this.mongoClientExtension.BulkWrite(docs,new BulkWriteOptions().ordered(false),this.dbName,this.collectionName);
                isSucceeded=true;
                break;
            }
            catch (MongoCommandException mongoCommandException)
            {
                if(ErrorHandler.IsThrottle(mongoCommandException))
                {
                    throttleRetries++;
                    isThrottled=true;

                }
                else
                {
                    ErrorMessages.add(mongoCommandException.toString());
                    break;
                }
            }
            catch (Exception ex)
            {
                if(ErrorHandler.IsThrottle(ex.getMessage()))
                {
                    throttleRetries++;
                    isThrottled=true;
                }
                else
                {
                    ErrorMessages.add(ex.getMessage());
                    break;
                }
            }
            if(isThrottled)
            {
                Thread.sleep(new Random().nextInt(MaxWaitInMilliSeconds - MinWaitInMilliSeconds + 1) + MinWaitInMilliSeconds);
            }
        }
    }

    private void ExecuteInsertOne(Document docToInsert) throws InterruptedException {
        int throttleRetries=0;

        while(throttleRetries<defaultRetriesForThrottles)
        {
            try
            {
                this.mongoClientExtension.InsertOne(this.dbName,this.collectionName,docToInsert);
                isSucceeded=true;
                break;
            }
            catch (MongoCommandException mongoCommandException)
            {
                if(ErrorHandler.IsThrottle(mongoCommandException))
                {
                    throttleRetries++;
                    isThrottled=true;

                }
                else
                {
                    ErrorMessages.add(mongoCommandException.toString());
                    break;
                }
            }
            catch (Exception ex)
            {
                if(ErrorHandler.IsThrottle(ex.getMessage()))
                {
                    throttleRetries++;
                    isThrottled=true;
                }
                else
                {
                    ErrorMessages.add(ex.getMessage());
                    break;
                }
            }
            if(isThrottled)
            {
                Thread.sleep(new Random().nextInt(MaxWaitInMilliSeconds - MinWaitInMilliSeconds + 1) + MinWaitInMilliSeconds);
            }
        }
    }

    public void run() {

        this.id = Thread.currentThread().getId();
        if(isBatch) {
            try {
                ExecuteBulkInsert(this.sourceDocuments);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else
        {
            try {
                ExecuteInsertOne(this.docToInsert);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.isRunning=false;
    }


}
