package cosmosdb.mongo.samples;

import com.google.common.collect.Lists;
import cosmosdb.mongo.samples.runnables.InsertDocumentRunnable;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InsertionHelper {

    public static List<InsertDocumentRunnable> InsertInBatch(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            List<Document> docs,
            int batchSize)
    {

        List<List<Document>> batches = Lists.partition(docs, batchSize);
        int numberOfThreads=batches.size();
        List<InsertDocumentRunnable> threads=new ArrayList<InsertDocumentRunnable>(numberOfThreads);
        for(int i=0;i<numberOfThreads;i++)
        {
            InsertDocumentRunnable insertDocumentRunnable=new InsertDocumentRunnable(
                    mongoClientExtension,
                    batches.get(i),
                    dbName,
                    collectionName,
                    partitionKey,
                    true);
            Thread t = new Thread(insertDocumentRunnable);
            threads.add(insertDocumentRunnable);
            t.start();
        }

        return threads;
    }

    public static void InsertInBatchUsingExecutionService(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            List<Document> docs,
            int batchSize) throws InterruptedException {

        List<List<Document>> batches = Lists.partition(docs, batchSize);
        int numberOfThreads=batches.size();
        ExecutorService es = Executors.newCachedThreadPool();
        for(int i=0;i<numberOfThreads;i++)
        {
            InsertDocumentRunnable insertDocumentRunnable=new InsertDocumentRunnable(
                    mongoClientExtension,
                    batches.get(i),
                    dbName,
                    collectionName,
                    partitionKey,
                    true);
            es.execute(insertDocumentRunnable);
        }

        es.shutdown();
        es.awaitTermination(5, TimeUnit.MINUTES);
    }

    public static void InsertOneInParallelUsingExecutionService(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            List<Document> docs) throws InterruptedException {

        int numberOfThreads=docs.size();
        ExecutorService es = Executors.newCachedThreadPool();
        for(int i=0;i<numberOfThreads;i++)
        {
            InsertDocumentRunnable insertDocumentRunnable= new InsertDocumentRunnable(
                    mongoClientExtension,
                    docs.get(i),
                    dbName,
                    collectionName,
                    partitionKey);
            es.execute(insertDocumentRunnable);

        }
        es.shutdown();
        es.awaitTermination(5, TimeUnit.MINUTES);

    }

    public static List<InsertDocumentRunnable> InsertOneInParallel(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            List<Document> docs)
    {

        int numberOfThreads=docs.size();
        List<InsertDocumentRunnable> threads=new ArrayList<InsertDocumentRunnable>(numberOfThreads);
        for(int i=0;i<numberOfThreads;i++)
        {
            InsertDocumentRunnable insertDocumentRunnable= new InsertDocumentRunnable(
                    mongoClientExtension,
                    docs.get(i),
                    dbName,
                    collectionName,
                    partitionKey);
            Thread t = new Thread(insertDocumentRunnable);
            threads.add(insertDocumentRunnable);
            t.start();
        }

        return threads;
    }
}
