package cosmosdb.mongo.samples;

import cosmosdb.mongo.samples.runnables.InsertDocumentRunnable;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import cosmosdb.mongo.samples.sdkextensions.RuCharge;
import org.bson.Document;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Main {

    public static void main(final String[] args) throws InterruptedException, IOException {

       HowToFunctions();
        // SkipLimitValidation();


    }

    private static void SkipLimitValidation() throws IOException {
        String dbName="dms";
        String collectionName="Devices2";
        String partitionKey="Name";
        int batchSize=24;
        int numberOfThreads=25;
        int sampleDocumentsCount=numberOfThreads*batchSize;
        MongoClientExtension mongoClientExtension=
                new MongoClientExtension(
                        "cifxdb-dev",
                        "jhulcwO3myWG3YpwGw7zVOB7OqEqezARIg2ht0o74LljHRyng26rkCfiNbkVjVuKCgwRGQ7c0yVnnR6dHGNAsA==",
                        10255,
                        true,
                        10
                );

        ArrayList<Document> findDocs=mongoClientExtension.Find(dbName,collectionName);
        ArrayList<String> findMethodNames=new ArrayList<>();

        String findQueryResult="FindQuery.txt";
        File findQueryFile = new File(findQueryResult);
        FileWriter findQueryWriter = new FileWriter(findQueryFile);
        for(int i=0;i<findDocs.size();i++)
        {
            findQueryWriter.write(findDocs.get(i).getString("Name"));
            findQueryWriter.write(System.lineSeparator());
            findMethodNames.add(findDocs.get(i).getString("Name"));
        }
        findQueryWriter.close();

        ArrayList<Document> findSkipLimitDocs=new ArrayList<>();
        for(int j=0;j<100;j=j+10)
        {
            findSkipLimitDocs.addAll(mongoClientExtension.FindSkipLimit(dbName,collectionName,j,11));
        }

        ArrayList<String> findSkipLimitNames=new ArrayList<>();

        String findSkipQueryResult="FindSkipQuery.txt";
        File findSkipQueryFile = new File(findSkipQueryResult);
        FileWriter findSkipQueryWriter = new FileWriter(findSkipQueryFile);
        for(int i=0;i<findSkipLimitDocs.size();i++)
        {
            findSkipQueryWriter.write(findSkipLimitDocs.get(i).getString("Name"));
            findSkipQueryWriter.write(System.lineSeparator());
            findSkipLimitNames.add(findSkipLimitDocs.get(i).getString("Name"));
        }
        findSkipQueryWriter.close();

        System.out.println("Total Find method documents :"+findMethodNames.size());

        System.out.println("Total Find skip and limit method documents :"+findSkipLimitNames.size());

        for(int k=0;k<findMethodNames.size();k++)
        {
            if(!findSkipLimitNames.contains(findMethodNames.get(k)))
            {
                System.out.println("Missing name in find skip limit operation : "+findMethodNames.get(k));
            }
        }



    }

    private static void HowToFunctions() throws IOException, InterruptedException {
        String dbName="arrow";
        String collectionName="test";
        String partitionKey="facebookId";
        int batchSize=4;
        int numberOfThreads=25;
        int sampleDocumentsCount=numberOfThreads*batchSize;
        MongoClientExtension mongoClientExtension=
                new MongoClientExtension(
                        "samstest",
                        "AVScP9Hzdi4PMsMBIZ0Sr9KDuurpxbKNTA46BWT1llOpA7yYAAb8suG7dUUArMPpUp93k3ObhDrzXcYzRdcN2A==",
                        10255,
                        true,
                        numberOfThreads
                );

        //HowToCreatePartitionedCollection(mongoClientExtension,dbName,collectionName,partitionKey);

        //HowToGenerateSampleDoc();

        //HowToGetLastOperationRuCharge(mongoClientExtension,dbName);

        //HowToFineOneDocument(mongoClientExtension,dbName,collectionName);

        //TestD40MBLimit(mongoClientExtension,dbName,collectionName);


        InsertSampleDocs2(
                mongoClientExtension,
                dbName,
                collectionName,
                partitionKey,
                sampleDocumentsCount,
                batchSize);
    }
    private static void InsertSampleDocs(MongoClientExtension mongoClientExtension,
                                    String dbName,
                                    String collectionName,
                                    String partitionKey,
                                    int sampleDocumentsCount,
                                    int batchSize) throws IOException, InterruptedException {
        if(!mongoClientExtension.IsCollectionExists(dbName,collectionName))
        {
            mongoClientExtension.CreatePartitionedCollection(dbName,collectionName,partitionKey);
        }

        // Each execution is roughly 2 mb, let's insert 40 mb data approx.
        for(int i=0;i<1;i++) {
            HowtoInsertDocumentsUsingBatchWrite(
                    mongoClientExtension,
                    dbName,
                    collectionName,
                    partitionKey,
                    sampleDocumentsCount,
                    batchSize,
                    i);

        }
    }

    private static void InsertSampleDocs2(MongoClientExtension mongoClientExtension,
                                         String dbName,
                                         String collectionName,
                                         String partitionKey,
                                         int sampleDocumentsCount,
                                         int batchSize) throws IOException, InterruptedException {
        if(!mongoClientExtension.IsCollectionExists(dbName,collectionName))
        {
            mongoClientExtension.CreatePartitionedCollection(dbName,collectionName,partitionKey);
        }

        // Each execution is roughly 2 mb, let's insert 40 mb data approx.
        for(int i=0;i<1;i++) {
            HowtoInsertDocumentsUsingInsertOne(
                    mongoClientExtension,
                    dbName,
                    collectionName,
                    partitionKey,
                    sampleDocumentsCount,
                    batchSize,
                    i);

        }
    }


    private static void TestD40MBLimit(MongoClientExtension mongoClientExtension, String dbName, String collectionName)
    {
        mongoClientExtension.Find(dbName,collectionName);
        mongoClientExtension.GetDocumentsCount(dbName,collectionName);
    }

    private static void HowToFineOneDocument(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName)
    {
        Document d=  mongoClientExtension.FindOne(dbName,collectionName);
        System.out.println("First document is "+d);

    }

    private static void HowToCreatePartitionedCollection(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String newCollectionName,
            String partitionKey)
    {
        mongoClientExtension.CreatePartitionedCollection(dbName,newCollectionName,partitionKey);


    }

    private static void HowToGenerateSampleDoc()
    {
        HashMap<String, Object> sampleDoc=SampleDoc.Get();
        System.out.println("Sample document player id is "+sampleDoc.get("playerId"));
    }

    private static void HowtoInsertDocumentsUsingBatchWrite(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            int sampleDocumentsCount,
            int batchSize,
            int runId) throws InterruptedException, IOException {
        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(sampleDocumentsCount,partitionKey);
        List<InsertDocumentRunnable> tasks=
                InsertionHelper.InsertInBatch(
                        mongoClientExtension,
                        dbName,
                        collectionName,
                        partitionKey,
                        sampleDocs,
                        batchSize);
        System.out.println("Inserting documents of size: " + sampleDocumentsCount);
        WaitUntilAllInsertionComplete(tasks);
        //ValidateBatchModeResult(tasks,runId);
    }

    private static void HowtoInsertDocumentsUsingInsertOne(
            MongoClientExtension mongoClientExtension,
            String dbName,
            String collectionName,
            String partitionKey,
            int sampleDocumentsCount,
            int batchSize,
            int runId) throws InterruptedException, IOException {
        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(sampleDocumentsCount,partitionKey);
        List<InsertDocumentRunnable> tasks=
                InsertionHelper.InsertOneInParallel(
                        mongoClientExtension,
                        dbName,
                        collectionName,
                        partitionKey,
                        sampleDocs);
        System.out.println("Inserting documents of size: " + sampleDocumentsCount);
        WaitUntilAllInsertionComplete(tasks);
        //ValidateBatchModeResult(tasks,runId);
    }

    private static void ValidateBatchModeResult(
            List<InsertDocumentRunnable> tasks,
            int runId) throws IOException {
        boolean anyFailures=false;
        int noOfBatchesFailed=0;
        for(int i=0;i<tasks.size();i++)
        {
            if(!tasks.get(i).GetIsSucceeded())
            {
                noOfBatchesFailed++;
                anyFailures=true;
            }
        }
        if(anyFailures)
        {
            System.out.println("There are failures while inserting the documents");
            String fileName=String.format("Errors%d.txt",runId);
            File file = new File(fileName);
            FileWriter writer = new FileWriter(file);

            writer.write("===Summary===");
            writer.write(System.lineSeparator());
            writer.write("Number of batches failed : "+noOfBatchesFailed);
            writer.write(System.lineSeparator());
            writer.write("===End summary===");
            writer.write(System.lineSeparator());
            for(int i=0;i<tasks.size();i++)
            {
                if(!tasks.get(i).GetIsSucceeded())
                {
                    writer.write("===Start of failed batch data===");
                    writer.write(System.lineSeparator());
                    List<Document> failedDocs=tasks.get(i).GetProcessedDocument();
                    for(int j=0;j<failedDocs.size();j++)
                    {
                        writer.write(failedDocs.get(j).toJson());
                        writer.write(System.lineSeparator());
                    }
                    writer.write(System.lineSeparator());
                    writer.write("===Errors===");
                    writer.write(System.lineSeparator());
                    List<String> failedDocsError=tasks.get(i).GetErrorMessages();
                    for(int k=0;k<failedDocsError.size();k++)
                    {
                        writer.write(failedDocsError.get(k));
                        writer.write(System.lineSeparator());
                    }
                    writer.write(System.lineSeparator());
                    writer.write("===Errors End===");
                    writer.write(System.lineSeparator());
                    writer.write("===End of failed batch data===");
                    writer.close();
                }
            }
            System.out.println("Failed Document and errors available in file: "+fileName);
        }
        else
        {
            System.out.println("All documents inserted successfully.");
        }
    }

    private static void WaitUntilAllInsertionComplete(List<InsertDocumentRunnable> tasks) throws InterruptedException {
        boolean isCompleted=false;
        final long startTime = System.currentTimeMillis();
        while(!isCompleted)
        {
            isCompleted=true;
            for(int i=0;i<tasks.size();i++)
            {
                if(tasks.get(i).IsRunning())
                {
                    isCompleted=false;
                }
            }
            if(isCompleted)
            {
                Thread.sleep(5000);
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Execution time in seconds: " + totalTime / 1000);
    }

    private static void HowToGetLastOperationRuCharge(MongoClientExtension mongoClientExtension,String dbName)
    {
        RuCharge latestOperationCharge=mongoClientExtension.GetLatestOperationRus(dbName);
        if(latestOperationCharge!=null) {
            System.out.println(
                    String.format(
                            "Command: %s, Charge: %s ",
                            latestOperationCharge.GetCommand(),
                            latestOperationCharge.GetRus()));
        }
    }
}
