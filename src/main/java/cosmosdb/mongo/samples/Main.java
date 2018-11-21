package cosmosdb.mongo.samples;

import cosmosdb.mongo.samples.runnables.InsertDocumentRunnable;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import cosmosdb.mongo.samples.sdkextensions.RuCharge;
import org.bson.Document;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

import static cosmosdb.mongo.samples.InsertionHelper.*;

public class Main {

    private static ConfigSettings configSettings=new ConfigSettings();
    private static MongoClientExtension mongoClientExtension;
    public static void main(final String[] args) throws InterruptedException, IOException, URISyntaxException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        configSettings.Init();
        InitMongoClient();
        ExecuteScenario();
    }

    public static void ExecuteScenario() throws URISyntaxException, InterruptedException, IOException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        Run();
    }

    private static void Run() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        java.lang.reflect.Method method;
        method = Main.class.getDeclaredMethod(configSettings.getScenario());
        method.invoke(null);

    }

    private static void InitMongoClient()
    {
        mongoClientExtension=
                new MongoClientExtension(
                        configSettings.getUserName(),
                        configSettings.getPassword(),
                        10255,
                        true,
                        configSettings.getClientThreadsCount()
                );
    }

    private static void  How_To_Ingest_Sample_Docs_In_Batch_Using_Execution_Service() throws URISyntaxException, InterruptedException {
        int batchSize=configSettings.getBatchSize();
        int numberOfBatches=configSettings.getNumberOfBatches();
        int sampleDocumentsCount=numberOfBatches*batchSize;

        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(
                sampleDocumentsCount,
                configSettings.getPartitionkey());
        System.out.println(
                "Inserting total documents: " + sampleDocumentsCount+
                ", with batch size: "+batchSize+
                " in "+numberOfBatches+" batches.");

        final long startTime = System.currentTimeMillis();
        InsertInBatchUsingExecutionService(
                        mongoClientExtension,
                        configSettings.getDbName(),
                        configSettings.getCollName(),
                        configSettings.getPartitionkey(),
                        sampleDocs,
                        batchSize);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Execution time in milli seconds: " + totalTime);
        System.out.println("Execution time in seconds: " + totalTime / 1000);

    }

    private static void How_To_Ingest_Sample_Docs_In_Batch_Using_Threads_And_Validate_Result() throws URISyntaxException, InterruptedException, IOException {
        int batchSize=configSettings.getBatchSize();
        int numberOfBatches=configSettings.getNumberOfBatches();
        int sampleDocumentsCount=numberOfBatches*batchSize;

        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(
                sampleDocumentsCount,
                configSettings.getPartitionkey());
        System.out.println(
                "Inserting total documents: " + sampleDocumentsCount+
                        ", with batch size: "+batchSize+
                        " in "+numberOfBatches+" batches.");
        List<InsertDocumentRunnable> tasks=InsertInBatch(
                mongoClientExtension,
                configSettings.getDbName(),
                configSettings.getCollName(),
                configSettings.getPartitionkey(),
                sampleDocs,
                batchSize);
        WaitUntilAllInsertionComplete(tasks);
        ValidateBatchModeResult(tasks,0);
    }

    private static void How_To_Ingest_Sample_Docs_Using_Insert_One_Using_Threads_And_Validate_Result() throws URISyntaxException, InterruptedException, IOException {
        int batchSize=configSettings.getBatchSize();
        int numberOfBatches=configSettings.getNumberOfBatches();
        int sampleDocumentsCount=numberOfBatches*batchSize;

        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(
                sampleDocumentsCount,
                configSettings.getPartitionkey());
        System.out.println(
                "Inserting total documents: " + sampleDocumentsCount);
        List<InsertDocumentRunnable> tasks=InsertOneInParallel(
                mongoClientExtension,
                configSettings.getDbName(),
                configSettings.getCollName(),
                configSettings.getPartitionkey(),
                sampleDocs);
        WaitUntilAllInsertionComplete(tasks);
        ValidateBatchModeResult(tasks,0);
    }

    private static void How_To_Ingest_Sample_Docs_Using_Insert_One_Using_Execution_Service()
            throws InterruptedException, URISyntaxException {
        int batchSize=configSettings.getBatchSize();
        int numberOfBatches=configSettings.getNumberOfBatches();
        int sampleDocumentsCount=numberOfBatches*batchSize;

        List<Document> sampleDocs=SampleDoc.GetSampleDocuments(
                sampleDocumentsCount,
                configSettings.getPartitionkey());
        System.out.println(
                "Inserting total documents: " + sampleDocumentsCount+
                        ", with batch size: "+batchSize+
                        " in "+numberOfBatches+" batches.");

        final long startTime = System.currentTimeMillis();
        InsertOneInParallelUsingExecutionService(
                mongoClientExtension,
                configSettings.getDbName(),
                configSettings.getCollName(),
                configSettings.getPartitionkey(),
                sampleDocs);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Execution time in milli seconds: " + totalTime);
        System.out.println("Execution time in seconds: " + totalTime / 1000);
    }

    private static void How_To_Find_One_Document()
    {
        Document d=  mongoClientExtension.FindOne(
                configSettings.getDbName(),
                configSettings.getCollName());
        System.out.println("First document is "+d);

    }

    private static void How_To_Create_Partitioned_Collection()
    {
        mongoClientExtension.CreatePartitionedCollection(
                configSettings.getDbName(),
                configSettings.getCollName(),
                configSettings.getPartitionkey());


    }

    private static void How_To_Generate_Sample_Document() throws URISyntaxException {
        HashMap<String, Object> sampleDoc=SampleDoc.Get();
        System.out.println("Sample document is: "+sampleDoc);
    }

    private static void How_To_Get_Last_Operation_Ru_Charge()
    {
        RuCharge latestOperationCharge=mongoClientExtension.GetLatestOperationRus(configSettings.getDbName());
        if(latestOperationCharge!=null) {
            System.out.println(
                    String.format(
                            "Command: %s, Charge: %s ",
                            latestOperationCharge.GetCommand(),
                            latestOperationCharge.GetRus()));
        }
    }

    private static void How_To_Check_Is_Collection_Exists()
    {
        boolean isExits= mongoClientExtension.IsCollectionExists(
                configSettings.getDbName(),
                configSettings.getCollName());
        if(isExits) {
            System.out.println("Collection: " + configSettings.getCollName() +
                    " found in Database: " + configSettings.getDbName());
        }
        else
        {
            System.out.println("Collection: " + configSettings.getCollName() +
                    " not found in Database: " + configSettings.getDbName());
        }
    }

    private static void How_To_Update_Rus()
    {
        mongoClientExtension.UpdateRus(
                configSettings.getDbName(),
                configSettings.getCollName(),
                configSettings.getRus());
    }

    private static void How_To_How_Many_Rus_Provisioned()
    {
        mongoClientExtension.ShowRus(
                configSettings.getDbName(),
                configSettings.getCollName());
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
                Thread.sleep(1);
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("Execution time in milli seconds: " + totalTime);
        System.out.println("Execution time in seconds: " + totalTime / 1000);
    }

}
