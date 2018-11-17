package cosmosdb.mongo.samples;

import com.mongodb.MongoCommandException;

public class ErrorHandler {

    public static boolean IsThrottle(MongoCommandException ex)
    {
        if(ex.getCode() == 16500)
        {
            return true;
        }
        return false;
    }

    public static boolean IsThrottle(String strException)
    {
        return strException.toLowerCase().contains("Request rate is large".toLowerCase());
    }
}
