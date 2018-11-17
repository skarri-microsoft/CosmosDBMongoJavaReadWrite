package cosmosdb.mongo.samples.sdkextensions;

public class RuCharge {
    private String command;
    private double charge;

    public RuCharge(String command, double charge)
    {
        this.command=command;
        this.charge=charge;
    }

    public double GetRus()
    {
        return this.charge;
    }

    public String GetCommand()
    {
        return this.command;
    }
}
