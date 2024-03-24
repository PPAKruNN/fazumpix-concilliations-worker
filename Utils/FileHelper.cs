
namespace ConcilliationWorker.Utils;
public class FileHelper(string concilliationId)
{
    private readonly string _concilliationId = concilliationId;
    private readonly string outDir = "./temp/";
    private const string finalAssemblyDir = "./temp/out/";
    private const string databaseToFile = "databaseToFile";
    private const string fileToDatabase = "fileToDatabase";
    private const string differentStatus = "differentStatus";
    private const string ok = "ok";
    private const string assembly = "out";

    public string DatabaseToFileFilename => $"{outDir}{_concilliationId}-{databaseToFile}.json";
    public string FileToDatabaseFilename => $"{outDir}{_concilliationId}-{fileToDatabase}.json";
    public string DifferentStatusFilename => $"{outDir}{_concilliationId}-{differentStatus}.json";
    public string OkFilename => $"{outDir}{_concilliationId}-{ok}.json";
    public string AssemblyFilename => $"{finalAssemblyDir}{_concilliationId}-{assembly}.json";

    public void CleanTempFiles()
    {
        File.Delete(AssemblyFilename);
        File.Delete(FileToDatabaseFilename);
        File.Delete(OkFilename);
        File.Delete(DatabaseToFileFilename);
        File.Delete(DifferentStatusFilename);
    }
}