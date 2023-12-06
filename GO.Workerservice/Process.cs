namespace GO.Workerservice;

public class Process 
{
    private readonly DatabaseService _databaseService;
    private readonly Configuration _configuration;
    private readonly ILogger<Process> _logger;

    public Process(Configuration configuration, DatabaseService databaseService, ILogger<Process> logger) 
    {
        _configuration = configuration;
        _databaseService = databaseService;
        _logger = logger;
    }

    public async Task ProcessPackageAsync(ScaleDimensionerResult scan)
    {
        await _databaseService.Begin();

        try
        {
            var orderData = await _databaseService.GetOrderAsync(scan);

            if (orderData == null)
                return;


            var scanExists = await _databaseService.ScanExistsAsync(scan);

            if (!scanExists)
            {
                await _databaseService.AddScanAsync(orderData, scan);
            }


            var totalWeight = await _databaseService.GetTotalWeightAsync(scan);

            _logger.LogInformation("Committing database transaction");
            await _databaseService.Commit();
        }
        catch
        {
            await _databaseService.Rollback();
            _logger.LogWarning("Transaction has been rolled back, no changes were made to the database");
            throw;
        }

            //var scanLocation = packageData.df_ndl;
            //var date = packageData.df_datauftannahme;
            //var orderNumber = packageData.df_lfdnrauftrag;

            //if (scanData == null)
            //{
            //    await _databaseService.AddScanAsync(scan, packageData); // 3

            //    var realWeight = await _databaseService.GetWeightAsync(); // 4

            //    if (realWeight == null) return;

            //    await _databaseService.UpdateWeightAsync((int) realWeight, scanLocation, date, orderNumber); // 5

            //    var packageExists = await _databaseService.DoesPackageExistAsync(scanLocation, date, orderNumber); // 6

            //    if (packageExists == null) return;

            //    float volumeFactor;

            //    if (packageData.df_ndl == _configuration.ScanLocation // 7
            //        && _configuration.ExceptionList!.ContainsKey(packageData.df_kundennr)) 
            //    {
            //        volumeFactor = _configuration.ExceptionList[packageData.df_kundennr];
            //    } else {
            //        volumeFactor = _configuration.DefaultVolumeFactor;
            //    }

            //    await _databaseService.CreatePackageAsync(packageData, scan, volumeFactor); // 8

            //    var volumeWeight = await _databaseService.GetTotalWeightAsync(packageData.df_ndl,   // 9
            //                                                                      packageData.df_datauftannahme, 
            //                                                                      packageData.df_lfdnrauftrag);

            //    if (volumeWeight == null) return;

            //    await _databaseService.UpdateTotalWeightAsync((int) volumeWeight, scanLocation, date, orderNumber);

            //    var weight = (int) realWeight;

            //    if (packageData.df_ndl == _configuration.ScanLocation && volumeWeight > realWeight) {
            //        weight = (int) volumeWeight;
            //    } 

            //    await _databaseService.UpdateOrderWeightAsync((int) realWeight, scanLocation, date, orderNumber);
            //}
    }
}