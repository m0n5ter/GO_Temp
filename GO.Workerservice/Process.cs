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
        _logger.LogInformation("Starting database transaction");
        await _databaseService.Begin();

        try
        {
            var order = await _databaseService.GetOrderAsync(scan);

            if (order == null)
            {
                _logger.LogInformation("Order was not found, exiting");
            }
            else
            {
                _logger.LogInformation("Order located: {orderId}", order.DF_LFDNRAUFTRAG);
                var isLocalOrder = order.DF_NDL == _configuration.ScanLocation;
                var volumeFactor = _configuration.DefaultVolumeFactor;

                if (isLocalOrder && _configuration.ExceptionList.TryGetValue(order.DF_KUNDENNR, out var customVolumeFactor))
                {
                    volumeFactor = customVolumeFactor;
                    _logger.LogInformation("Using custom volume factor for local order and client {customer}: {volumeFactor}", order.DF_KUNDENNR, volumeFactor);
                }
                else
                {
                    _logger.LogInformation("Using default volume factor: {volumeFactor}", volumeFactor);
                }
                
                if (!await _databaseService.ScanExistsAsync(scan))
                {
                    _logger.LogInformation("Scan didn't exist, adding");
                    await _databaseService.AddScanAsync(order, scan);
                }
                else
                {
                    _logger.LogInformation("Scan with the same parameters already exists, updating");
                    await _databaseService.UpdateScanAsync(scan);
                }

                if (!await _databaseService.PackageExistsAsync(order, scan))
                {
                    _logger.LogInformation("Package didn't exist, adding");
                    await _databaseService.AddPackageAsync(order, scan, volumeFactor);
                }
                else
                {
                    _logger.LogInformation("Package with the same parameters already exists, updating");
                    await _databaseService.UpdatePackageAsync(order, scan, volumeFactor);
                }
                
                var realWeight = await _databaseService.GetTotalWeightAsync(scan);
                var volumeWeight = await _databaseService.GetTotalVolumeWeightAsync(order);
                var chargeableWeight = isLocalOrder ? Math.Max(realWeight, volumeWeight) : realWeight;

                _logger.LogInformation("Updating order weights:");
                _logger.LogInformation("    - Real weight: {realWeight}", realWeight);
                _logger.LogInformation("    - Volume weight: {volumeWeight}", volumeWeight);
                _logger.LogInformation("    - Chargeable weight: {chargeableWeight}", chargeableWeight);

                await _databaseService.SetOrderWeightsAsync(order, realWeight, volumeWeight, chargeableWeight);
            }

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