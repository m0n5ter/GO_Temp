using System.Diagnostics;
using GO.Workerservice.Config;
using GO.Workerservice.Data;
using GO.Workerservice.Model;

namespace GO.Workerservice.Logic;

public class Process(Configuration configuration, DatabaseService databaseService, ILogger<Process> logger)
{
    public async Task ProcessPackageAsync(string message)
    {
        var sw = Stopwatch.StartNew();
        
        var scan = new ScaleDimensionerResult(message);
        
        logger.LogInformation("Processing scan: {scan}", scan);
        logger.LogInformation("Starting database transaction");

        await databaseService.Begin();

        try
        {
            var order = await databaseService.GetOrderAsync(scan.OrderNumber);

            if (order == null)
            {
                logger.LogWarning("Order was not found, exiting");
            }
            else
            {
                logger.LogInformation("Order located: {orderId}", order.DF_LFDNRAUFTRAG);
                var isLocalOrder = order.DF_NDL == configuration.ScanLocation;
                var volumeFactor = configuration.DefaultVolumeFactor;

                if (isLocalOrder && configuration.ExceptionList.TryGetValue(order.DF_KUNDENNR, out var customVolumeFactor))
                {
                    volumeFactor = customVolumeFactor;
                    logger.LogInformation("Using custom volume factor for local order and client {customer}: {volumeFactor}", order.DF_KUNDENNR, volumeFactor);
                }
                else
                {
                    logger.LogInformation("Using default volume factor: {volumeFactor}", volumeFactor);
                }
                
                if (!await databaseService.ScanExistsAsync(scan))
                {
                    logger.LogInformation("Scan didn't exist, adding");
                    await databaseService.AddScanAsync(order, scan);
                }
                else
                {
                    logger.LogInformation("Scan with the same parameters already exists, updating");
                    await databaseService.UpdateScanAsync(scan);
                }

                if (!await databaseService.PackageExistsAsync(order, scan))
                {
                    logger.LogInformation("Package didn't exist, adding");
                    await databaseService.AddPackageAsync(order, scan, volumeFactor);
                }
                else
                {
                    logger.LogInformation("Package with the same parameters already exists, updating");
                    await databaseService.UpdatePackageAsync(order, scan, volumeFactor);
                }
                
                var realWeight = await databaseService.GetTotalWeightAsync(scan);
                var volumeWeight = await databaseService.GetTotalVolumeWeightAsync(order);
                var chargeableWeight = isLocalOrder ? Math.Max(realWeight, volumeWeight) : realWeight;

                logger.LogInformation("Updating order weights:");
                logger.LogInformation("    - Real weight: {realWeight}", realWeight);
                logger.LogInformation("    - Volume weight: {volumeWeight}", volumeWeight);
                logger.LogInformation("    - Chargeable weight: {chargeableWeight}", chargeableWeight);

                await databaseService.SetOrderWeightsAsync(order, realWeight, volumeWeight, chargeableWeight);
            }

            logger.LogInformation("Committing database transaction");
            await databaseService.Commit();
            
            var endTime = sw.Elapsed;
            logger.LogInformation("Processing completed in {time:0} ms", endTime.TotalMilliseconds);
            
            if (endTime.TotalMilliseconds > 300)
                logger.LogWarning("Processing time exceeded 300 ms");
        }
        catch
        {
            await databaseService.Rollback();
            logger.LogWarning("Transaction has been rolled back, no changes were made to the database");
            throw;
        }
    }
}