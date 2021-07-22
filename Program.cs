using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            var sourceConnString = "";
            var targetConnString = "";

            var sourceClientSettings = MongoClientSettings.FromConnectionString(sourceConnString);
            var targetClientSettings = MongoClientSettings.FromConnectionString(targetConnString);

            var sourceClient = new MongoClient(sourceClientSettings);
            var targetClient = new MongoClient(targetClientSettings);
            var startPoint = DateTimeOffset.UtcNow.AddHours(-10);

            SyncDb(sourceClient, targetClient, startPoint).Wait();

            Console.ReadLine();
        }

        private static long DelaySeconds(int clusterTimestamp)
        {
            return DateTimeOffset.UtcNow.ToUnixTimeSeconds() - clusterTimestamp;
        }

        private static async Task SyncDb(IMongoClient sourceClient, IMongoClient targetClient, DateTimeOffset startPoint)
        {
            var options = new ChangeStreamOptions
            {
                BatchSize = 2000,
                StartAtOperationTime = new BsonTimestamp((int)startPoint.ToUnixTimeSeconds(), 1),
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup
            };

            using (var cursor = await sourceClient.WatchAsync(options))
            {
                long count = 0;
                while (await cursor.MoveNextAsync())
                {
                    if (!cursor.Current.Any())
                    {
                        Console.WriteLine("No changes, skip...");
                        continue;
                    }

                    var token = cursor.GetResumeToken();
                    var headDoc = cursor.Current.First();
                    Console.WriteLine($"[{DelaySeconds(headDoc.ClusterTime.Timestamp)}s] [{count}] Token: {token} ----------------------------");

                    foreach (var change in cursor.Current)
                    {
                        ++count;
                        await ReplayChangeToTarget(targetClient, change);
                    }
                }
            }
        }

        private static async Task ReplayChangeToTarget(IMongoClient targetClient, ChangeStreamDocument<BsonDocument> change)
        {
            var dbName = change.CollectionNamespace.DatabaseNamespace.ToString();
            var collectionName = change.CollectionNamespace.CollectionName.ToString();
            var collection = targetClient.GetDatabase(dbName).GetCollection<BsonDocument>(collectionName);

            // Uncomment the following two lines to print changes without replaying to the target
            //Console.WriteLine($"[{DelaySeconds(change.ClusterTime.Timestamp)}s] {change.CollectionNamespace.FullName}");
            //return;
            
            try
            {
                using (var session = await targetClient.StartSessionAsync())
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2)))
                {
                    await session.WithTransactionAsync(
                        async (s, ct) =>
                        {
                            if (change.OperationType == ChangeStreamOperationType.Insert)
                            {
                                await collection.InsertOneAsync(s, change.FullDocument, cancellationToken: ct);
                            }
                            else if (change.OperationType == ChangeStreamOperationType.Delete)
                            {
                                var id = change.DocumentKey.GetValue("_id").ToString();
                                var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
                                await collection.DeleteOneAsync(s, filter, cancellationToken: ct);
                            }
                            else if (change.OperationType == ChangeStreamOperationType.Update || change.OperationType == ChangeStreamOperationType.Replace)
                            {
                                var id = change.FullDocument.GetValue("_id").ToString();
                                var filter = Builders<BsonDocument>.Filter.Eq("_id", id);
                                await collection.ReplaceOneAsync(s, filter, change.FullDocument, cancellationToken: ct);
                            }
                            else
                            {
                                Console.WriteLine($"Unknown type: [{change.OperationType}]");
                            }

                            return string.Empty;
                        }, cancellationToken: cts.Token);
                }
            }
            catch (MongoWriteException ex)
            {
                if (ex.WriteError.Code == 11000)
                {
                    Console.WriteLine($"[{DelaySeconds(change.ClusterTime.Timestamp)}s] Dup key: {change.FullDocument.GetValue("_id")}, ignore...");
                }
                else
                {
                    Console.WriteLine($"[{DelaySeconds(change.ClusterTime.Timestamp)}s] [# {change.OperationType}] MongoWriteException: {change.ToJson()}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{DelaySeconds(change.ClusterTime.Timestamp)}s] [# {change.OperationType}] Exception [{change.DocumentKey.ToJson()}] [{ex.GetType()}]: {ex.Message}");
            }
        }
    }
}
