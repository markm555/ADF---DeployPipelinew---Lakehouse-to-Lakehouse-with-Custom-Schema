using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using System.Threading.Tasks;
using System;

// ----> Before running populate default values in class object starting on line ~250 <---- *** IMPORTANT ***
internal class Program
{
    private const string ApiVersion = "2018-06-01";

    static async Task<int> Main(string[] args)
    {
        var o = Options.Parse(args);
        if (o == null) return 2;

        // Auth to ARM. DefaultAzureCredential works for:
        // - local dev with "az login" or VS sign-in
        // - managed identity when deployed
        var credential = new DefaultAzureCredential();
        var token = await credential.GetTokenAsync(
            new TokenRequestContext(new[] { "https://management.azure.com/.default" }));

        using var http = new HttpClient();
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);

        // Names we’ll deploy
        var lsSourceLakehouse = "ls_fabric_lakehouse_source";
        var lsSinkLakehouse = "ls_fabric_lakehouse_sink";

        var dsSource = "ds_fabric_lakehouse_source_table";
        var dsSink = "ds_fabric_lakehouse_sink_table";

        var pipelineName = "LH2LH";

        // 1) Linked Service: Source Fabric Lakehouse connector (type = Lakehouse)
        var sourceLakehouseLinkedService = BuildFabricLakehouseLinkedService(lsSourceLakehouse,
            workspaceId: o.SourceFabricWorkspaceId,
            lakehouseId: o.SourceFabricLakehouseId,
            tenantId: o.TenantId,
            spnId: o.ServicePrincipalId,
            spnSecret: o.ServicePrincipalSecret,
            connectViaIrName: o.LakehouseIntegrationRuntimeName);

        await PutLinkedServiceAsync(http, o, lsSourceLakehouse, sourceLakehouseLinkedService);

        // 2) Linked Service: Sink Fabric Lakehouse connector (type = Lakehouse)
        var sinkLakehouseLinkedService = BuildFabricLakehouseLinkedService(lsSinkLakehouse,
            workspaceId: o.SinkFabricWorkspaceId,
            lakehouseId: o.SinkFabricLakehouseId,
            tenantId: o.TenantId,
            spnId: o.ServicePrincipalId,
            spnSecret: o.ServicePrincipalSecret,
            connectViaIrName: o.LakehouseIntegrationRuntimeName);

        await PutLinkedServiceAsync(http, o, lsSinkLakehouse, sinkLakehouseLinkedService);

        // 3) Dataset: source Lakehouse table WITH SCHEMA + TABLE
        var sourceDataset = BuildLakehouseTableDataset(dsSource, lsSourceLakehouse, o.SourceSchema, o.SourceTable);
        await PutDatasetAsync(http, o, dsSource, sourceDataset);

        // 4) Dataset: sink Lakehouse table WITH SCHEMA + TABLE
        var sinkDataset = BuildLakehouseTableDataset(dsSink, lsSinkLakehouse, o.SinkSchema, o.SinkTable);
        await PutDatasetAsync(http, o, dsSink, sinkDataset);

        // 5) Pipeline: Copy activity lakehouse(table) -> lakehouse(table)
        var pipeline = BuildCopyPipeline(pipelineName, dsSource, dsSink);
        await PutPipelineAsync(http, o, pipelineName, pipeline);

        // 6) Run pipeline (createRun)
        var runId = await CreateRunAsync(http, o, pipelineName);
        Console.WriteLine($"Pipeline runId: {runId}");
        Console.WriteLine("Done.");
        return 0;
    }

    // ---------------------------
    // REST Functions
    // ---------------------------

    private static async Task PutLinkedServiceAsync(HttpClient http, Options o, string linkedServiceName, object body)
    {
        var url = ArmUrl(o, $"linkedservices/{linkedServiceName}");
        await PutAsync(http, url, body);
        Console.WriteLine($"Upserted linked service: {linkedServiceName}");
    }

    private static async Task PutDatasetAsync(HttpClient http, Options o, string datasetName, object body)
    {
        var url = ArmUrl(o, $"datasets/{datasetName}");
        await PutAsync(http, url, body);
        Console.WriteLine($"Upserted dataset: {datasetName}");
    }

    private static async Task PutPipelineAsync(HttpClient http, Options o, string pipelineName, object body)
    {
        var url = ArmUrl(o, $"pipelines/{pipelineName}");
        await PutAsync(http, url, body);
        Console.WriteLine($"Upserted pipeline: {pipelineName}");
    }

    private static async Task<string> CreateRunAsync(HttpClient http, Options o, string pipelineName)
    {
        var url =
            $"https://management.azure.com/subscriptions/{o.SubscriptionId}/resourceGroups/{o.ResourceGroupName}/providers/Microsoft.DataFactory/factories/{o.FactoryName}/pipelines/{pipelineName}/createRun?api-version={ApiVersion}";

        using var req = new HttpRequestMessage(HttpMethod.Post, url);
        req.Content = new StringContent("{}", Encoding.UTF8, "application/json");

        using var resp = await http.SendAsync(req);
        var content = await resp.Content.ReadAsStringAsync();
        resp.EnsureSuccessStatusCode();

        using var doc = JsonDocument.Parse(content);
        return doc.RootElement.GetProperty("runId").GetString()!;
    }

    private static async Task PutAsync(HttpClient http, string url, object body)
    {
        var json = JsonSerializer.Serialize(body, new JsonSerializerOptions { WriteIndented = true });
        using var req = new HttpRequestMessage(HttpMethod.Put, url);

        // "*" forces unconditional update
        req.Headers.TryAddWithoutValidation("If-Match", "*");
        req.Content = new StringContent(json, Encoding.UTF8, "application/json");

        using var resp = await http.SendAsync(req);
        var content = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            Console.Error.WriteLine(content);
        }
        resp.EnsureSuccessStatusCode();
    }

    private static string ArmUrl(Options o, string childPath)
        => $"https://management.azure.com/subscriptions/{o.SubscriptionId}/resourceGroups/{o.ResourceGroupName}/providers/Microsoft.DataFactory/factories/{o.FactoryName}/{childPath}?api-version={ApiVersion}";

    // ---------------------------
    // Payload builder Functions
    // ---------------------------

    private static object BuildFabricLakehouseLinkedService(
        string name,
        string workspaceId,
        string lakehouseId,
        string tenantId,
        string spnId,
        string spnSecret,
        string? connectViaIrName)
    {
        // Linked service properties for Fabric Lakehouse: type=Lakehouse with workspaceId/artifactId/tenant/SPN info.
        // connectVia is optional.
        var ls = new
        {
            name,
            properties = new
            {
                type = "Lakehouse",
                typeProperties = new
                {
                    workspaceId = workspaceId,
                    artifactId = lakehouseId,
                    tenant = tenantId,
                    servicePrincipalId = spnId,
                    servicePrincipalCredentialType = "ServicePrincipalKey",
                    servicePrincipalCredential = new
                    {
                        type = "SecureString",
                        value = spnSecret
                    }
                },
                // Only include connectVia when provided (keeps payload clean; default Azure IR otherwise)
                connectVia = string.IsNullOrWhiteSpace(connectViaIrName)
                    ? null
                    : new
                    {
                        referenceName = connectViaIrName,
                        type = "IntegrationRuntimeReference"
                    }
            }
        };

        return PruneNulls(ls);
    }

    private static object BuildLakehouseTableDataset(string name, string linkedServiceName, string schema, string table)
    {
        // Dataset type is LakehouseTable with typeProperties.schema and typeProperties.table.
        return new
        {
            name,
            properties = new
            {
                linkedServiceName = new { referenceName = linkedServiceName, type = "LinkedServiceReference" },
                type = "LakehouseTable",
                typeProperties = new
                {
                    schema = schema,
                    table = table
                }
            }
        };
    }

    private static object BuildCopyPipeline(string pipelineName, string sourceDatasetName, string sinkDatasetName)
    {
        // Copy activity: lakehouse table -> lakehouse table
        // Source type discriminator may be "LakehouseTableSource" (common) or "LakeHouseTableSource" (SDK casing).
        // If you get a validation error on the discriminator, flip that casing; everything else is correct.
        return new
        {
            name = pipelineName,
            properties = new
            {
                activities = new object[]
                {
                    new
                    {
                        name = "CopyLakehouseTableToLakehouseTable",
                        type = "Copy",
                        inputs = new object[]
                        {
                            new { referenceName = sourceDatasetName, type = "DatasetReference" }
                        },
                        outputs = new object[]
                        {
                            new { referenceName = sinkDatasetName, type = "DatasetReference" }
                        },
                        typeProperties = new
                        {
                            source = new { type = "LakehouseTableSource" },
                            sink   = new { type = "LakehouseTableSink" }
                        }
                    }
                }
            }
        };
    }

    // Removes null-valued properties (so connectVia can be optional without emitting "connectVia": null)
    private static object PruneNulls(object obj)
    {
        var json = JsonSerializer.SerializeToElement(obj);
        using var doc = JsonDocument.Parse(json.GetRawText());
        var pruned = Prune(doc.RootElement);
        return JsonSerializer.Deserialize<object>(pruned.GetRawText())!;
    }

    private static JsonElement Prune(JsonElement element)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            WritePruned(element, writer);
        }
        stream.Position = 0;
        using var doc = JsonDocument.Parse(stream);
        return doc.RootElement.Clone();
    }

    private static void WritePruned(JsonElement element, Utf8JsonWriter writer)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.Value.ValueKind == JsonValueKind.Null) continue;

                    // If nested object becomes empty after pruning, skip it
                    if (prop.Value.ValueKind == JsonValueKind.Object)
                    {
                        using var ms = new MemoryStream();
                        using (var w2 = new Utf8JsonWriter(ms))
                        {
                            WritePruned(prop.Value, w2);
                        }
                        ms.Position = 0;
                        using var d2 = JsonDocument.Parse(ms);
                        if (!d2.RootElement.EnumerateObject().Any())
                            continue;
                    }

                    writer.WritePropertyName(prop.Name);
                    WritePruned(prop.Value, writer);
                }
                writer.WriteEndObject();
                break;

            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (var item in element.EnumerateArray())
                    WritePruned(item, writer);
                writer.WriteEndArray();
                break;

            default:
                element.WriteTo(writer);
                break;
        }
    }

    // ---------------------------
    // Options / CLI parsing
    // ---------------------------

    private sealed class Options
    {
        // ARM / Factory
        public string SubscriptionId { get; set; } = "";
        public string ResourceGroupName { get; set; } = "";
        public string FactoryName { get; set; } = "";

        // Lakehouse IR (optional)
        public string LakehouseIntegrationRuntimeName { get; set; } = ""; // optional; leave empty for default Azure IR

        // Fabric Source
        public string SourceFabricWorkspaceId { get; set; } = "";
        public string SourceFabricLakehouseId { get; set; } = "";
        public string SourceSchema { get; set; } = "";
        public string SourceTable { get; set; } = "";

        // Fabric Sink
        public string SinkFabricWorkspaceId { get; set; } = "";
        public string SinkFabricLakehouseId { get; set; } = "";
        public string SinkSchema { get; set; } = "";
        public string SinkTable { get; set; } = "";

        // Auth for Fabric linked service (SPN)
        public string TenantId { get; set; } = "";
        public string ServicePrincipalId { get; set; } = "";
        public string ServicePrincipalSecret { get; set; } = "";

        public static Options? Parse(string[] args)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].StartsWith("--") && i + 1 < args.Length)
                {
                    dict[args[i][2..]] = args[i + 1];
                    i++;
                }
            }

            var o = new Options();

            string GetOr(string key, string current)
                => (dict.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v)) ? v : current;

            o.SubscriptionId = GetOr("sub", o.SubscriptionId);
            o.ResourceGroupName = GetOr("rg", o.ResourceGroupName);
            o.FactoryName = GetOr("factory", o.FactoryName);

            o.LakehouseIntegrationRuntimeName = GetOr("ir", o.LakehouseIntegrationRuntimeName);

            o.SourceFabricWorkspaceId = GetOr("srcWorkspaceId", o.SourceFabricWorkspaceId);
            o.SourceFabricLakehouseId = GetOr("srcLakehouseId", o.SourceFabricLakehouseId);
            o.SourceSchema = GetOr("srcSchema", o.SourceSchema);
            o.SourceTable = GetOr("srcTable", o.SourceTable);

            o.SinkFabricWorkspaceId = GetOr("dstWorkspaceId", o.SinkFabricWorkspaceId);
            o.SinkFabricLakehouseId = GetOr("dstLakehouseId", o.SinkFabricLakehouseId);
            o.SinkSchema = GetOr("dstSchema", o.SinkSchema);
            o.SinkTable = GetOr("dstTable", o.SinkTable);

            o.TenantId = GetOr("tenantId", o.TenantId);
            o.ServicePrincipalId = GetOr("spnId", o.ServicePrincipalId);
            o.ServicePrincipalSecret = GetOr("spnSecret", o.ServicePrincipalSecret);

            // Minimal validation
            var required = new[]
            {
                o.SubscriptionId, o.ResourceGroupName, o.FactoryName,
                o.SourceFabricWorkspaceId, o.SourceFabricLakehouseId, o.SourceTable,
                o.SinkFabricWorkspaceId, o.SinkFabricLakehouseId, o.SinkTable,
                o.TenantId, o.ServicePrincipalId, o.ServicePrincipalSecret
            };

            if (required.Any(string.IsNullOrWhiteSpace))
            {
                Console.Error.WriteLine("Missing required args. Example:");
                Console.Error.WriteLine("dotnet run -- " +
                    "--sub <subId> --rg <rg> --factory <adfName> " +
                    "--tenantId <tenantGuid> --spnId <appId> --spnSecret <secret> " +
                    "--srcWorkspaceId <fabricWorkspaceGuid> --srcLakehouseId <lakehouseArtifactGuid> --srcSchema <schema> --srcTable <table> " +
                    "--dstWorkspaceId <fabricWorkspaceGuid> --dstLakehouseId <lakehouseArtifactGuid> --dstSchema <schema> --dstTable <table> " +
                    "[--ir <integrationRuntimeName>]");
                return null;
            }

            return o;
        }
    }
}

