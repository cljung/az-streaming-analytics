Param(
   [Parameter(Mandatory=$True)][string]$Operation = "status",
   [Parameter(Mandatory=$False)][string]$JobName = "",
   [Parameter(Mandatory=$False)][string]$ResourceGroupName = "StreamAnalytics-Default-West-Europe",
   [Parameter(Mandatory=$False)][string]$DatasourcesFile = "",
   [Parameter(Mandatory=$False)][string]$JobStartTime = ""
)

$templateFile = ".\$JobName.json"
if ( $DatasourcesFile -eq "" ) {
     $DatasourcesFile = ".\$JobName-datasources.json"
}

# WARNING: Make sure to reference the latest version of the \Microsoft.ServiceBus.dll (I tested with ver 3.0.50497.1)
Add-Type -Path ".\Microsoft.ServiceBus.dll"

# --------------------------------------------------------------------------------------------------------------
#
function Login()
{
	Login-AzureRmAccount
}
# --------------------------------------------------------------------------------------------------------------
# Storage Account datasource
function SetDatasourceStorageAccount($DataSource, $dict, $ds) {
    $StorageAccountName =  $ds.StorageAccountName
    write-output "Setting credentials for StorageAccount $StorageAccountName"
    # get storage account key if we haven't already retrieved it
    $StorageAccountKey =  $dict[$StorageAccountName]
    if ( $StorageAccountKey -eq $null ) {
        $StorageAccountKey = (Get-AzureStorageKey $storageAccountName).Primary
        $dict[$StorageAccountName] = $StorageAccountKey
    }
    $DataSource.Properties.StorageAccounts[0].AccountName = $StorageAccountName
    $DataSource.Properties.StorageAccounts[0].AccountKey = $StorageAccountKey
}
# --------------------------------------------------------------------------------------------------------------
# 
function GetServiceBusNamespacePolicyKey($ServiceBusNamespace, $SharedAccessPolicyName, $dict, $ds) {
    # done this before?? if not, go and get the key from Azure
    $SharedAccessPolicyKey = $dict["$ServiceBusNamespace.$SharedAccessPolicyName"]
    if ( $SharedAccessPolicyKey -eq $null ) {
        # get the Event Hub namespace and get the key from the connection string
        write-output "Getting SharedAccessKey for namespace $ServiceBusNamespace rule $SharedAccessPolicyName" 
        $sbr = Get-AzureSBAuthorizationRule -Namespace $ServiceBusNamespace
        # this akward line locates the right Policy, splits the connect string in parts, gets the PolicyKey [2] and removes the leading name (leaving only the key)
        $SharedAccessPolicyKey = (($sbr | where { $_.Name -eq $SharedAccessPolicyName }).ConnectionString -Split ";")[2] -Replace "SharedAccessKey=",""
        if ( $SharedAccessPolicyKey.Length -gt 0 ) {
            $dict["$ServiceBusNamespace.$SharedAccessPolicyName"] = $SharedAccessPolicyKey
        }
    }
}
# --------------------------------------------------------------------------------------------------------------
# Event Hub datasource
function SetDatasourceEventHub($DataSource, $dict, $ds) {
    $ServiceBusNamespace =  $ds.ServiceBusNamespace
    $EventHubName = $ds.EventHubName
    write-output "Setting credentials for Event Hub $EventHubName"
    $DataSource.Properties.ServiceBusNamespace = $ServiceBusNamespace 
    $DataSource.Properties.EventHubName = $EventHubName 
    $SharedAccessPolicyName = $DataSource.Properties.SharedAccessPolicyName 
    GetServiceBusNamespacePolicyKey $ServiceBusNamespace $SharedAccessPolicyName $dict        
    $DataSource.Properties.SharedAccessPolicyKey = $dict["$ServiceBusNamespace.$SharedAccessPolicyName"]
}
# --------------------------------------------------------------------------------------------------------------
# Service Bus Queue datasource
function SetDatasourceSbQueueTopic( $DataSource, $dict, $ds, $Type ) {
    $qtName = ""
    if ( $Type -eq "Queue" ) {
        # this complexity due to QueueName/TopicName may or may not be specified in datasource param file
        $qtName = $ds.QueueName
        if ( $qtName -eq $null ) {
            $qtName = $DataSource.Properties.QueueName            
        } else {
            $DataSource.Properties.QueueName = $qtName
        }
    } else {
        $qtName = $ds.TopicName
        if ( $qtName -eq $null ) {
            $qtName = $DataSource.Properties.TopicName            
        } else {
            $DataSource.Properties.TopicName = $qtName
        }        
    }
    write-output "Setting credentials for Service Bus $Type $qtName"         
    $DataSource.Properties.ServiceBusNamespace = ($ServiceBusNamespace = $ds.ServiceBusNamespace)
    $SharedAccessPolicyName = $DataSource.Properties.SharedAccessPolicyName     
    GetServiceBusNamespacePolicyKey $ServiceBusNamespace $SharedAccessPolicyName $dict
    $SharedAccessPolicyKey = $dict["$ServiceBusNamespace.$SharedAccessPolicyName"]
    # key is not on namespace level, but on the Queue itself. See if we already have it, else, get it from Queue (complex)
    if ( $SharedAccessPolicyKey -eq $null ) {
        $SharedAccessPolicyKey = $dict["$ServiceBusNamespace.$qtName.$SharedAccessPolicyName"]
    }
    if ( $SharedAccessPolicyKey -eq $null ) {
        write-output "Getting SharedAccessKey for $Type $qtName rule $SharedAccessPolicyName" 
        $ns = Get-AzureSBNamespace -Name $ServiceBusNamespace
        $nsMgr = [Microsoft.ServiceBus.NamespaceManager]::CreateFromConnectionString($ns.ConnectionString);
        if ( $Type -eq "Queue" ) {
            $q = $nsMgr.GetQueue($qtName)
        } else {
            $q = $nsMgr.GetTopic($qtName)
        }
        $idx = [array]::IndexOf($q.Authorization.KeyName, $SharedAccessPolicyName)
        $SharedAccessPolicyKey = $q.Authorization.PrimaryKey[$idx]
        $dict["$ServiceBusNamespace.$qtName.$SharedAccessPolicyName"] = $SharedAccessPolicyKey
    }
    $DataSource.Properties.SharedAccessPolicyKey = $SharedAccessPolicyKey    
}
# --------------------------------------------------------------------------------------------------------------
# SQL Azure datasource
function SetDatasourceSqlAzure( $DataSource, $dict, $ds ) {
    # init with json config values    
    $DataSource.Properties.Server = ($DbServer = $ds.Server)
    $DataSource.Properties.Database = ($DbName = $ds.Database)
    write-output "Setting credentials for SQL Azure $DbServer/$DBName"
    # since the userid may be changed in the UI prompt, we need to store the Uid/Pwd pair both in the dictionary for the server/db
    $dbKey = "$DbServer.$DbName"
    $DbUid = $dict["$dbKey.Uid"]
    if ( $DbUid -eq $null ) {
        $DbUid = $ds.User
    }
    $DbPwd = $dict["$dbKey.Pwd"]
    if ( $DbPwd -eq $null ) {
        $DbPwd = $ds.Password
    }
    if ( $DbUid -eq $null -or $DbPwd -eq $null ) {
        $Credential = Get-Credential $DbUid -Message "DB login $DbServer.$DBName"
        $DbUid = $Credential.UserName
        $DbPwd = $Credential.GetNetworkCredential().password
        $dict["$dbKey.Uid"] = $DbUid
        $dict["$dbKey.Pwd"] = $DbPwd
    }
    $DataSource.Properties.User = $DbUid
    $DataSource.Properties.Password = $DbPwd    
}
# --------------------------------------------------------------------------------------------------------------
# All DataSources that this script supports updating
function SetDataSource( $DataSourceName, $DataSource, $dict ) {
    write-output "$($Datasource.Type) : $DataSourceName"
    $ds = ($dict["Datasources"] | where { $_.Name -eq $DataSourceName })
    switch ( $Datasource.Type.ToLower() )
    {
        "microsoft.servicebus/eventhub" { SetDatasourceEventHub $DataSource $dict $ds }
        "microsoft.storage/blob" { SetDatasourceStorageAccount $DataSource $dict $ds }
        "microsoft.sql/server/database" { SetDatasourceSqlAzure $DataSource $dict $ds }
        "microsoft.servicebus/queue" { SetDatasourceSbQueueTopic $DataSource $dict $ds "Queue" }
        "microsoft.servicebus/topic" { SetDatasourceSbQueueTopic $DataSource $dict $ds "Topic" }
        default { 
            "Unsupported DataSource type: $($Datasource.Type)"
            exit 3
        }
    }            
}
# --------------------------------------------------------------------------------------------------------------
#
function CreateStreamingAnalyticsJob()
{    
    # load saved template from file into object model
    if ( (test-Path $templateFile) -eq $false ) {
        write-error "Input template file do not exist: $templateFile"
        exit 1
    }
    write-output "Loading template file: $templateFile"
    $jobj = (Get-Content $templateFile | ConvertFrom-Json)

    # load the datasource parameters file into object model
    if ( (test-Path $DatasourcesFile) -eq $false ) {
        write-error "Datasource parameters file do not exist: $DatasourcesFile"
        exit 1
    }
    write-output "Loading datasources file: $DatasourcesFile"
    $p = (Get-Content $DatasourcesFile | ConvertFrom-Json)
    $pJob = ($p.Jobs | where { $_.Name -eq $JobName })
    if ( $pJob -eq $null ) {
        write-error "No datasource parameters found for job $JobName in file $DatasourcesFile"
        exit 2
    }   
    
    # dictionary that we store global variables in (so we don't have query/ask for them more than once)
    $dict = @{}
    $dict.add( "Datasources", $pJob.Datasources )

    # update input datasources
    foreach( $input in $jobj.Properties.Inputs ) {
        SetDataSource $input.Name $input.Properties.DataSource $dict
    }
    
    # update output datasources
    foreach( $output in $jobj.Properties.Outputs ) {
        SetDataSource $output.Name $output.Properties.DataSource $dict
    }
    # save a new temp file with updated keys to use for creation
    $inputFile = "$env:TEMP\$JobName.tmp.json"
    $jobj | ConvertTo-Json -Depth 25 -Compress > $inputFile

    # create the Streaming Analytics job via importing the json file
    write-output "Updating StreamAnalytics Job in Azure..."
    $res = New-AzureRmStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -File $inputFile -Name $JobName -Force
    Remove-Item $inputFile
}

# --------------------------------------------------------------------------------------------------------------
#
function ExportDataSource( $DataSourceName, $DataSource, $dsa ) {
    $dict = @{}
    $dict["Name"] = $DataSourceName
    switch ( $Datasource.Type.ToLower() )
    {
        "microsoft.servicebus/eventhub" {  $dict["ServiceBusNamespace"] = $DataSource.Properties.ServiceBusNamespace  
                                           $dict["EventHubName"] = $DataSource.Properties.EventHubName 
                                        }
        "microsoft.storage/blob" { $dict["StorageAccountName"] = $DataSource.Properties.StorageAccounts[0].AccountName }
        "microsoft.sql/server/database" {  $dict["Server"] = $DataSource.Properties.Server
                                           $dict["Database"] = $DataSource.Properties.Database
                                           $dict["User"] = $DataSource.Properties.User
                                           $dict["Password"] = $DataSource.Properties.Password
                                         }
        "microsoft.servicebus/queue" { $dict["ServiceBusNamespace"] = $DataSource.Properties.ServiceBusNamespace
                                       $dict["QueueName"] = $DataSource.Properties.QueueName
                                     }
        "microsoft.servicebus/topic" { $dict["ServiceBusNamespace"] = $DataSource.Properties.ServiceBusNamespace
                                       $dict["TopicName"] = $DataSource.Properties.TopicName
                                      }
        default { 
            "Unsupported DataSource type: $($Datasource.Type)"
            exit 3
        }
    }                
    $dsa.Add($dict) | Out-Null
}
# --------------------------------------------------------------------------------------------------------------
#
function ExportStreamingAnalyticsJob()
{
    write-output "Exporting Job to file $templateFile"
    $saj = Get-AzureRMStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $JobName
    $saj.PropertiesInJson > $templateFile
    
    # generate JSON structure that we can add to the datasource-parameters file
    write-output "Exporting Job Datasources to file $datasourcesFile"
    $dsa = [System.Collections.ArrayList]@()
    foreach( $input in $saj.Properties.Inputs ) {
        ExportDataSource $input.Name $input.Properties.DataSource $dsa
    }
    # update output datasources
    foreach( $output in $saj.Properties.Outputs ) {
        ExportDataSource $output.Name $output.Properties.DataSource $dsa
    }
    $dsj = @{}
    $dsj["Name"] = $JobName
    $dsj["Datasources"] = $dsa
    $json = @{}
    $json.Add("Jobs", @($dsj))
    # dump it on the console so we can grab it from there
    $json | ConvertTo-Json -depth 5 > $DatasourcesFile
}

# --------------------------------------------------------------------------------------------------------------
#
function StatusStreamingAnalyticsJob()
{
    # export the Streaming Analytics job to a json file
    $saj = Get-AzureRMStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $JobName
    write-Output "JobName  : $JobName"
    write-Output "Created  : $($saj.CreatedDate)"
    write-Output "JobState : $($saj.JobState)"
    write-output "LastEvent: $($saj.Properties.LastOutputEventTime)"
    write-output "Input    : $($saj.Properties.Inputs.Properties.DataSource.Type)"
    write-output "Output   : $($saj.Properties.Outputs.Properties.DataSource.Type)"
    
}

# --------------------------------------------------------------------------------------------------------------
#
function DeleteStreamingAnalyticsJob()
{
    Remove-AzureRmStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $JobName -Force  
}
# --------------------------------------------------------------------------------------------------------------
#
function StartStreamingAnalyticsJob()
{
    if ( $JobStartTime.Length -gt 0 ) {
        write-output "JobStartTime: $JobStartTime"
        $dt = [datetime]::Parse($JobStartTime) 
        #Start-AzureRmStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $JobName -OutputStartMode "CustomTime" -OutputStartTime $dt        
        Start-AzureRmStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $JobName -OutputStartMode "JobStartTime" # LastOutputEventTime 
    } else {
        Start-AzureRmStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $JobName -OutputStartMode "JobStartTime" # LastOutputEventTime 
    }
    -OutputStartTime
}
# --------------------------------------------------------------------------------------------------------------
#
function StopStreamingAnalyticsJob()
{
    Stop-AzureRmStreamAnalyticsJob -ResourceGroupName $ResourceGroupName -Name $JobName  
}
# --------------------------------------------------------------------------------------------------------------
#
$startTime = Get-Date

switch ( $Operation.ToLower() )
{
	"login" { Login }
	"create" { CreateStreamingAnalyticsJob }
	"import" { CreateStreamingAnalyticsJob }
	"delete" { DeleteStreamingAnalyticsJob }
	"export" { ExportStreamingAnalyticsJob }
	"start" { StartStreamingAnalyticsJob }
	"stop" { StopStreamingAnalyticsJob }
	"status" { StatusStreamingAnalyticsJob }
	default { Write-Host "Operation must be login, status, create, delete, export, start, stop" }
}

$finishTime = Get-Date
$TotalTime = ($finishTime - $startTime).TotalSeconds
Write-Output "Time: $TotalTime sec(s)"        
