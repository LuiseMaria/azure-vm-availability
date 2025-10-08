<#
.SYNOPSIS
    Generates a VM and Arc Machine availability report across Azure subscriptions and Log Analytics workspaces.

.DESCRIPTION
    Enumerates Azure subscriptions (or uses a provided list), discovers Log Analytics Workspaces, and runs Kusto queries
    to compute per-Machine availability for a specified month. Availability is
    calculated as the percentage of minutes a Machine reported heartbeats during the reporting period.
    This PowerShell script calculates the availability of Azure Virtual Machines (VMs) and Azure Arc-enabled servers by analyzing heartbeat data from Azure Monitor Logs. It also checks for any suppression alert rules that might affect the availability calculations.
    The script generates a CSV report with availability metrics, including adjustments for any suppression rules that were active during the reporting period.
    VMs whose names start with "vba" or end with "-tmp" are excluded from the main report.
    Current VM power state is retrieved via Azure REST (Get-VMState). If unavailable, that field may be blank.
    Time ranges are handled in UTC.

.PARAMETER ReportMonth
    [int] Optional. Month (1-12) to run the report for. If omitted, the script defaults to the previous calendar month
    of the current year.

.PARAMETER SubscriptionIdList
    [string[]] Optional. Array of subscription IDs to restrict processing to. When not supplied, the script enumerates all
    subscriptions available in the current Az context.

.PARAMETER SubRangeStartEnd
    [int[]] Optional. Two-element array or comma-separated pair (start,end). When supplied, the script processes only the
    subset of subscriptions in the enumerated list from index start (inclusive) to end (inclusive). Useful for batching
    work across large tenants. For Example, to process subscriptions 20 through end, use "-SubRangeStartEnd 20,-1".

.OUTPUTS
    Machine_Availability_<Mon>_<yyyyMMdd_HHmm>.csv
        CSV containing availability metrics for VMs whose subscriptions were resolved. Typical columns:
            subscriptionId, resourceId, vmName, resourceGroup, osType, region,
            availabilityPercent, totalMinutes, availableMinutes, missingMinutes,
            lastHeartbeat, currentPowerState (when available)
    Log-file: Machine_Availability_<Mon>_<yyyyMMdd_HHmm>_Logfile.txt

.REQUIREMENTS
    - Az.Accounts PowerShell module
    - Az.OperationalInsights PowerShell module (or equivalent method to query Log Analytics)
    - Permissions to list subscriptions, read Log Analytics workspaces, and query workspace data
    - run Connect-AzAccount first if not already authenticated

.NOTES
    - Availability is computed at minute granularity for the reporting month.
    -  Time ranges are handled in UTC
    - VMs whose names start with "vba" or end with "-tmp" are excluded from the main report.
    - Current VM power state is retrieved via Azure REST (Get-VMState). If unavailable set to "unknown/deleted".
    - Time ranges are handled in UTC unless explicitly converted or adjusted in the script.
    - Use SubRangeStartEnd to split processing for large numbers of subscriptions to mitigate throttling and long runtimes.
    - Output CSV files are written to the current working directory with timestamped filenames.

.EXAMPLE
    # Default: previous month across all accessible subscriptions
    .\Machine_AvailabilityReport-vm-arcs.ps1

    # Specific month and subscription list
    .\Machine_AvailabilityReport-vm-arcs.ps1 -ReportMonth 3 -SubscriptionIdList '11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222'

    # Process a subset of subscriptions by index range
    .\Machine_AvailabilityReport-vm-arcs.ps1 -ReportMonth 3 -SubRangeStartEnd 20,310

    # Authenticate first (optional) and run against a single subscription:
    Connect-AzAccount -TenantId '<tenant-id>' -SubscriptionId '<subscription-id>'
    .\Machine_AvailabilityReport-vm-arcs.ps1 -ReportMonth 3 -SubscriptionIdList '<subscription-id>'

    Notes:
    - The script requires the Az.Accounts and Az.OperationalInsights modules.
    - Output CSV files are written to the current working directory and are named like:
        Machine_Availability_<Mon>_<yyyyMMdd_HHmm>.csv
#>

#Requires -Modules Az.Accounts, Az.OperationalInsights

param (
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 12)]
    [int]$ReportMonth, # Month for which the report is generated (1-12)
    [string[]]$SubscriptionIdList, # Optional: Specify n Subscription IDs to limit the report to those subscriptions only. If not provided, all accessible subscriptions in Tenant will be included.
    [ValidateCount(2, 2)]
    [int[]]$SubRangeStartEnd, # Provide exactly two integers to define a range (e.g. 20,310)
    $ExportFilePath = "./Machine_Availability_" # Optional: Directory path to save the output CSV files. Default: current directory.
)

# $tenantId = "xxxxxx-xxxx-xxxxx-xxxx-xxxxxxxxx"
# $subscriptionId = "xxxxxx-xxxx-xxxxx-xxxx-xxxxxxxxx"
# Connect-AzAccount -TenantId $tenantId -SubscriptionId $subscriptionId

$LogSessionId = Get-Date -Format "yyyyMMdd_HHmm"
$scriptStartTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$UtcTimeRangeStartDate = (Get-Date -Day 1 -Month $ReportMonth).ToString('yyyy-MM-dd')

function Write-Log {
    param (
        $LogFilePath = "$ExportFilePath",
        [Parameter(Mandatory = $true, Position = 0)]
        $Message,
        [ValidateSet('Info', 'Debug', 'Error')]
        [string]$Severity = "Info",
        [System.ConsoleColor]$Color = [System.ConsoleColor]::Blue
    )
    if ($Severity -eq "Info" -or $Severity -eq "Error") {
        if ($Severity -eq "Error") {
            Write-Host "$Message" -ForegroundColor Red
        } else {
            Write-Host "$Message" -ForegroundColor $Color
        }
        $logMessage = "$($Message)`n"
        $logMessage | Out-File -FilePath "$($LogFilePath)$($Severity)Logfile_$($script:LogSessionId).txt" -Append
    }
    else {
        Write-Host "$Message" -ForegroundColor $Color
    }
}

Write-Log "$scriptStartTime - Starting script for month '$ReportMonth'." -severity Info


function Get-EnabledSubscriptions {
    try {
        $response = Invoke-AzRestMethod -Method Get -Uri "https://management.azure.com/subscriptions?api-version=2022-12-01"
        $subs = (($response.Content | ConvertFrom-Json).value | Where-Object { $_.state -eq "Enabled" }).SubscriptionId
        if (-not $subs -or $subs.Count -eq 0) {
            Write-Log "ERROR: No available subscriptions found." -Severity Error
            exit 1
        }
        $subs = $subs | Sort-Object
        if ($SubRangeStartEnd.Count -eq 2) {
            $subs = $subs[$($SubRangeStartEnd[0])..$($SubRangeStartEnd[-1])]
            Write-Log "Generating report for subscriptions in range index $($SubRangeStartEnd[0]) to $($SubRangeStartEnd[-1])" -Severity Info
        }
        return $($subs)
    }
    catch {
        Write-Log "Error retrieving subscriptions: $_" -Severity Error
        exit 1
    }
}

function Get-LogAnalyticsWorkspaces {
    param (
        [string[]]$SubscriptionIds # optional, if not provided, will search with -UseTenantScope (Search-AzGraph)
    )
    $useTenantScope = -not $($SubscriptionIds)
    $azGraphGetLAWQuery = "resources | where type =~ 'microsoft.operationalinsights/workspaces' | project name, subscriptionId, WorkspaceId = tostring(properties.customerId) | sort by tolower(subscriptionId) asc"

    if($useTenantScope) {
        $workspaces = Search-AzGraph -UseTenantScope -First 1000 -Query $azGraphGetLAWQuery
    }
    else {
        $workspaces = Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Query $azGraphGetLAWQuery
    }
    $skip = 1000
    while($workspaces.Count -ge $skip) {
        if($useTenantScope) {
            $workspaces += Search-AzGraph -UseTenantScope -First 1000 -Skip $skip -Query $azGraphGetLAWQuery
        }
        else {
            $workspaces += Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Skip $skip -Query $azGraphGetLAWQuery
        }
        $skip += 1000
    }
    $script:LogAnalyticsWorkspacesInTenant += $workspaces
}

function Get-VMsInTenant {
    param (
        [string[]]$SubscriptionIds # optional, if not provided, will search with -UseTenantScope (Search-AzGraph)
    )
    try {
        $azGraphGetVMQuery = "resources | where type =~ 'microsoft.compute/virtualmachines' | project name, id, subscriptionId, powerState = properties.extended.instanceView.powerState.displayStatus | sort by tolower(subscriptionId) asc"
        $useTenantScope = -not $($SubscriptionIds)

        if($useTenantScope) {
            $vmListResponse = Search-AzGraph -UseTenantScope -First 1000 -Query $azGraphGetVMQuery
        }
        else {
            $vmListResponse = Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Query $azGraphGetVMQuery
        }

        $skip = 1000
        while($vmListResponse.Count -ge $skip) {
            if($useTenantScope) {
                $vmListResponse += Search-AzGraph -UseTenantScope -First 1000 -Skip $skip -Query $azGraphGetVMQuery
            }
            else {
                $vmListResponse += Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Skip $skip -Query $azGraphGetVMQuery
            }
            $skip += 1000
        }
        $script:vmsInTenant += $vmListResponse
        foreach ($vm in $vmListResponse) {
            $script:VmStatusById[$vm.Id] = $vm.powerState
        }
    }
    catch {
        Write-Log "Error requesting status for VM with Query '$azGraphGetVMQuery': $_" -Severity Error
    }
}
function Get-ArcMachinesInTenant {
    param (
        [string[]]$SubscriptionIds # optional, if not provided, will search with -UseTenantScope (Search-AzGraph)
    )
    try {
        $useTenantScope = -not $($SubscriptionIds)
        $azGraphGetArcMachinesQuery = "resources | where type =~ 'microsoft.hybridcompute/machines' | project name, id, subscriptionId,  status = properties.status | sort by tolower(subscriptionId) asc"

        if($useTenantScope) {
            $arcMachineListResponse = Search-AzGraph -UseTenantScope -First 1000 -Query $azGraphGetArcMachinesQuery
        }
        else {
            $arcMachineListResponse = Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Query $azGraphGetArcMachinesQuery
        }

        $skip = 1000
        while($arcMachineListResponse.Count -ge $skip) {
            if($useTenantScope) {
                $arcMachineListResponse += Search-AzGraph -UseTenantScope -First 1000 -Skip $skip -Query $azGraphGetArcMachinesQuery
            }
            else {
                $arcMachineListResponse += Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Skip $skip -Query $azGraphGetArcMachinesQuery
            }
            $skip += 1000
        }
        $script:ArcMachinesInTenant += $arcMachineListResponse
        foreach ($machine in $arcMachineListResponse) {
            $script:ArcMachinesStatusById[$machine.Id] = $machine.status
        }
    }
    catch {
        Write-Log "Error requesting status for Arc Machines with Query '$azGraphGetArcMachinesQuery': $_" -Severity Error
    }
}

function Get-AlertSuppressionRulesInTenant {
    param (
        [string[]]$SubscriptionIds # optional, if not provided, will search with -UseTenantScope (Search-AzGraph)
    )
    try {
        $azGraphGetSuppressionRulesQuery = "resources | where type =~ 'microsoft.alertsmanagement/actionrules' and not(isempty(properties.schedule)) and ((todatetime(properties.schedule.effectiveFrom) >= startofmonth(datetime($($UtcTimeRangeStartDate))))
            and (todatetime(properties.schedule.effectiveUntil) <= endofmonth(datetime($($UtcTimeRangeStartDate)))))
        | project effectiveFrom = properties.schedule.effectiveFrom, effectiveUntil = properties.schedule.effectiveUntil, scopes = properties.scopes, name, subscriptionId | sort by tolower(subscriptionId) asc"
        if($SubscriptionIds.Count -gt 0) {
            $actionRulesResponse = Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Query $azGraphGetSuppressionRulesQuery
        }
        else {
            $actionRulesResponse = Search-AzGraph -UseTenantScope -First 1000 -Query $azGraphGetSuppressionRulesQuery
        }        
        if($actionRulesResponse.Count -gt 0) {
            $script:SuppressionRulesInTenant = $actionRulesResponse
        }
        if ($actionRulesResponse.Count -ge 1000 -and $SubscriptionIds.Count -eq 0) {
            $actionRulesResponse = Search-AzGraph -UseTenantScope -Skip 1000 -First 1000 -Query $azGraphGetSuppressionRulesQuery
            $script:SuppressionRulesInTenant += $actionRulesResponse
        }
    }
    catch {
        Write-Log "Error requesting Suppressions with Query '$($azGraphGetSuppressionRulesQuery)': $_" -Severity Error
        exit 1
    }
}

function Merge-Law {
    param(
        [Parameter(Mandatory)][string]$existingLAW,
        [Parameter(Mandatory)][string]$newLAW
    )
    $lawSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($match in ($existingLAW -split ',')) { 
        if ($match) { $null = $lawSet.Add($match.Trim()) }
    }
    if ($newLAW) { $null = $lawSet.Add($newLAW.Trim()) }
    $array = @($lawSet)
    return ($array | Sort-Object) -join ', '
}


# GET Suppression Rule List, Subscriptions and Workspace List &  for all Subscriptions
$script:SuppressionRulesInTenant = @()
$script:LogAnalyticsWorkspacesInTenant = @()
$script:VmsInTenant = @()
$script:VmStatusById = New-Object 'System.Collections.Generic.Dictionary[string, string]' ([System.StringComparer]::OrdinalIgnoreCase)

$script:ArcMachinesInTenant = @()
$script:ArcMachinesStatusById = New-Object 'System.Collections.Generic.Dictionary[string, string]' ([System.StringComparer]::OrdinalIgnoreCase)


if(-not $SubscriptionIdList) {
    Write-Log "Starting to query Subscriptions, LAWs, Suppression Rules, VMs and Arc Machines in TenantScope..." -Severity Info
    $subscriptions = (Get-EnabledSubscriptions)
    Get-LogAnalyticsWorkspaces
    Get-AlertSuppressionRulesInTenant
    Get-VMsInTenant
    Get-ArcMachinesInTenant
}
else {
    Write-Log "Starting to query LAWs, Suppression Rules and VMs in for Subscription ID(s): '$($SubscriptionIdList)...'" -severity Info
    $subscriptions = $SubscriptionIdList
    Get-AlertSuppressionRulesInTenant -SubscriptionIds $SubscriptionIdList
    Get-LogAnalyticsWorkspaces -SubscriptionIds $SubscriptionIdList
    Get-VMsInTenant -SubscriptionIds $SubscriptionIdList
    Get-ArcMachinesInTenant -SubscriptionIds $SubscriptionIdList
}
Write-Log "Found $($LogAnalyticsWorkspacesInTenant.Count) LAWs, $($VmsInTenant.Count) VMs, $($ArcMachinesInTenant.Count) Arc Machines and $($SuppressionRulesInTenant.Count) Suppression Rules in Tenant ($($subscriptions.Count) Subscriptions)." -Color Yellow


# KQL - retrieves the uptime of VMs for the previous month, excluding VMs with names starting with "vba" or ending with "-tmp".
$VMHeartbeatsKQL = @"
let timeRangeEnd = endofmonth(datetime($($UtcTimeRangeStartDate)));
let timeRangeStart = startofmonth(timeRangeEnd);
let FilteredHeartbeat = Heartbeat
    | where ResourceType =~ "virtualMachines"
        and not(Resource startswith "vba")
        and not(Resource endswith "-tmp")
        and TimeGenerated between (timeRangeStart .. timeRangeEnd)
    | extend RG = tolower(ResourceGroup), ResourceType;
let VMStartTimes = FilteredHeartbeat
    | summarize first_heartbeat = min(TimeGenerated), last_heartbeat = max(TimeGenerated) by _ResourceId;
FilteredHeartbeat
| lookup kind=leftouter VMStartTimes on _ResourceId
| extend minute_bin = bin(TimeGenerated, 1m)
| where minute_bin >= first_heartbeat
| extend start_time = iff(first_heartbeat > timeRangeStart, first_heartbeat, timeRangeStart)
| extend vm_end = iff(last_heartbeat < timeRangeEnd, last_heartbeat, timeRangeEnd)
| summarize available_minutes = count(), RG = any(RG), Resource = any(Resource), SubscriptionId = any(SubscriptionId), ResourceType = any(ResourceType), start_time = any(start_time), vm_end = any(vm_end) by _ResourceId
| extend total_minutes = datetime_diff("minute", vm_end, start_time)
| extend capped_minutes = iff(available_minutes > total_minutes, total_minutes, available_minutes)
| extend total_available_hours = round(capped_minutes / 60.0, 3)
| extend availability_rate = round(100.0 * capped_minutes / total_minutes, 2)
| extend down_rate = round(100.0 - availability_rate, 2)
| project Resource, ResourceType, RG, _ResourceId, SubscriptionId, timeRangeStart, timeRangeEnd, FirstHeartbeat=start_time, LastHeartbeat=vm_end,
down_rate, availability_rate, total_available_hours, total_down_hours = round((total_minutes - capped_minutes) / 60.0, 3), total_hours_in_month = round(datetime_diff("minute", timeRangeEnd, timeRangeStart) / 60.0, 1)
"@

$ArcMachineHeartbeatsKQL = @"
let timeRangeEnd = (endofmonth(datetime($($UtcTimeRangeStartDate))));
let timeRangeStart = startofmonth(timeRangeEnd);
let FilteredHeartbeat = Heartbeat
    | where ResourceType =~ "machines"
        and TimeGenerated between (timeRangeStart .. timeRangeEnd)
    | extend RG = tolower(ResourceGroup), ResourceType;
let MachineStartTimes = FilteredHeartbeat
    | summarize first_heartbeat = min(TimeGenerated), last_heartbeat = max(TimeGenerated) by _ResourceId;
FilteredHeartbeat
| lookup kind=leftouter MachineStartTimes on _ResourceId
| extend minute_bin = bin(TimeGenerated, 1m)
| where minute_bin >= first_heartbeat
| extend start_time = iff(first_heartbeat > timeRangeStart, first_heartbeat, timeRangeStart)
| extend machine_end = iff(last_heartbeat < timeRangeEnd, last_heartbeat, timeRangeEnd)
| summarize available_minutes = count(), RG = any(RG), Resource = any(Resource), SubscriptionId = any(SubscriptionId), ResourceType = any(ResourceType), start_time = any(start_time), machine_end = any(machine_end) by _ResourceId
| extend total_minutes = datetime_diff("minute", machine_end, start_time)
| extend capped_minutes = iff(available_minutes > total_minutes, total_minutes, available_minutes)
| extend total_available_hours = round(capped_minutes / 60.0, 3)
| extend availability_rate = round(100.0 * capped_minutes / total_minutes, 2)
| extend down_rate = round(100.0 - availability_rate, 2)
| project Resource, ResourceType, RG, _ResourceId, SubscriptionId, timeRangeStart, timeRangeEnd, FirstHeartbeat=start_time, LastHeartbeat=machine_end,
down_rate, availability_rate, total_available_hours, total_down_hours = round((total_minutes - capped_minutes) / 60.0, 3), total_hours_in_month = round(datetime_diff("minute", timeRangeEnd, timeRangeStart) / 60.0, 1)
"@

function Invoke-DataPerLAW {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $Workspace,
        [Parameter(Mandatory = $true)]
        [string]$VMHeartbeatsKQL,
        [Parameter(Mandatory = $true)]
        [string]$ArcMachineHeartbeatsKQL
    )

    Write-Log "[$($global:CurrentWorkspaceIndex)/$($script:LogAnalyticsWorkspacesInTenant.count)] Querying Log Analytics Workspace: $($Workspace.name)" -Severity Debug -Color Black
    $global:CurrentWorkspaceIndex++

    try {
        # run both kql (Arc Machine Heartbeats and VM Heartbeats) for each LAW
        $ArcMachineHeartbeatsKQL, $VMHeartbeatsKQL | ForEach-Object {

            $queryResponse = Invoke-AzOperationalInsightsQuery -WorkspaceId $Workspace.WorkspaceId -Query $($_)
            if ($queryResponse -and $queryResponse.Results.Count -ge 1) {
                Write-Log "Results found for workspace: $($Workspace.name)"
                Get-QueryResults -Results $queryResponse.Results -Workspace $Workspace
            }
        }
    }
    catch {
        Write-Log "ERROR while querying workspace: $($Workspace.name) in Subscription $($Workspace.SubscriptionId). $_" -Severity Error
    }
}

function Get-QueryResults {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $Results,
        [Parameter(Mandatory = $true)]
        $Workspace
    )

    # filter results by subscriptions
    $filteredResults = $Results | Where-Object { $Subscriptions -contains $_.SubscriptionId }

    foreach ($resultRow in $filteredResults) {
        # extract machine data and calculate time values
        $machineData = Merge-MachineWithStatus -ResultRow $resultRow

        # Calculate suppression duration
        $suppressionDuration = Get-SuppressionDuration -MachineData $machineData

        # Calculate availability metrics
        $availabilityMetrics = Measure-AvailabilityMetrics -ResultRow $resultRow -SuppressionDuration $suppressionDuration
        
        # add new entry or update LAW in the ResultList for matching SubscriptionId, FirstHeartbeat and LastHeartbeat
        Update-ResultList -MachineData $machineData -AvailabilityMetrics $availabilityMetrics -SuppressionDuration $suppressionDuration -Workspace $Workspace -ResultRow $resultRow
    }
}

function Merge-MachineWithStatus {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $ResultRow
    )

    $machineStart = (Get-Date ([datetime]$ResultRow.FirstHeartbeat) -Second 0).ToUniversalTime()
    $machineEnd = (Get-Date ([datetime]$ResultRow.LastHeartbeat) -Second 0).ToUniversalTime()
    $machineName = $ResultRow.Resource
    $resourceType = $ResultRow.ResourceType

    # Map status based on resource type
    if ($resourceType -eq "virtualMachines") {
        $status = $script:VmStatusById[$ResultRow._ResourceId] ?? 'unknown/deleted'
    }
    else {
        $status = $script:ArcMachinesStatusById[$ResultRow._ResourceId] ?? 'unknown/deleted'
    }

    return @{
        Start          = $machineStart
        End            = $machineEnd
        Name           = $machineName
        Status         = $status
        ResourceId     = $ResultRow._ResourceId
        ResourceGroup  = $ResultRow.RG
        SubscriptionId = $ResultRow.SubscriptionId
        ResourceType   = $resourceType
    }
}

function Get-SuppressionDuration {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $MachineData
    )

    try {
        $duration = 0
        foreach ($rule in $script:SuppressionRulesInTenant) {  
            $rule | Where-Object { $MachineData.ResourceId -in $_.Scopes } | ForEach-Object {
                $ruleStart = if ($_.effectiveFrom) { ($_.effectiveFrom).ToUniversalTime() } else { [datetime]::MinValue }
                $ruleEnd = if ($_.effectiveUntil) { ($_.effectiveUntil).ToUniversalTime() } else { [datetime]::MaxValue }

                $overlapStart = if ($ruleStart -gt $MachineData.Start) { $ruleStart } else { $MachineData.Start }
                $overlapEnd = if ($ruleEnd -lt $MachineData.End) { $ruleEnd }   else { $MachineData.End }

                if ($overlapEnd -gt $overlapStart) {
                    $duration += [math]::Round(($overlapEnd - $overlapStart).TotalMinutes, 0)
                }
            }
        }
        return $duration
    }
    catch {
        Write-Log "Could not calculate suppression duration for Machine $($MachineData.Name): $_" -Severity Error
        return 0
    }
}

function Measure-AvailabilityMetrics {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $ResultRow,
        [Parameter(Mandatory = $true)]
        [int]$SuppressionDuration
    )

    $totalHoursInMonth = if ($ResultRow.total_hours_in_month) { [double]$ResultRow.total_hours_in_month } else { 0.0 }
    $availabilityRate = if ($ResultRow.availability_rate) { [math]::Round([double]$ResultRow.availability_rate, 2) } else { 0.0 }
    # Convert hours to minutes and ensure minimum values are not zero
    $totalMinutesInMonth = [math]::Max(1, [math]::Round($totalHoursInMonth * 60, 0))
    $downMinutes = [math]::Max(0, [math]::Round([double]$ResultRow.total_down_hours * 60, 0))
    
    if ($SuppressionDuration -gt 0) {
        # Take suppression time into availability calculation
        $adjustedTotalMinutes = [math]::Max(1, $totalMinutesInMonth - $SuppressionDuration)
        $adjustedDownMinutes = [math]::Max(0, $downMinutes - $SuppressionDuration)
        $adjustedAvailableMinutes = [math]::Max(0, $adjustedTotalMinutes - $adjustedDownMinutes)
        
        if ($adjustedTotalMinutes -gt 0) {
            $availabilityRateWithSuppression = [math]::Round(100.0 * $adjustedAvailableMinutes / $adjustedTotalMinutes, 2)
        }
    }
    else {
        $availabilityRateWithSuppression = $availabilityRate
    }

    return @{
        TotalHoursInMonth               = $totalHoursInMonth
        AvailabilityRate                = $availabilityRate
        AvailabilityRateWithSuppression = $availabilityRateWithSuppression
        DownRate                        = [double]$ResultRow.down_rate
        DownRateWithSuppression         = [math]::Round((100 - $availabilityRateWithSuppression), 2)
        TotalHoursDown                  = [math]::Round($ResultRow.total_down_hours, 2)
        TotalHoursAvailable             = [math]::Round($ResultRow.total_available_hours, 2)
        UnobservedHours                 = [math]::Round($totalHoursInMonth - [math]::Round((([datetime]$ResultRow.LastHeartbeat) - ([datetime]$ResultRow.FirstHeartbeat)).TotalHours, 3), 0)
    }
}

function Update-ResultList {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        [hashtable]$MachineData,
        [Parameter(Mandatory = $true)]
        [hashtable]$AvailabilityMetrics,
        [Parameter(Mandatory = $true)]
        [int]$SuppressionDuration,
        [Parameter(Mandatory = $true)]
        $Workspace,
        [Parameter(Mandatory = $true)]
        $ResultRow
    )

    # Check whether the machine is already in the results list
    $existingEntries = $QueryResultList | Where-Object {
        $_.MachineName -eq $MachineData.Name -and 
        $_.SubscriptionId -eq $MachineData.SubscriptionId -and 
        $_.ResourceGroup -eq $MachineData.ResourceGroup -and 
        $_.FirstHeartbeat -eq $MachineData.Start.ToString("u") -and 
        $_.LastHeartbeat -eq $MachineData.End.ToString("u") -and 
        ([math]::Round($SuppressionDuration / 60, 2) -eq [double]($_.'Suppression Duration (h)'))
    }

    if ($existingEntries.Count -le 0) {
        $QueryResultList.Add([PSCustomObject]@{
                TimeRangeStart                                  = ([datetime]$ResultRow.TimeRangeStart).ToUniversalTime().ToString("u")
                TimeRangeEnd                                    = ([datetime]$ResultRow.TimeRangeEnd).ToUniversalTime().ToString("u")
                SubscriptionId                                  = $MachineData.SubscriptionId
                LAW                                             = $Workspace.Name
                ResourceGroup                                   = $MachineData.ResourceGroup
                MachineName                                     = $MachineData.Name
                Status                                          = $MachineData.Status
                ResourceType                                    = $MachineData.ResourceType
                FirstHeartbeat                                  = $MachineData.Start.ToString("u")
                LastHeartbeat                                   = $MachineData.End.ToString("u")
                "down_rate (%)"                                 = $AvailabilityMetrics.DownRate
                "Down Rate considering suppression (%)"         = $AvailabilityMetrics.DownRateWithSuppression
                "availability_rate (%)"                         = $AvailabilityMetrics.AvailabilityRate
                "Availability Rate considering suppression (%)" = $AvailabilityMetrics.AvailabilityRateWithSuppression
                "Suppression Duration (h)"                      = [math]::Round($SuppressionDuration / 60, 2)
                "Down (h)"                                      = $AvailabilityMetrics.TotalHoursDown
                "Available (h)"                                 = $AvailabilityMetrics.TotalHoursAvailable
                "Time without observation (h)"                  = $AvailabilityMetrics.UnobservedHours
                "Total hours in month"                          = $AvailabilityMetrics.TotalHoursInMonth
            })
    }
    else {
        # Update the existing LAW field entry
        foreach ($entry in $existingEntries) {
            try {
                $QueryResultList[$QueryResultList.IndexOf($entry)].LAW = Merge-Law -existingLAW $entry.LAW -newLAW $Workspace.Name
            }
            catch {
                Write-Log "ERROR while merging LAW values for Machine: $($MachineData.Name) in Subscription $($MachineData.SubscriptionId). $_" -Severity Error
            }
        }
    }
}

$global:CurrentWorkspaceIndex = 1
$QueryResultList = New-Object System.Collections.Generic.List[PSCustomObject]

foreach ($workspace in $LogAnalyticsWorkspacesInTenant) {
    Invoke-DataPerLAW -Workspace $workspace -VMHeartbeatsKQL $VMHeartbeatsKQL -ArcMachineHeartbeatsKQL $ArcMachineHeartbeatsKQL
}
$queryMonth = ([datetime]$QueryResultList.TimeRangeStart[0]).ToString("MMM")
$QueryResultList | Sort-Object ResourceType, MachineName | Export-Csv -Path "$($ExportFilePath)${queryMonth}_$($LogSessionId).csv" -Delimiter "," -Encoding UTF8

# Count unique VMs and Arc machines in the result list
$vmCount = ($QueryResultList | Where-Object { $_.ResourceType -eq 'virtualMachines' }).Count
$arcCount = ($QueryResultList | Where-Object { $_.ResourceType -eq 'machines' }).Count


Write-Log "For month $($queryMonth): Queried data from $($QueryResultList.Count) Machines. VM count: $vmCount. Arc machine count: $arcCount."

### Identify subscriptions that were in the input list but not found in the results
$foundSubscriptionIds = $QueryResultList | Select-Object -ExpandProperty SubscriptionId -Unique
$unresolvedSubscriptions = @()
foreach ($subId in $subscriptions) {
    if ($foundSubscriptionIds -notcontains $subId) {
        $unresolvedSubscriptions += $subId
    }
}
### Identify Machines that had no data in LAW
$noDataInLAW = $QueryResultList | Where-Object { $subscriptions -notcontains $($_.SubscriptionId) }

Write-Log "Subscriptions not found in results: $($unresolvedSubscriptions.Count)" -Color Yellow -Debug
if ($unresolvedSubscriptions.Count -gt 0) {
    Write-Log "Unresolved subscriptions: $($unresolvedSubscriptions -join ', ')" -Info
    $($unresolvedSubscriptions) | fl
}
if ($noDataInLAW.Count -gt 0) {
    Write-Log "Machines with no data in LAW: $($noDataInLAW.Count)" -Info
    Write-Log "Unresolved Machines: $($noDataInLAW.MachineName -join ', ')" -Info
    $noDataInLAW | fl -Property SubscriptionId, ResourceGroup, MachineName, ResourceType
}


Write-Log "Script started at: $scriptStartTime. Script ended at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -Color Green