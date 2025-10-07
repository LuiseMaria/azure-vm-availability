<#
.SYNOPSIS
    Generates a report on the availability of Azure Virtual Machines across all subscriptions and Log Analytics Workspaces.

.DESCRIPTION
    This script queries all Azure subscriptions accessible to the user, retrieves all Log Analytics Workspaces, and runs a Kusto query to calculate VM availability for the previous month. 
    It collects VM heartbeat data, calculates uptime, and exports the results to CSV files. VMs with subscription IDs not found in the current context are exported to a separate CSV for further investigation.

.PARAMETER None
    The script does not take any parameters. It uses the current Azure context and requires the Az.Accounts and Az.OperationalInsights modules.

.REQUIREMENTS
    - Az.Accounts PowerShell module
    - Az.OperationalInsights PowerShell module
    - Sufficient permissions to list subscriptions, Log Analytics Workspaces, and query Operational Insights data

.FUNCTIONS
    Get-LogAnalyticsWorkspaces
        Retrieves all Log Analytics Workspaces for a given subscription and adds them to a global list.

    Get-VMState
        Retrieves the current power state of a specified VM using the Azure REST API.

.NOTES
    - The script exports two CSV files:
        1. Machine_Availability_<Month>_<Date>.csv: Contains availability data for VMs with valid subscription IDs.
        2. Machine_Availability_Faulty_Resources_<Month>_<Date>.csv: Contains data for VMs with subscription IDs not found in the current context.
    - The script filters out VMs whose names start with "vba" or end with "-tmp".
    - The script calculates availability as the percentage of minutes the VM was available during the previous month.

.EXAMPLE
    Examples:
    1) Run for a given month for all accessible subscriptions:
        .\Machine_AvailabilityReport-vm-arcs.ps1 -ReportMonth 3
    
    2) Run for a specific set of subscriptions:
        .\Machine_AvailabilityReport-vm-arcs.ps1 -ReportMonth 3 -SubscriptionIdList '11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222'
    
    3) Run for a specific subscription index range (useful for large tenants):
        .\Machine_AvailabilityReport-vm-arcs.ps1 -ReportMonth 3 -SubRangeStartEnd 20,310
    
    4) Authenticate first (optional) and run against a single subscription:
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
    [int[]]$SubRangeStartEnd # Provide exactly two integers to define a range (e.g. 20,310)
)

# $tenantId = "xxxxxx-xxxx-xxxxx-xxxx-xxxxxxxxx"
# $subscriptionId = "xxxxxx-xxxx-xxxxx-xxxx-xxxxxxxxx"
# Connect-AzAccount -TenantId $tenantId -SubscriptionId $subscriptionId

$scriptStartTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$UtcTimeRangeStartDate = (Get-Date -Day 1 -Month $ReportMonth).ToString('yyyy-MM-dd')

function Get-EnabledSubscriptions {
    try {
        $response = Invoke-AzRestMethod -Method Get -Uri "https://management.azure.com/subscriptions?api-version=2022-12-01"
        $subs = (($response.Content | ConvertFrom-Json).value | Where-Object { $_.state -eq "Enabled" }).SubscriptionId
        if (-not $subs -or $subs.Count -eq 0) {
            Write-Host "ERROR: No available subscriptions found." -ForegroundColor Red
            exit 1
        }
        $subs = $subs | Sort-Object
        if ($SubRangeStartEnd.Count -eq 2) {
            $subs = $subs[$($SubRangeStartEnd[0])..$($SubRangeStartEnd[-1])]
            Write-Host "Generating report for subscriptions in range index $($SubRangeStartEnd[0]) to $($SubRangeStartEnd[-1])" -ForegroundColor Blue
        }
        return $($subs)
    }
    catch {
        Write-Host "Error retrieving subscriptions: $_" -ForegroundColor Red
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
        Write-Host "Error requesting status for VM with Query '$azGraphGetVMQuery': $_"-ForegroundColor Red
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
        Write-Host "Error requesting status for Arc Machines with Query '$azGraphGetArcMachinesQuery': $_"-ForegroundColor Red
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
        Write-Host "Error requesting Suppressions with Query '$($azGraphGetSuppressionRulesQuery)': $_" -ForegroundColor Red
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
    Write-Host "Starting to query Subscriptions, LAWs, Suppression Rules, VMs and Arc Machines in TenantScope..." -ForegroundColor Yellow
    $subscriptions = (Get-EnabledSubscriptions)
    Get-LogAnalyticsWorkspaces
    Get-AlertSuppressionRulesInTenant
    Get-VMsInTenant
    Get-ArcMachinesInTenant
}
else {
    Write-Host "Starting to query LAWs, Suppression Rules and VMs in for Subscription ID(s): '$($SubscriptionIdList)...'" -ForegroundColor Yellow
    $subscriptions = $SubscriptionIdList
    Get-AlertSuppressionRulesInTenant -SubscriptionIds $SubscriptionIdList
    Get-LogAnalyticsWorkspaces -SubscriptionIds $SubscriptionIdList
    Get-VMsInTenant -SubscriptionIds $SubscriptionIdList
    Get-ArcMachinesInTenant -SubscriptionIds $SubscriptionIdList
}
Write-Host "Found $($LogAnalyticsWorkspacesInTenant.Count) LAWs, $($VmsInTenant.Count) VMs, $($ArcMachinesInTenant.Count) Arc Machines and $($SuppressionRulesInTenant.Count) Suppression Rules in Tenant ($($subscriptions.Count) Subscriptions)." -ForegroundColor Yellow


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

    Write-Host "[$($global:CurrentWorkspaceIndex)/$($script:LogAnalyticsWorkspacesInTenant.count)] Querying Log Analytics Workspace: $($Workspace.name)"
    $global:CurrentWorkspaceIndex++

    try {
        # run both kql (Arc Machine Heartbeats and VM Heartbeats) for each LAW
        $ArcMachineHeartbeatsKQL, $VMHeartbeatsKQL | ForEach-Object {

            $queryResponse = Invoke-AzOperationalInsightsQuery -WorkspaceId $Workspace.WorkspaceId -Query $($_)
            if ($queryResponse -and $queryResponse.Results.Count -ge 1) {
                # Write-Host "Results found for workspace: $($Workspace.name)" -ForegroundColor Green
                
                Get-QueryResults -Results $queryResponse.Results -Workspace $Workspace
            }
        }
    }
    catch {
        Write-Host "ERROR while querying workspace: $($Workspace.name) in Subscription $($Workspace.SubscriptionId). $_" -ForegroundColor Red
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
        Write-Host "Could not calculate suppression duration for Machine $($MachineData.Name): $_" -ForegroundColor Red
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
        AvailabilityRate                = [double]$ResultRow.availability_rate
        AvailabilityRateWithSuppression = [double]$availabilityRateWithSuppression
        DownRate                        = [double]$ResultRow.down_rate
        DownRateWithSuppression         = (100 - $availabilityRateWithSuppression)
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
                "availability_rate (%)"                         = $AvailabilityMetrics.AvailabilityRate
                "Availability Rate considering suppression (%)" = $AvailabilityMetrics.AvailabilityRateWithSuppression
                "Down Rate considering suppression (%)"         = $AvailabilityMetrics.DownRateWithSuppression
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
                Write-Host "ERROR while merging LAW values for Machine: $($MachineData.Name) in Subscription $($MachineData.SubscriptionId). $_" -ForegroundColor Red
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
$dateString = Get-Date -Format "yyyyMMdd_HHmm"
$QueryResultList | Sort-Object MachineName | Export-Csv -Path "Machine_Availability_${queryMonth}_$($dateString).csv" -Delimiter "," -Encoding UTF8

# Count unique VMs and Arc machines in the result list (unique by MachineName + SubscriptionId)
$vmCount = ($QueryResultList | Where-Object { $_.ResourceType -eq 'virtualMachines' }).Count
$arcCount = ($QueryResultList | Where-Object { $_.ResourceType -eq 'machines' }).Count

Write-Host "For month $($queryMonth): Queried data from $($QueryResultList.Count) Machines. VM count: $vmCount. Arc machine count: $arcCount." -ForegroundColor Blue
Write-Host "Script started at: $scriptStartTime. Script ended at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Green