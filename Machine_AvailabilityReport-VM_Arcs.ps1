<#
.SYNOPSIS
    Generates a monthly availability report for Azure Virtual Machines and Azure Arc-enabled servers by querying Log Analytics workspaces across one or more subscriptions.

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
    [int] Required. Month (1-12) to run the report for. If omitted, the script defaults to the previous calendar month
    of the current year.

.PARAMETER SubscriptionIdList
    [string[]] Optional. Array of subscription IDs to restrict processing to. When not supplied, the script enumerates all
    subscriptions available in the current Az context.

.PARAMETER SubRangeStartEnd
    [int[]] Optional. Two-element array or comma-separated pair (start,end). When supplied, the script processes only the
    subset of subscriptions in the enumerated list from index start (inclusive) to end (inclusive). Useful for batching
    work across large tenants. For Example, to process subscriptions 20 through end, use "-SubRangeStartEnd 20,-1".

.PARAMETER ExportFilePath
    [string] Optional. Directory path to save the output CSV files. Default: current directory.

.PARAMETER ResourceScope
    [string] Optional. Type of resources to include in the report. Valid values are 'All' (default), 'VM', or 'Arc'.
    - 'All': Includes both Azure Virtual Machines and Azure Arc-enabled servers.
    - 'VM': Includes only Azure Virtual Machines.
    - 'Arc': Includes only Azure Arc-enabled servers.

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
    .\Machine_AvailabilityReport-VM_Arcs.ps1

    # Specific month and subscription list
    .\Machine_AvailabilityReport-VM_Arcs.ps1 -ReportMonth 3 -SubscriptionIdList '11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222'

    # Process a subset of subscriptions by index range
    .\Machine_AvailabilityReport-VM_Arcs.ps1 -ReportMonth 3 -SubRangeStartEnd 20,310

    # Authenticate first (optional) and run against a single subscription:
    Connect-AzAccount -TenantId '<tenant-id>' -SubscriptionId '<subscription-id>'
    .\Machine_AvailabilityReport-VM_Arcs.ps1 -ReportMonth 3 -SubscriptionIdList '<subscription-id>'

    # Report VMs only 
    .\Machine_AvailabilityReport-VM_Arcs.ps1 -ReportMonth 3 -ResourceScope VM

    # Report Arc machines only
    .\Machine_AvailabilityReport-VM_Arcs.ps1 -ReportMonth 3 -ResourceScope Arc

    # Report both VMs and Arc machines (default behavior)
    .\Machine_AvailabilityReport-VM_Arcs.ps1 -ReportMonth 3 -ResourceScope All

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

    [string[]]$SubscriptionIdList, # Optional: Specify Subscription IDs to limit the report to those subscriptions only. If not provided, all accessible subscriptions in Tenant will be included.
    [ValidateCount(2, 2)]

    [int[]]$SubRangeStartEnd, # Provide exactly two integers to define a range (e.g. 20,310)

    [string]$ExportFilePath = "./Machine_Availability_", # Optional: Directory path to save the output CSV files. Default: current directory.

    [ValidateSet('All', 'VM', 'Arc')]
    [string]$ResourceScope = 'All'
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
        [ValidateSet('Info', 'Console', 'Debug', 'Error')]
        [string]$Severity = "Info",
        [System.ConsoleColor]$Color = [System.ConsoleColor]::Blue
    )
    $filePath = "$($LogFilePath)Logfile_$($script:LogSessionId).txt"
    switch ($Severity) {
        'Info' {
            ## print only to Log file
            $logMessage = "$($Message)`n"
            $logMessage | Out-File -FilePath $filePath -Append -Encoding UTF8
            continue
        }
        'Console' {
            ## print only to console
            Write-Host "$Message" -ForegroundColor $Color
            continue
        }
        { $_ -in ('Debug', 'Error') } {
            ## print to console and log file
            if($Severity -eq 'Error') {
                $Color = "Red"
            }
            Write-Host "$Message" -ForegroundColor $Color
            $logMessage = "$($Message)`n"
            $logMessage | Out-File -FilePath $filePath -Append -Encoding UTF8
            continue
        }
    }
}

Write-Log "$scriptStartTime - Starting script for month '$ReportMonth'." -Severity Debug -Color Green


function Get-EnabledSubscriptions {
    try {
        $response = Invoke-AzRestMethod -Method Get -Uri "https://management.azure.com/subscriptions?api-version=2022-12-01" -ErrorAction Stop
        $subs = (($response.Content | ConvertFrom-Json).value | Where-Object { $_.state -eq "Enabled" }).SubscriptionId
        if (-not $subs -or $subs.Count -eq 0) {
            Write-Log "ERROR: No available subscriptions found." -Severity Error
            exit 1
        }
        $subs = $subs | Sort-Object
        if ($SubRangeStartEnd.Count -eq 2) {
            $subs = $subs[$($SubRangeStartEnd[0])..$($SubRangeStartEnd[-1])]
            Write-Log "Generating report for subscriptions in range index $($SubRangeStartEnd[0]) to $($SubRangeStartEnd[-1])" -Severity Debug
        }
        return $($subs)
    }
    catch {
        Write-Log "Error retrieving subscriptions: $_. Please run Connect-AzAccount with your credentials." -Severity Error
        exit 1
    }
}

function Get-LogAnalyticsWorkspaces {
    param (
        [string[]]$SubscriptionIds # optional, if not provided, will search with -UseTenantScope (Search-AzGraph)
    )
    $useTenantScope = -not $($SubscriptionIds)
    $azGraphGetLAWQuery = "resources | where type =~ 'microsoft.operationalinsights/workspaces' | project name, subscriptionId, WorkspaceId = tostring(properties.customerId) | sort by tolower(subscriptionId) asc"
    $skipToken = $null
    do {
        if($useTenantScope) {
            $workspaces = Search-AzGraph -UseTenantScope -First 1000 -Query $azGraphGetLAWQuery -SkipToken $skipToken
        }
        else {
            $workspaces = Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Query $azGraphGetLAWQuery -SkipToken $skipToken
        }
        $script:LogAnalyticsWorkspacesInTenant += $workspaces
        $skipToken = $workspaces.SkipToken
    } while($skipToken)
}

function Get-VMsInTenant {
    param (
        [string[]]$SubscriptionIds # optional, if not provided, will search with -UseTenantScope (Search-AzGraph)
    )
    try {
        $azGraphGetVMQuery = "resources | where type =~ 'microsoft.compute/virtualmachines' | extend timeCreated = todatetime(properties.timeCreated) | where timeCreated < (endofmonth(datetime($($UtcTimeRangeStartDate)))) | project name, id, subscriptionId, powerState = properties.extended.instanceView.powerState.displayStatus, timeCreated | sort by tolower(subscriptionId) asc"
        $useTenantScope = -not $($SubscriptionIds)
        $skipToken = $null
        do {

            if($useTenantScope) {
                $vmListResponse = Search-AzGraph -UseTenantScope -First 1000 -Query $azGraphGetVMQuery -SkipToken $skipToken
            }
            else {
                $vmListResponse = Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Query $azGraphGetVMQuery -SkipToken $skipToken
            }
            $script:vmsInTenant += $vmListResponse
            $skipToken = $vmListResponse.SkipToken

        } while($skipToken)        

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

        $skipToken = $null
        do {

            if($useTenantScope) {
                $arcMachineListResponse = Search-AzGraph -UseTenantScope -First 1000 -Query $azGraphGetArcMachinesQuery -SkipToken $skipToken
            }
            else {
                $arcMachineListResponse = Search-AzGraph -Subscription $SubscriptionIds -First 1000 -Query $azGraphGetArcMachinesQuery -SkipToken $skipToken
            }
            $script:ArcMachinesInTenant += $arcMachineListResponse
            $skipToken = $arcMachineListResponse.SkipToken

        } while($skipToken)  

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
        | project schedule = (properties.schedule), effectiveFrom = properties.schedule.effectiveFrom, effectiveUntil = properties.schedule.effectiveUntil, scopes = properties.scopes, name, subscriptionId | sort by tolower(subscriptionId) asc"
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

function Initialize-TenantData {
    param(
        [string[]]$SubscriptionIdListParam
    )
    try {
        # GET Suppression Rule List, Subscriptions and Workspace List &  for all Subscriptions
        $script:SuppressionRulesInTenant = @()
        $script:LogAnalyticsWorkspacesInTenant = @()
        $script:VmsInTenant = @()
        $script:VmStatusById = New-Object 'System.Collections.Generic.Dictionary[string, string]' ([System.StringComparer]::OrdinalIgnoreCase)

        $script:ArcMachinesInTenant = @()
        $script:ArcMachinesStatusById = New-Object 'System.Collections.Generic.Dictionary[string, string]' ([System.StringComparer]::OrdinalIgnoreCase)

        # Resolve subscriptions (either provided or enumerate enabled ones)
        if (-not $SubscriptionIdListParam -or $SubscriptionIdListParam.Count -eq 0) {
            $subscriptions = Get-EnabledSubscriptions
            # keep the param variable in sync for downstream functions
            $script:SubscriptionIdList = $subscriptions
        }
        else {
            $subscriptions = $SubscriptionIdListParam
            $script:SubscriptionIdList = $SubscriptionIdListParam
        }

        # expose resolved subscription list to script scope (used by other functions)
        $script:Subscriptions = $subscriptions

        Write-Log "Resolved $($subscriptions.Count) subscription(s). Querying tenant data (LAWs, suppression rules, VM/Arc lists)..." -Severity Debug -Color Green

        # Retrieve Log Analytics Workspaces and suppression rules, scoped to subscriptions when provided
        if ($SubscriptionIdList -and $SubscriptionIdList.Count -gt 0) {
            Get-LogAnalyticsWorkspaces -SubscriptionIds $SubscriptionIdList
            Get-AlertSuppressionRulesInTenant -SubscriptionIds $SubscriptionIdList
            if($ResourceScope -in 'VM', 'All') {
                Get-VMsInTenant -SubscriptionIds $SubscriptionIdList
            }
            if ($ResourceScope -in 'Arc', 'All') {
                Get-ArcMachinesInTenant -SubscriptionIds $SubscriptionIdList
            }
        }
        else {
            # fallback to tenant scope
            Get-LogAnalyticsWorkspaces
            Get-AlertSuppressionRulesInTenant
            if($ResourceScope -in 'VM', 'All') {
                Get-VMsInTenant
            }
            if ($ResourceScope -in 'Arc', 'All') {
                Get-ArcMachinesInTenant
            }
        }
        Write-Log "Initialization for ResourceScope '$($ResourceScope.toUpper())': $($LogAnalyticsWorkspacesInTenant.Count) LAWs, $($VmsInTenant.Count) VMs, $($ArcMachinesInTenant.Count) Arc Machines, $($SuppressionRulesInTenant.Count) suppression rules found." -Severity Debug
    }
    catch {
        Write-Log "Initialize-TenantData failed: $_" -Severity Error
        exit 1
    }
}


Write-Log "Starting to query Subscriptions, LAWs, Suppression Rules, and optional VMs and/or Arc Machines in TenantScope..." -Severity Debug
Initialize-TenantData -SubscriptionIdListParam $SubscriptionIdList

# KQL - retrieves the uptime of VMs for the previous month, excluding VMs with names starting with "vba" or ending with "-tmp".
if($ResourceScope -in 'VM', 'All') {
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
}
else {
    $VMHeartbeatsKQL = $null
}

# KQL - retrieves the uptime of Arc Machines for the previous month.
if ($ResourceScope -in 'Arc', 'All') {
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
}
else {
    $ArcMachineHeartbeatsKQL = $null
}

function Invoke-DataPerLAW {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $Workspace,
        [string]$MachineHeartbeatsKQL
    )

    try {
        # run kql (Arc Machine Heartbeats/VM Heartbeats) for each LAW
        $queryResponse = Invoke-AzOperationalInsightsQuery -WorkspaceId $Workspace.WorkspaceId -Query $MachineHeartbeatsKQL -ErrorAction Stop
        if ($queryResponse -and $queryResponse.Results.Count -ge 1) {
            Write-Log "Results found in workspace: $($Workspace.name)" -Severity Info
            Get-QueryResults -Results $queryResponse.Results -Workspace $Workspace
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

        # Calculate suppression duration and get schedule details
        $suppressionInfo = Get-SuppressionDuration -MachineData $machineData

        # Calculate availability metrics
        $availabilityMetrics = Measure-AvailabilityMetrics -ResultRow $resultRow -SuppressionDuration  $suppressionInfo.Duration
        
        # add new entry or update LAW in the ResultList for matching SubscriptionId, FirstHeartbeat and LastHeartbeat
        Update-ResultList -MachineData $machineData -AvailabilityMetrics $availabilityMetrics -SuppressionInfo $suppressionInfo -Workspace $Workspace -ResultRow $resultRow
    }
}

function Merge-MachineWithStatus {
    [CmdletBinding()]
    param (
        [Parameter(Mandatory = $true)]
        $ResultRow
    )

    $machineStart = (Get-Date ([datetime]$ResultRow.FirstHeartbeat)).ToUniversalTime()
    $machineEnd = (Get-Date ([datetime]$ResultRow.LastHeartbeat)).ToUniversalTime()
    $machineName = $ResultRow.Resource
    $resourceType = $ResultRow.ResourceType

    # Map status based on resource type
    if ($resourceType -eq "virtualMachines") {
        $status = $script:VmStatusById[$ResultRow._ResourceId] ?? 'unknown/deleted'
    }
    elseif ($resourceType -eq "machines") {
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
        $suppressionDetails = @()
        $StartTimeResetedSeconds = (Get-Date ([datetime]$MachineData.Start) -Second 0)
        $EndTimeResetedSeconds = (Get-Date ([datetime]$MachineData.End) -Second 0)
        
        foreach ($rule in $script:SuppressionRulesInTenant) {  
            $rule | Where-Object { $MachineData.ResourceId -in $_.Scopes } | ForEach-Object {
                $ruleStart = if ($_.effectiveFrom) { ($_.effectiveFrom).ToUniversalTime() } else { [datetime]::MinValue }
                $ruleEnd = if ($_.effectiveUntil) { ($_.effectiveUntil).ToUniversalTime() } else { [datetime]::MaxValue }

                $overlapStart = if ($ruleStart -gt $StartTimeResetedSeconds) { $ruleStart } else { $StartTimeResetedSeconds }
                $overlapEnd = if ($ruleEnd -lt $EndTimeResetedSeconds) { $ruleEnd }   else { $EndTimeResetedSeconds }

                if ($overlapEnd -gt $overlapStart) {
                    $duration += [math]::Round(($overlapEnd - $overlapStart).TotalMinutes, 0)
                    
                    # Add suppression details
                    $suppressionDetails += $rule.schedule
                }
            }
        }
        
        return @{
            Duration        = $duration
            ScheduleDetails = $suppressionDetails | ForEach-Object { $_  -join '; '}
        }
    }
    catch {
        Write-Log "Could not calculate suppression duration for Machine $($MachineData.Name): $_" -Severity Error
        return @{
            Duration        = 0
            ScheduleDetails = ""
        }
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
    $totalMinutesUp = if ($ResultRow.total_minutes) { [double]$ResultRow.total_minutes } else { 0.0 }
    
    $totalMinutesDown = [math]::Max(0, [math]::Round([double]$ResultRow.total_down_hours * 60, 0))
    $actualMachineRuntimeHours  = [math]::Round($totalMinutesUp / 60, 2)
        
    if ($SuppressionDuration -gt 0) {
        # Take suppression time into availability calculation
        $adjustedTotalMinutes = [math]::Max(1, $totalMinutesUp - $SuppressionDuration)
        $adjustedDownMinutes = [math]::Max(0, $totalMinutesDown - $SuppressionDuration)
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
        UnobservedHours                 = [math]::Round($totalHoursInMonth - $actualMachineRuntimeHours, 0)
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
        $SuppressionInfo,
        [Parameter(Mandatory = $true)]
        $Workspace,
        [Parameter(Mandatory = $true)]
        $ResultRow
    )

    $QueryResultList.Add([PSCustomObject]@{
            TimeRangeStart                               = ([datetime]$ResultRow.TimeRangeStart).ToUniversalTime().ToString("u")
            TimeRangeEnd                                 = ([datetime]$ResultRow.TimeRangeEnd).ToUniversalTime().ToString("u")
            SubscriptionId                               = $MachineData.SubscriptionId
            ResourceId                                   = $MachineData.ResourceId
            LAW                                          = $Workspace.Name
            ResourceGroup                                = $MachineData.ResourceGroup
            MachineName                                  = $MachineData.Name
            Status                                       = $MachineData.Status
            ResourceType                                 = $MachineData.ResourceType
            FirstHeartbeat                               = $MachineData.Start.ToString("u")
            LastHeartbeat                                = $MachineData.End.ToString("u")
            "down_rate (%)"                              = $AvailabilityMetrics.DownRate
            "availability_rate (%)"                      = $AvailabilityMetrics.AvailabilityRate
            "AvailabilityRateConsideringSuppression (%)" = $AvailabilityMetrics.AvailabilityRateWithSuppression
            "DownRateConsideringSuppression (%)"         = $AvailabilityMetrics.DownRateWithSuppression
            "SuppressionDuration (h)"                    = [math]::Round($SuppressionInfo.Duration / 60, 2)
            "SuppressionScheduleDetails"                 = $SuppressionInfo.ScheduleDetails
            "Down (h)"                                   = $AvailabilityMetrics.TotalHoursDown
            "Available (h)"                              = $AvailabilityMetrics.TotalHoursAvailable
            "Total hours in month"                       = $AvailabilityMetrics.TotalHoursInMonth
            "Time without observation (h)"               = $AvailabilityMetrics.UnobservedHours
        })
}

$global:CurrentWorkspaceIndex = 1
$QueryResultList = New-Object System.Collections.Generic.List[PSCustomObject]
$kqlQueries = @($VMHeartbeatsKQL, $ArcMachineHeartbeatsKQL) | Where-Object { $_ }

# ------ starting to query each LAW for Heartbeats (VM/Arc Machines) ---------- 
foreach ($workspace in $LogAnalyticsWorkspacesInTenant) {
    Write-Log "[$($global:CurrentWorkspaceIndex)/$($script:LogAnalyticsWorkspacesInTenant.count)] Querying Log Analytics Workspace: $($Workspace.name)" -Severity Console -Color Black
    foreach ($kql in $kqlQueries) {
        Invoke-DataPerLAW -Workspace $workspace -MachineHeartbeatsKQL $kql
    }
    $global:CurrentWorkspaceIndex++
}


$originalCount = $QueryResultList.Count

# ------ Remove duplicates based on ResourceId, FirstHeartbeat, LastHeartbeat and Suppression Duration (h) ---------- 

# Group by the same criteria and merge LAWs for each group
$UniqueQueryResultList = $QueryResultList |
Group-Object -Property {
    $StartTimeResetedSec = (Get-Date ([datetime]($_.FirstHeartbeat)) -Second 0).ToUniversalTime().ToString("u")
    $EndTimeResetedSec = (Get-Date ([datetime]($_.LastHeartbeat)) -Second 0).ToUniversalTime().ToString("u")
    $suppressionHours = if ($_.PSObject.Properties.Match('SuppressionDuration (h)')) { [math]::Round([double]$_.('SuppressionDuration (h)'), 2) } else { 0.0 }
    "$($_.ResourceId)|$StartTimeResetedSec|$EndTimeResetedSec|$suppressionHours"
} | ForEach-Object {
    # Take the first entry as the base
    $baseEntry = $_.Group | Select-Object -First 1
    
    # If there are multiple entries in this group, merge their LAWs
    if ($_.Group.Count -gt 1) {
        try {
            # Start with the first LAW and merge others
            $mergedLAW = $baseEntry.LAW
            for ($i = 1; $i -lt $_.Group.Count; $i++) {
                $mergedLAW = Merge-Law -existingLAW $mergedLAW -newLAW $_.Group[$i].LAW
            }
            $baseEntry.LAW = $mergedLAW
        }
        catch {
            Write-Log "ERROR while merging LAW values for ResourceId: $($baseEntry.ResourceId). $_" -Severity Error
        }
    }
    return $baseEntry
}


if($UniqueQueryResultList.Count -lt $originalCount) {
    Write-Log "Duplicates: $originalCount -> $($UniqueQueryResultList.Count) entries remain." -Severity Debug -Color Yellow
}



# ------ Logging Results & exporting results to CSV ----------

$queryMonth = ([datetime]$QueryResultList.TimeRangeStart[0]).ToString("MMM", [System.Globalization.CultureInfo]::InvariantCulture)
$outputFileName = "$($ExportFilePath)$($queryMonth)_Report_$($Scope)$($LogSessionId).csv"


# Count unique VMs and Arc machines in the result list
$uniqueVmsInQueryResult = $QueryResultList | Where-Object { $_.ResourceType -eq 'virtualMachines' } | Sort-Object -Unique ResourceId
$uniqueArcsInQueryResult = $QueryResultList | Where-Object { $_.ResourceType -eq 'machines' } | Sort-Object -Unique ResourceId

if($script:VmsInTenant.Count -gt 0) {
    $vmNotInLaw = Compare-Object -ReferenceObject ($uniqueVmsInQueryResult) -DifferenceObject ($script:VmsInTenant) -Property ResourceId -PassThru | Where-Object { $_.SideIndicator -eq '=>' } | Select-Object -Property Name, ResourceId
    $Scope = "VM_"
}
if($script:ArcMachinesInTenant.Count -gt 0) {
    $arcsNotInLaw = Compare-Object -ReferenceObject ($uniqueArcsInQueryResult) -DifferenceObject ($script:ArcMachinesInTenant) -Property ResourceId -PassThru | Where-Object { $_.SideIndicator -eq '=>' } | Select-Object -Property Name, ResourceId
    $Scope += "Arc_"
}

Write-Log "Unresolved VMs: $($vmNotInLaw.Name -join ', ')" -Severity Info
Write-Log "Unresolved Arc Machines: $($arcsNotInLaw.Name -join ', ')" -Severity Info

Write-Log "Machines in LAW: $($VmsInTenant.Count) VMs, $($ArcMachinesInTenant.Count) Arc Machines. NOT in LAW: $($vmNotInLaw.Count) VMs, $($arcsNotInLaw.Count) Arc Machines" -Severity Debug -Color Magenta

Write-Log "SCOPE: '$($ResourceScope.ToUpper())' for month '$($queryMonth)': Queried data with $($UniqueQueryResultList.Count) entries. Unique VM number: $($uniqueVmsInQueryResult.Count). Unique Arc machine number: $($uniqueArcsInQueryResult.Count). ----> In total $($($uniqueVmsInQueryResult.Count) + $($uniqueArcsInQueryResult.Count)) single Machines in Report" -Severity Debug


# Replace the List with a new List containing only unique entries
$UniqueQueryResultList | Sort-Object ResourceType, MachineName -Descending | Export-Csv -Path "$($outputFileName)" -Delimiter "," -Encoding UTF8
Write-Log "Exporting results to CSV file: '$outputFileName'" -Severity Debug


$scriptRunTime = (Get-Date).Subtract([datetime]($scriptStartTime))
Write-Log "Script started at: $scriptStartTime. Script ended at: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'). Duration: $($scriptRunTime.Hours)h $($scriptRunTime.Minutes)m $($scriptRunTime.Seconds)s" -Color Green -Severity Debug