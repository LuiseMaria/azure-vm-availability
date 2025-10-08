# PowerShell Script using Azure Monitor Logs, Azure Resource Graph Explorer and Suppresion Alert Rules for calculating VM and Arc Machine Availability

### Example, how to run Script for getting Data from all LAWs in tenant e.g. from 01. Sep - 30. Sep 2025:

````powershell
    .\Machine_AvailabilityReport-vm-arcs.ps1 -ReportMonth 9
````
*Depending on how many Log Analytics Workspaces (LAWs) exist in tenant, the script may take some time to run because it retrieves data from all LAWs.
For more than 400 LAWs and > 1000 Machines, execution typically takes about 10–30 minutes.*

### KQL for VM/Arcs Availability

**1. Retrieve Heartbeats for VMs "microsoft.compute/virtualmachines"**

````kql
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
````



**2. Retrieve Heartbeats for Azure Arc Machines "microsoft.hybridcompute/machines"**

````kql
let timeRangeEnd = (endofmonth(datetime(2025-09-01)));
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
````