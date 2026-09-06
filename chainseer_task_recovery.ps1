# Idempotent transformation of an EXISTING task. Does not change its action,
# credentials, cadence, enabled state, multiple-instance policy or power policy.
function Add-RobinhoodResumeTrigger([xml]$TaskXml) {
  $ns = 'http://schemas.microsoft.com/windows/2004/02/mit/task'
  $manager = New-Object System.Xml.XmlNamespaceManager($TaskXml.NameTable)
  $manager.AddNamespace('t', $ns)
  $triggers = $TaskXml.SelectSingleNode('/t:Task/t:Triggers', $manager)
  if ($null -eq $triggers) { throw 'Task has no triggers; refusing a blind rewrite.' }
  $policy = $TaskXml.SelectSingleNode('/t:Task/t:Settings/t:MultipleInstancesPolicy', $manager)
  if ($null -eq $policy -or $policy.InnerText -ne 'IgnoreNew') {
    throw 'Resume recovery requires IgnoreNew to prevent duplicate supervisors.'
  }
  $trigger = $triggers.SelectSingleNode("t:EventTrigger[@id='ChainseerResume']", $manager)
  if ($null -eq $trigger) {
    $trigger = $TaskXml.CreateElement('EventTrigger', $ns)
    $trigger.SetAttribute('id', 'ChainseerResume')
    [void]$triggers.AppendChild($trigger)
  }
  # This named trigger is ours; retain every unrelated trigger and setting.
  $trigger.RemoveAll()
  $trigger.SetAttribute('id', 'ChainseerResume')
  $fields = [ordered]@{
    Enabled = 'true'
    Subscription = "<QueryList><Query Id='0' Path='System'><Select Path='System'>*[System[Provider[@Name='Microsoft-Windows-Power-Troubleshooter'] and EventID=1]]</Select></Query></QueryList>"
    Delay = 'PT60S'
  }
  foreach ($field in $fields.GetEnumerator()) {
    $node = $TaskXml.CreateElement($field.Key, $ns)
    $node.InnerText = $field.Value
    [void]$trigger.AppendChild($node)
  }
  return $TaskXml.OuterXml
}
