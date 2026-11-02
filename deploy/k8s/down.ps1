<#
.SYNOPSIS
  Calvin kind kümesini tamamen siler.
#>
param([string]$ClusterName = "calvin")

$kind = (Get-Command kind -ErrorAction SilentlyContinue).Source
if (-not $kind) { $kind = Join-Path (& go env GOPATH) "bin\kind.exe" }
& $kind delete cluster --name $ClusterName
