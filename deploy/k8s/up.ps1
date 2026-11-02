<#
.SYNOPSIS
  Yerel bir kind kümesinde Calvin'i istenen boyutta ayağa kaldırır.

.DESCRIPTION
  1. kind kümesi yoksa oluşturur (1 control-plane + 3 worker).
  2. calvin:dev imajını derler ve kümeye yükler.
  3. Replicas x Partitions adet calvin-node pod'u, bir calvin-client ve
     gözlem yığınını (Prometheus, Loki, Alloy, Grafana) kurar.

  Calvin düğümlerinin durumu bellektedir; bu script her çalıştığında Calvin
  kümesini (düğümler + istemci) sıfırdan başlatır. Gözlem yığını korunur.

.EXAMPLE
  .\deploy\k8s\up.ps1 -Replicas 3 -Partitions 4
  .\deploy\k8s\up.ps1 -Replicas 2 -Partitions 6 -Mode async -Clients 128 -MultiPartition 0.5
#>
param(
    [ValidateRange(1, 9)][int]$Replicas = 3,
    [ValidateRange(1, 64)][int]$Partitions = 4,
    [ValidateSet("paxos", "async")][string]$Mode = "paxos",
    [ValidateSet("micro", "bank", "mixed")][string]$Workload = "mixed",
    [int]$Clients = 64,
    [double]$MultiPartition = 0.1,
    [double]$Contention = 0.01,
    [int]$Accounts = 1000,
    [int]$Records = 10000,
    [int]$Customers = 100,
    [string]$ClusterName = "calvin",
    [switch]$SkipBuild
)

$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$inv = [System.Globalization.CultureInfo]::InvariantCulture

function Invoke-Native {
    param([string]$What, [scriptblock]$Cmd)
    Write-Host "==> $What" -ForegroundColor Cyan
    & $Cmd
    if ($LASTEXITCODE -ne 0) { throw "$What failed (exit $LASTEXITCODE)" }
}

$kind = (Get-Command kind -ErrorAction SilentlyContinue).Source
if (-not $kind) { $kind = Join-Path (& go env GOPATH) "bin\kind.exe" }
if (-not (Test-Path $kind)) { throw "kind bulunamadı. Kurulum: go install sigs.k8s.io/kind@v0.27.0" }

$clusters = & $kind get clusters 2>$null
if (-not ($clusters -contains $ClusterName)) {
    Invoke-Native "kind kümesi oluşturuluyor ($ClusterName)" {
        & $kind create cluster --name $ClusterName --config (Join-Path $PSScriptRoot "kind-config.yaml")
    }
}
Invoke-Native "kubectl bağlamı" { kubectl config use-context "kind-$ClusterName" | Out-Null }

if (-not $SkipBuild) {
    Invoke-Native "imaj derleniyor (calvin:dev)" { docker build -t calvin:dev $root }
}
Invoke-Native "imaj kümeye yükleniyor" { & $kind load docker-image calvin:dev --name $ClusterName }

# Topolojiyi bir overlay ile uygula: StatefulSet boyutu ve ConfigMap birlikte değişir.
$overlay = Join-Path $PSScriptRoot "overlay"
New-Item -ItemType Directory -Force $overlay | Out-Null
$nodes = $Replicas * $Partitions
$kustomization = @"
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../base
replicas:
  - name: calvin-node
    count: $nodes
configMapGenerator:
  - name: calvin-topology
    namespace: calvin
    behavior: replace
    literals:
      - REPLICAS=$Replicas
      - PARTITIONS=$Partitions
      - MODE=$Mode
      - ACCOUNTS=$Accounts
      - RECORDS=$Records
      - CUSTOMERS=$Customers
      - WORKLOAD=$Workload
      - CLIENTS=$Clients
      - MP=$($MultiPartition.ToString($inv))
      - CI=$($Contention.ToString($inv))
"@
# PowerShell 5.1'in Set-Content'i BOM ekler; kustomize için BOM'suz yaz.
[System.IO.File]::WriteAllText((Join-Path $overlay "kustomization.yaml"), $kustomization)

# Düğüm durumu bellekte olduğu için Calvin kümesini sıfırdan başlat.
Invoke-Native "önceki Calvin düğümleri ve istemci siliniyor" {
    kubectl -n calvin delete statefulset/calvin-node deployment/calvin-client --ignore-not-found --cascade=foreground --wait=true 2>$null
}
Invoke-Native "manifest'ler uygulanıyor ($Replicas replika x $Partitions partition = $nodes düğüm)" {
    kubectl apply -k $overlay
}
Invoke-Native "düğümler bekleniyor" { kubectl -n calvin rollout status statefulset/calvin-node --timeout=5m }
foreach ($d in "calvin-client", "prometheus", "loki", "alloy", "grafana") {
    Invoke-Native "$d bekleniyor" { kubectl -n calvin rollout status deployment/$d --timeout=5m }
}

Write-Host ""
kubectl -n calvin get pods -o wide
Write-Host ""
Write-Host "Grafana        http://localhost:3300" -ForegroundColor Green
Write-Host "Prometheus     http://localhost:9300" -ForegroundColor Green
Write-Host "İstemci API'si http://localhost:2213/api/settings" -ForegroundColor Green
