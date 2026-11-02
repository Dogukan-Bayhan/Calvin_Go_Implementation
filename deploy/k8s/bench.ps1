<#
.SYNOPSIS
  Çalışan Calvin kümesine karşı süreli bir yük testi (Kubernetes Job) çalıştırır.

.DESCRIPTION
  Doğrulama, kümede tek bir yük üreticisi olduğunu varsayar. Bu yüzden sürekli
  çalışan calvin-client geçici olarak 0'a ölçeklenir, "calvin-client bench"
  bir Job olarak çalıştırılır, sonuç raporu yazdırılır ve istemci geri açılır.
  Rapor; commit/abort sayıları, saniyedeki commit, checkpoint tutarlılığı ve
  replikaların birebir aynı olup olmadığını içerir. Doğrulama başarısızsa Job
  başarısız olur.

.EXAMPLE
  .\deploy\k8s\bench.ps1 -Duration 60s -Clients 256 -MultiPartition 0.5
#>
param(
    [string]$Duration = "30s",
    [int]$Clients = 128,
    [double]$MultiPartition = 0.1,
    [double]$Contention = 0.01,
    [ValidateSet("micro", "bank", "mixed")][string]$Workload = "mixed"
)

$inv = [System.Globalization.CultureInfo]::InvariantCulture
$ns = "calvin"
$cm = kubectl -n $ns get deployment calvin-client -o "jsonpath={.spec.template.spec.containers[0].envFrom[0].configMapRef.name}"
if ($LASTEXITCODE -ne 0 -or -not $cm) { throw "calvin-client bulunamadı; önce up.ps1 çalıştırın." }

$job = "calvin-bench-" + (Get-Date -Format "HHmmss")
$manifest = @"
apiVersion: batch/v1
kind: Job
metadata:
  name: $job
  namespace: $ns
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    metadata:
      labels:
        app: calvin-client
        role: bench
    spec:
      restartPolicy: Never
      containers:
        - name: bench
          image: calvin:dev
          imagePullPolicy: IfNotPresent
          command: ["calvin-client"]
          args:
            - -replicas=`$(REPLICAS)
            - -partitions=`$(PARTITIONS)
            - -accounts=`$(ACCOUNTS)
            - -records=`$(RECORDS)
            - -customers=`$(CUSTOMERS)
            - -peer-template=calvin-node-%d.calvin-node:7000
            - -workload=$Workload
            - -clients=$Clients
            - -mp=$($MultiPartition.ToString($inv))
            - -ci=$($Contention.ToString($inv))
            - -duration=$Duration
            - bench
          envFrom:
            - configMapRef:
                name: $cm
"@

Write-Host "==> sürekli istemci durduruluyor" -ForegroundColor Cyan
kubectl -n $ns scale deployment/calvin-client --replicas=0 | Out-Null
kubectl -n $ns wait --for=delete pod -l "app=calvin-client,role!=bench" --timeout=60s 2>$null | Out-Null

try {
    Write-Host "==> bench Job'u başlatılıyor ($job, $Duration, $Clients istemci)" -ForegroundColor Cyan
    $manifest | kubectl apply -f - | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Job oluşturulamadı" }

    $status = ""
    while ($true) {
        Start-Sleep -Seconds 3
        $succeeded = kubectl -n $ns get job $job -o "jsonpath={.status.succeeded}"
        $failed = kubectl -n $ns get job $job -o "jsonpath={.status.failed}"
        if ($succeeded -eq "1") { $status = "BAŞARILI"; break }
        if ($failed -and [int]$failed -gt 0) { $status = "BAŞARISIZ"; break }
    }
    Write-Host ""
    # stdout'taki JSON rapor (log satırları da JSON; raporu çok satırlı bloktan ayır)
    kubectl -n $ns logs "job/$job"
    Write-Host ""
    $color = "Red"
    if ($status -eq "BAŞARILI") { $color = "Green" }
    Write-Host "Bench sonucu: $status" -ForegroundColor $color
}
finally {
    Write-Host "==> sürekli istemci yeniden başlatılıyor" -ForegroundColor Cyan
    kubectl -n $ns scale deployment/calvin-client --replicas=1 | Out-Null
}
