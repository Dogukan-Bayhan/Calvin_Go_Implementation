# Calvin (Go)

Thomson ve ark., *Calvin: Fast Distributed Transactions for Partitioned Database Systems* (SIGMOD '12) makalesinin Go implementasyonu. İki şekilde çalışır:

- **Tek süreç:** tüm düğümler aynı süreçte goroutine olarak çalışır; ağ gecikmesi simüle edilir. Testler ve `calvin-lab` bu modu kullanır.
- **Dağıtık:** her düğüm ayrı bir süreç/pod'dur ve düğümler gerçek ağ üzerinden RPC ile konuşur (`calvin-node`). İstemci `calvin-client` (ve `client` paketi) düğümlere istek gönderir.

Makale bölümleri ile dosyaların eşleşmesi için [doc.go](doc.go) dosyasına bakın.

## Hızlı başlangıç

```powershell
go test -race ./...                                  # birim, entegrasyon ve gerçek TCP üzerinde dağıtık testler
.\deploy\k8s\up.ps1 -Replicas 3 -Partitions 4        # Kubernetes (kind) üzerinde 12 düğümlü küme + istemci + Grafana
.\deploy\k8s\bench.ps1 -Duration 60s -Clients 256    # süreli yük testi ve doğrulama raporu
docker compose up -d --build                         # tek süreçli lab + Grafana (Kubernetes'siz)
```

## Kubernetes ortamı

Gereksinimler: Docker Desktop, `kubectl`, `kind` (`go install sigs.k8s.io/kind@v0.27.0`).

```
kind kümesi (1 control-plane + 3 worker)
└── namespace calvin
    ├── calvin-node-0 … calvin-node-(R·P−1)   StatefulSet: her pod bir Calvin düğümü
    │     pod i  →  replika i / P, partition i % P
    │     birbirini headless servis DNS'iyle bulur: calvin-node-i.calvin-node:7000
    ├── calvin-client                          Deployment: sürekli yük + doğrulama + kontrol API'si
    ├── prometheus                             düğümleri DNS ile bulur (düğüm sayısından bağımsız)
    ├── loki + alloy                           pod loglarını Kubernetes API'sinden toplar
    └── grafana                                hazır "Calvin Lab" dashboard'u
```

| Adres | Ne |
|---|---|
| http://localhost:3300 | Grafana |
| http://localhost:9300 | Prometheus |
| http://localhost:2213/api/settings | İstemci kontrol API'si |

### Düğüm sayısını seçmek

```powershell
.\deploy\k8s\up.ps1 -Replicas 3 -Partitions 4                 # 12 pod, Paxos (varsayılan)
.\deploy\k8s\up.ps1 -Replicas 5 -Partitions 4 -Mode async     # 20 pod
.\deploy\k8s\up.ps1 -Replicas 2 -Partitions 8 -Clients 256 -MultiPartition 0.5
.\deploy\k8s\up.ps1 -SkipBuild ...                            # kod değişmediyse imajı yeniden derleme
.\deploy\k8s\down.ps1                                         # kind kümesini sil
```

`up.ps1` her çalıştığında Calvin düğümlerini ve istemciyi **sıfırdan** başlatır (durum bellektedir); gözlem yığını korunur. Topoloji ve iş yükü parametreleri tek bir ConfigMap'ten (`calvin-topology`) hem düğümlere hem istemciye geçer.

### Yük testi

`bench.ps1`, sürekli çalışan istemciyi geçici olarak durdurur, `calvin-client bench`'i bir Job olarak çalıştırır ve raporu yazdırır: commit/abort sayıları, saniyedeki commit, checkpoint tutarlılığı, replikaların birebir aynı olup olmadığı. Doğrulama başarısızsa Job başarısız olur.

```powershell
.\deploy\k8s\bench.ps1 -Duration 30s -Clients 256 -MultiPartition 0.5 -Workload bank
```

### İstemci (`calvin-client`)

```
calvin-client [bayraklar] run          sürekli yük + periyodik checkpoint/doğrulama + HTTP kontrol API'si
calvin-client [bayraklar] bench        -duration boyunca yük, ardından doğrulama ve JSON rapor
calvin-client [bayraklar] stats        düğüm durumları
calvin-client [bayraklar] verify       replikaları ve değişmezleri doğrula
calvin-client [bayraklar] checkpoint   checkpoint al ve replikalar arasında karşılaştır
calvin-client [bayraklar] exec PROC [ARGS] -keys k1,k2 [-write k1]
```

Küme içinden tek seferlik komutlar:

```powershell
kubectl -n calvin exec deploy/calvin-client -- calvin-client -replicas 3 -partitions 4 stats
kubectl -n calvin exec deploy/calvin-client -- calvin-client -replicas 3 -partitions 4 checkpoint
```

Go'dan kullanım ([client/client.go](client/client.go)):

```go
cl := client.New([]string{"calvin-node-0.calvin-node:7000", /* ... */})
res, err := cl.Exec(ctx, calvin.Request{Proc: "transfer", Args: args, ReadSet: keys, WriteSet: keys})
```

Doğrulama, kümede **tek bir yük üreticisi** olduğunu varsayar (yükü durdurup işlemleri sayar). `exec` ile elle yapılan yazmalar, çalışan istemcinin değişmez kontrolünü bozar. İstemci başlarken kümenin mevcut durumunu başlangıç kabul eder, bu yüzden istemci yeniden başlatılabilir.

## Tek süreçli lab (`docker compose`)

`docker compose up -d --build` tek süreçli `calvin-lab`'ı ve gözlem yığınını başlatır: Grafana http://localhost:3000, Prometheus http://localhost:9090, lab API'si http://localhost:2112. Disk simülasyonu ve prefetch (§4) yalnızca bu modda vardır: `-disk-latency=10ms -prefetch=15ms`.

Kontrol API'si iki ortamda aynıdır:

```powershell
curl.exe -X POST localhost:2213/api/settings -d '{\"clients\":512,\"multipartition\":0.5}'
curl.exe -X POST localhost:2213/api/verify
curl.exe -X POST localhost:2213/api/checkpoint
```

Ayarlar: `workload` (micro|bank|mixed), `clients`, `multipartition`, `contention`, `ollp`, `reindex`, `think_ms`.

## İş yükleri ve doğrulama

- **micro**: makalenin §6.2 microbenchmark'ı. 10 kayıt okunur ve sayaçları artırılır; çekişme indeksi ve çok partition'lı işlem oranı ayarlanabilir.
- **bank**: transferler (bakiye yetmezse deterministik abort), `deposit_by_name` (ikincil indeks üzerinden OLLP, §3.2.1) ve `reindex` (OLLP restart'larını tetikler).

Doğrulayıcı yükü kısa süre durdurur, tüm düğümlerin işini bitirmesini bekler ve iki şeyi denetler:

- tüm replikaların durumu **birebir aynı** mı,
- **değişmezler** sağlanıyor mu: sayaç toplamı = başlangıç + 10 × commit; bakiye toplamı = başlangıç + deposit; indeksler geçerli hesapları gösteriyor.

Ayrıca her checkpoint'in tüm replikalarda aynı olduğu kontrol edilir.

## Dashboard'da neye bakılır

- **Doğruluk satırı**: üç kutunun hepsi yeşil olmalı, "Başarısız doğrulama" 0 kalmalı.
- **Kilit tutma süresi (contention footprint)**: makalenin ana fikri. 2PC olmadığı için kilitler yalnızca uzak okuma beklemesi + yürütme süresi kadar tutulur.
- **Replika gecikmesi**: bir epoch'un liderde kesilmesinden replikada kilit istenmesine kadar geçen süre (Paxos + ağ).
- **Uçtan uca gecikme**: epoch batch'leme (ortalama yarım epoch) + replikasyon + yürütme; istemci eğrisi ağı da içerir.

## Dağıtık mod nasıl çalışır

- Düğümler arası her etkileşim (istek iletme, epoch batch'leri, Paxos, uzak okumalar, sonuç bildirimi) bir `peer` arayüzünden geçer ([peer.go](peer.go)). Aynı süreçteki düğüme doğrudan çağrı, uzak düğüme `net/rpc` ile gider ([rpc.go](rpc.go)).
- Tek yönlü mesajlar bağlantı hatasında yeniden gönderilir (en az bir kez teslimat); alıcılar tekrarları yok sayar (liderde istek, sequencer'da epoch, scheduler'da batch, mailbox'ta uzak okuma, origin'de sonuç).
- Liderlerin epoch'ları ortak bir başlangıç anından (genesis, `r0/p0`'dan alınır) duvar saatiyle sayılır. Bu yüzden makinelerin saatleri senkron olmalıdır (NTP); saat farkı doğruluğu değil yalnızca gecikmeyi etkiler.

## Bilinen sınırlar

- Çökmeden kurtarma yok. Girdi logu ve checkpoint'ler tutulur ve log her checkpoint'te kırpılır, ama replay yazılmadı. Bir düğüm pod'u yeniden başlarsa küme `up.ps1` ile yeniden kurulmalıdır.
- Paxos lideri sabittir (replica 0); lider değişimi yok.
- Disk simülasyonu ve prefetch yalnızca tek süreç modunda var.
- Aralık kilitleri ve phantom koruması yok (makalede de yok).
