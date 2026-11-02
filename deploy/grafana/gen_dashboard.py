"""Calvin Grafana dashboard'unu üretir.

Kullanım: python deploy/grafana/gen_dashboard.py
Çıktılar: deploy/grafana/dashboards/calvin.json (docker compose) ve
deploy/k8s/base/observability/calvin-dashboard.json (Kubernetes).
"""
import json, os, sys
PROM={"type":"prometheus","uid":"prometheus"}
LOKI={"type":"loki","uid":"loki"}
panels=[]; pid=[0]
def nid():
    pid[0]+=1; return pid[0]
def row(title,y):
    panels.append({"type":"row","title":title,"id":nid(),"collapsed":False,"gridPos":{"h":1,"w":24,"x":0,"y":y},"panels":[]})
def ts(title,targets,x,y,w=8,h=8,unit="short",desc=""):
    panels.append({"type":"timeseries","title":title,"description":desc,"id":nid(),"datasource":PROM,
      "gridPos":{"h":h,"w":w,"x":x,"y":y},
      "fieldConfig":{"defaults":{"unit":unit,"custom":{"lineWidth":1,"fillOpacity":10,"showPoints":"never"}},"overrides":[]},
      "options":{"legend":{"displayMode":"list","placement":"bottom"},"tooltip":{"mode":"multi","sort":"desc"}},
      "targets":[{"datasource":PROM,"refId":chr(65+i),"expr":e,"legendFormat":l} for i,(e,l) in enumerate(targets)]})
def stat(title,expr,x,y,w=4,h=4,unit="short",mappings=None,thresholds=None,desc=""):
    d={"unit":unit}
    if mappings: d["mappings"]=mappings
    d["thresholds"]=thresholds or {"mode":"absolute","steps":[{"color":"blue","value":None}]}
    panels.append({"type":"stat","title":title,"description":desc,"id":nid(),"datasource":PROM,"gridPos":{"h":h,"w":w,"x":x,"y":y},
      "fieldConfig":{"defaults":d,"overrides":[]},
      "options":{"colorMode":"background","graphMode":"none","reduceOptions":{"calcs":["lastNotNull"],"fields":"","values":False},"textMode":"value"},
      "targets":[{"datasource":PROM,"refId":"A","expr":expr}]})
def logs(title,expr,x,y,w=24,h=10):
    panels.append({"type":"logs","title":title,"id":nid(),"datasource":LOKI,"gridPos":{"h":h,"w":w,"x":x,"y":y},
      "options":{"showTime":True,"wrapLogMessage":True,"sortOrder":"Descending","enableLogDetails":True,"prettifyLogMessage":False},
      "targets":[{"datasource":LOKI,"refId":"A","expr":expr}]})
def okmap(ok,bad):
    return [{"type":"value","options":{"1":{"text":ok,"color":"green","index":0},"0":{"text":bad,"color":"red","index":1}}}]
GREENRED={"mode":"absolute","steps":[{"color":"red","value":None},{"color":"green","value":1}]}
R="$__rate_interval"
def q(p,m,by=""):
    extra = ","+by if by else ""
    return "histogram_quantile(%s, sum by (le%s) (rate(%s_bucket[%s])))" % (p, extra, m, R)

y=0; row("Doğruluk (canlı doğrulayıcı)",y); y+=1
stat("Replikalar birebir aynı","min(calvin_lab_replicas_consistent)",0,y,mappings=okmap("AYNI","AYRIŞTI"),thresholds=GREENRED,desc="Son doğrulamada tüm replikaların durumu birebir aynı mıydı.")
stat("İş yükü değişmezleri","min(calvin_lab_invariant_ok)",4,y,mappings=okmap("SAĞLANDI","BOZULDU"),thresholds=GREENRED,desc="Micro: sayaç toplamı = 10 x commit. Bank: bakiye toplamı = başlangıç + deposit; indeksler geçerli.")
stat("Checkpoint tutarlılığı","min(calvin_lab_checkpoint_consistent)",8,y,mappings=okmap("TUTARLI","FARKLI"),thresholds=GREENRED)
stat("Başarısız doğrulama",'sum(calvin_lab_verify_runs_total{result="fail"}) or vector(0)',12,y,thresholds={"mode":"absolute","steps":[{"color":"green","value":None},{"color":"red","value":1}]})
stat("Başarılı doğrulama",'sum(calvin_lab_verify_runs_total{result="ok"}) or vector(0)',16,y)
stat("Son doğrulamadan beri","time() - max(calvin_lab_last_verify_timestamp_seconds)",20,y,unit="s")
y+=4; row("Throughput ve gecikme",y); y+=1
ts("İşlem/s (sonuca göre)",[("sum by (result) (rate(calvin_txn_total[%s]))" % R,"{{result}}")],0,y,unit="ops")
ts("Commit/s (prosedür, çok partition)",[('sum by (proc, multipartition) (rate(calvin_txn_total{result="committed"}[%s]))' % R,"{{proc}} mp={{multipartition}}")],8,y,unit="ops")
ts("Uçtan uca gecikme",[(q(0.5,"calvin_txn_latency_seconds"),"p50"),(q(0.95,"calvin_txn_latency_seconds"),"p95"),(q(0.99,"calvin_txn_latency_seconds"),"p99")],16,y,unit="s",desc="Gönderimden ilk replikada tamamlanmaya kadar: epoch batch'leme + Paxos turu + yürütme.")
y+=8
ts("p99 gecikme: tek vs çok partition, istemci",[(q(0.99,"calvin_txn_latency_seconds","multipartition"),"düğüm mp={{multipartition}}"),(q(0.99,"calvin_client_latency_seconds","workload"),"istemci {{workload}} (ağ dahil)")],0,y,unit="s")
ts("Abort / restart oranı",[('sum(rate(calvin_txn_total{result="aborted"}[%s])) / sum(rate(calvin_txn_total[%s]))' % (R,R),"deterministik abort"),('sum(rate(calvin_txn_total{result="restart"}[%s])) / sum(rate(calvin_txn_total[%s]))' % (R,R),"OLLP restart")],8,y,unit="percentunit")
ts("Eşzamanlı istemci",[("max(calvin_lab_clients)","clients")],16,y)
y+=8; row("Contention footprint (makalenin ana metriği)",y); y+=1
ts("Kilit tutma süresi (footprint)",[(q(0.5,"calvin_contention_footprint_seconds","role"),"p50 {{role}}"),(q(0.99,"calvin_contention_footprint_seconds","role"),"p99 {{role}}")],0,y,unit="s",desc="Tüm kilitlerin alınmasından bırakılmasına kadar. 2PC olmadığı için yalnızca uzak okuma beklemesi + yürütme kadardır.")
ts("Kilit bekleme",[(q(0.5,"calvin_lock_wait_seconds","role"),"p50 {{role}}"),(q(0.99,"calvin_lock_wait_seconds","role"),"p99 {{role}}")],8,y,unit="s")
ts("Uzak okuma bekleme (aşama 4)",[(q(0.5,"calvin_remote_read_wait_seconds"),"p50"),(q(0.99,"calvin_remote_read_wait_seconds"),"p99")],16,y,unit="s",desc="Yüksekse bazı düğümler geride kalıyor (execution progress skew, §6.2).")
y+=8; row("Sequencer, scheduler, replikasyon",y); y+=1
ts("Replika gecikmesi",[("max by (replica) (calvin_schedule_delay_seconds)","replica {{replica}}")],0,y,w=6,unit="s",desc="Bir epoch'un liderde kesilmesinden replikanın en geride kalan scheduler'ında kilit istenmesine kadar geçen süre: replikasyon (Paxos) + ağ + kuyruk.")
ts("Scheduler'daki işlemler",[("sum by (replica) (calvin_scheduler_inflight)","replica {{replica}}")],6,y,w=6)
ts("Batch boyutu",[(q(0.5,"calvin_batch_txns"),"p50"),(q(0.99,"calvin_batch_txns"),"p99")],12,y,w=6)
ts("Paxos turu",[(q(0.5,"calvin_paxos_round_seconds"),"p50"),(q(0.99,"calvin_paxos_round_seconds"),"p99")],18,y,w=6,unit="s")
y+=8; row("Checkpoint, disk, kaynaklar",y); y+=1
ts("Checkpoint ve doğrulama süresi",[(q(0.99,"calvin_checkpoint_duration_seconds"),"p99 düğüm checkpoint"),(q(0.99,"calvin_lab_verify_pause_seconds"),"p99 doğrulama duraklaması")],0,y,w=6,unit="s")
ts("Girdi logu (batch)",[("sum by (replica) (calvin_input_log_batches)","replica {{replica}}")],6,y,w=6,desc="Checkpoint sonrası kırpılır; sürekli artıyorsa checkpoint çalışmıyor.")
ts("Disk takılmaları/s",[("sum by (replica) (rate(calvin_disk_stalls_total[%s]))" % R,"replica {{replica}}")],12,y,w=6,unit="ops",desc="Prefetch yetersizse kilit tutulurken diske gidilir (yalnızca -disk-latency ile).")
ts("Go çalışma zamanı",[("sum(go_goroutines)","goroutines"),("sum(go_memstats_heap_inuse_bytes) / 1e6","heap MB")],18,y,w=6)
y+=8; row("Loglar (Loki)",y); y+=1
logs("Uyarı ve hatalar",'{job=~"calvin.*", level=~"WARN|ERROR"}',0,y,h=8)
y+=8
logs("Tüm loglar (işlem başı debug logları hariç)",'{job=~"calvin.*"} != "txn completed"',0,y,h=12)

dash={"uid":"calvin-lab","title":"Calvin Lab","tags":["calvin"],"timezone":"browser","schemaVersion":39,"version":1,
 "refresh":"5s","time":{"from":"now-15m","to":"now"},"editable":True,"panels":panels,"templating":{"list":[]},"annotations":{"list":[]}}
here = os.path.dirname(os.path.abspath(__file__))
outs = sys.argv[1:] or [
    os.path.join(here, "dashboards", "calvin.json"),
    os.path.join(here, "..", "k8s", "base", "observability", "calvin-dashboard.json"),
]
for out in outs:
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        json.dump(dash, f, ensure_ascii=False, indent=2)
    print(len(panels), "panels ->", os.path.normpath(out))
