// Package calvin, Thomson ve ark. "Calvin: Fast Distributed Transactions for
// Partitioned Database Systems" (SIGMOD '12) makalesinin tek süreç içinde
// çalışan bir Go implementasyonudur. Düğümler (replica x partition) goroutine
// olarak çalışır; aralarındaki ağ, isteğe bağlı gecikmeli mesajlarla simüle
// edilir.
//
// Makale bölümleri ile kod eşleşmesi:
//
//   - §3   Üç katman: sequencer (sequencer.go), scheduler (scheduler.go,
//     worker.go), storage (storage.go). Storage katmanı basit bir CRUD
//     arayüzüdür (Engine) ve takılıp çıkarılabilir.
//   - §3.1 Sequencer zamanı 10 ms'lik epoch'lara böler, her epoch sonunda
//     topladığı istekleri bir batch'e çevirir, replike eder ve kendi
//     replikasındaki her scheduler'a yalnızca onun ilgilendiği işlemleri
//     gönderir.
//   - §3.1.1 Asenkron (master -> slave) ve Paxos tabanlı senkron replikasyon
//     (paxos.go). Replike edilen şey işlemlerin etkisi değil, girdisidir.
//   - §3.2 Deterministik kilit yöneticisi: kilitler tek bir goroutine'de,
//     global seri sırayla istenir ve her anahtar için FIFO sırayla verilir.
//     Worker'lar beş aşamada çalışır: okuma/yazma kümesi analizi, yerel
//     okumalar, uzak okumaları servis etme, uzak okumaları toplama, mantığı
//     çalıştırıp yerel yazmaları uygulama. İki fazlı commit yoktur.
//   - §3.2.1 Bağımlı işlemler için OLLP (keşif sorgusu + deterministik yeniden
//     başlatma).
//   - §4   Disk tabanlı depolama: sequencer soğuk kayıtlara erişen işlemleri
//     yapay olarak geciktirir ve bu sırada kayıtları belleğe ısıtır.
//   - §5   Sanal tutarlılık noktası kullanan Zig-Zag benzeri asenkron
//     checkpoint: global sıradaki bir işaretçiden önceki işlemler "before",
//     sonrakiler "after" sürümlerine yazar.
package calvin
