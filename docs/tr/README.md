# FlowCoord Türkçe Belgeleri

`flowcoord`, Python uygulamalarında eşzamanlı işleri, geçici hataları, hız/eşzamanlılık sınırlarını, zamanlanmış işleri ve görev/kaynak yaşam döngülerini güvenle yönetmek için sıfır bağımlılıklı yürütme primitifleri sunar. Her temel bileşen hem **thread tabanlı (senkron)** hem **`asyncio` tabanlı (asenkron)** simetrik API'lerle gelir.

> **Kapsam Notu:** `flowcoord` tek bir Python süreci içindeki (`in-process`) eşzamanlılık ve dayanıklılık koordinasyonu içindir. Redis tabanlı dağıtık kilitlerin, API gateway hız sınırlayıcılarının veya makine dışı iş kuyruklarının yerine geçmez.

---

## Konu Dizini

- [Nereden Başlamalısın?](#nereden-başlamalısın)
- [Doküman Haritası](#doküman-haritası)
- [Senkron / Asenkron Simetrisi](#senkron--asenkron-simetrisi)
- [Senaryoya Göre Doğru Primitif](#senaryoya-göre-doğru-primitif)

---

## Nereden Başlamalısın?

- İlk defa kullanıyorsan [01 · Başlangıç ve Mimari Felsefe](getting-started.md) ile başla. Bağımsız senkron import yolunu, CPython GIL gerçeğini ve paylaşılan `Deadline` mantığını öğrenirsin.
- "Gürültülü komşu" izolasyonu (`KeyedConcurrencyLimiter`, `KeyedRateLimiter`) ve aynı anda gelen eş istekleri birleştirme (`SingleFlight`) için [02 · Anahtar Bazlı Sınırlama ve SingleFlight](concurrency-and-deduplication.md) rehberini oku.
- Geçici hatalar, üssel bekleme (`RetryPolicy`), devre kesici durum makinesi (`CircuitBreaker`) ve soğuma kayıtları (`CooldownRegistry`) için [03 · Dayanıklılık: Retry, Circuit Breaker ve Cooldown](resilience-and-recovery.md) sayfasına geç.
- Yetim kalan arka plan işlerini önlemek (`TaskScope`, `LatestTaskCoordinator`), ters sıra kaynak temizliği (`ResourceScope`), sınırlı iş havuzu (`BoundedWorkPool`) ve thread ↔ event loop köprüsü (`AsyncLoopPortal`) için [04 · Görev Sahipliği, Kaynak Scope'ları ve Portal](tasks-scopes-and-portal.md) rehberini incele.
- Zamanlayıcı (`Scheduler`), güncelleme birleştirme (`KeyedCoalescer`), sınırlı kardinaliteli `OperationStats` ve `flowcoord._core` C çekirdeği için [05 · Zamanlama, Coalescing, Metrikler ve C Çekirdeği](scheduling-coalescing-and-observability.md) sayfasına bak.
- Tüm 46 public dışa aktarımın kurucu ve metot özetleri için [06 · API Başvuru Kartı](api-reference.md) kullan.

[Yukarı dön](#konu-dizini)

---

## Doküman Haritası

| Doküman | İçerik |
| --- | --- |
| **[01 · Başlangıç ve Mimari Felsefe](getting-started.md)** | Kurulum, gizli event loop olmayan bağımsız senkron import, CPython GIL gerçeği ve ortak `Deadline` zaman bütçesi. |
| **[02 · Anahtar Bazlı Sınırlama ve SingleFlight](concurrency-and-deduplication.md)** | `KeyedConcurrencyLimiter`, `KeyedRateLimiter` (token bucket + `max_keys`) ve `SingleFlight` (cache stampede koruması). |
| **[03 · Dayanıklılık: Retry, Circuit Breaker ve Cooldown](resilience-and-recovery.md)** | `RetryPolicy` (jitter, `Deadline`), `CircuitBreaker` (`closed`/`open`/`half_open`), `CooldownRegistry` ve doğru katmanlama sırası. |
| **[04 · Görev Sahipliği, Kaynak Scope'ları ve Portal](tasks-scopes-and-portal.md)** | `TaskScope`, `LatestTaskCoordinator`, `ResourceScope` (LIFO + retry), `BoundedWorkPool` ve `AsyncLoopPortal`. |
| **[05 · Zamanlama, Coalescing, Metrikler ve C Çekirdeği](scheduling-coalescing-and-observability.md)** | `Scheduler` / `AsyncScheduler` (`overlap`), `KeyedCoalescer`, `OperationStats` (`max_operations`), `flowcoord._core` ve free-threaded Python. |
| **[06 · API Başvuru Kartı](api-reference.md)** | Tüm senkron/asenkron sınıflar, istatistik veri sınıfları ve istisnalar. |

[Yukarı dön](#konu-dizini)

---

## Senkron / Asenkron Simetrisi

`flowcoord`'da senkron sınıfları import etmek **`asyncio` modülünü hiç yüklemez**. `Async*` sınıfları ise `__getattr__` üzerinden ilk erişimde yüklenir (`_ASYNC_EXPORTS`). Aynı kavramlar iki kipte aynı metot adlarını kullanır (`run()`, `call()`, `wait()`):

| Thread (Senkron) | Asyncio (Asenkron) | Amacı |
| --- | --- | --- |
| `KeyedConcurrencyLimiter` | `AsyncKeyedConcurrencyLimiter` | Anahtar bazlı eşzamanlılık sınırı |
| `KeyedRateLimiter` | `AsyncKeyedRateLimiter` | Anahtar bazlı token bucket hız sınırı |
| `SingleFlight` | `AsyncSingleFlight` | Aynı anahtarlı eşzamanlı çağrıları paylaşma |
| `RetryPolicy` | `AsyncRetryPolicy` | Üssel/sabit bekleme + jitter ile yeniden deneme |
| `CircuitBreaker` | `AsyncCircuitBreaker` | Arızalı bağımlılık için hızlı devre kesici |
| `CooldownRegistry` | `AsyncCooldownRegistry` | Anahtar bazlı soğuma penceresi |
| `Scheduler` | `AsyncScheduler` | Tek seferlik ve periyodik zamanlama |
| `TaskScope` | `AsyncTaskScope` | Sınırlı arka plan görev sahipliği |
| `LatestTaskCoordinator` | `AsyncLatestTaskCoordinator` | Anahtar başına yalnız en son isteği uygulama |
| `ResourceScope` | `AsyncResourceScope` | Ters sırada ve yeniden denenebilir kaynak temizliği |

[Yukarı dön](#konu-dizini)

---

## Senaryoya Göre Doğru Primitif

| Problem | Doğru Araç | Neden? |
| --- | --- | --- |
| Bir müşteri/tenant diğerlerinin tüm thread veya bağlantılarını tüketiyor | `KeyedConcurrencyLimiter` / `KeyedRateLimiter` | Limitleri anahtar başına izole eder; `A` beklerken `B` takılmaz. |
| 100 istek aynı anda boş cache anahtarına yükleniyor | `SingleFlight` / `AsyncSingleFlight` | İşi bir kez çalıştırır ve sonucu diğer bekleyenlere paylaştırır. |
| Çöken dış servis yüzünden tüm sistem kilitleniyor | `CircuitBreaker` + `RetryPolicy` + `Deadline` | Süreyi `Deadline` ile sınırlar, sürekli hata varsa devreyi açıp hızlı reddeder. |
| Standart `ThreadPoolExecutor` bekleyen iş kuyruğunu sınırsız büyütüyor | `BoundedWorkPool` / `TaskScope` | Çalışan ve bekleyen kapasitesini ayrı sınırlar; dolunca hızlı reddeder. |
| Thread içinden async bir SDK/veritabanı istemcisi çağırmak gerekiyor | `AsyncLoopPortal` | Arka plan event loop sahipliğini yönetir ve `max_pending` ile sınırlar. |
| Aynı kullanıcı/sensör için saniyede gelen 500 güncellemeyi birleştirmek | `KeyedCoalescer` | Aralık boyunca güncellemeleri `merge` ile tek değere indirir. |

[Yukarı dön](#konu-dizini)

---

**Sonraki:** [01 · Başlangıç ve Mimari Felsefe →](getting-started.md)
