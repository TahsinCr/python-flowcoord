[← Belge Haritası](README.md) · [Önceki: 04 · Görev Sahipliği ve Portal](tasks-scopes-and-portal.md) · [Sonraki: 06 · API Başvuru Kartı →](api-reference.md)

# 05 · Zamanlama, Coalescing, Metrikler ve C Çekirdeği

Bu rehber `Scheduler` / `AsyncScheduler` örtüşme politikalarını, yüksek frekanslı güncellemeleri anahtar bazında birleştiren `KeyedCoalescer`'ı, sınırlı kardinaliteli `OperationStats` metrik sistemini ve isteğe bağlı `flowcoord._core` C uzantısını anlatır.

---

## Konu Dizini

- [`Scheduler` ve `AsyncScheduler` (Tek Seferlik & Periyodik)](#scheduler-ve-asyncscheduler-tek-seferlik--periyodik)
  - [Örtüşme Politikaları (`overlap`: `skip`, `queue`, `parallel`)](#örtüşme-politikaları-overlap-skip-queue-parallel)
- [`KeyedCoalescer`: Tekrarlayan Güncellemeleri Birleştirme](#keyedcoalescer-tekrarlayan-güncellemeleri-birleştirme)
- [`OperationStats`: Kardinalite Patlaması Olmadan Metrik Toplama](#operationstats-kardinalite-patlaması-olmadan-metrik-toplama)
- [`flowcoord._core` C Çekirdeği ve Free-Threaded Python](#flowcoord_core-c-çekirdeği-ve-free-threaded-python)

---

## `Scheduler` ve `AsyncScheduler` (Tek Seferlik & Periyodik)

`Scheduler` işleri sınırlı işçi havuzuna yönlendiren tek bir zamanlayıcı thread çalıştırır (`max_workers=4`, `max_jobs=4096`). `AsyncScheduler` ise mevcut `asyncio` döngüsü üzerinde ek thread açmadan tek seferlik veya periyodik hem senkron hem asenkron callback'leri zamanlar (`max_jobs=4096`).

- `schedule_once(cb, *args, delay=0.0, name=None, **kwargs)` → `ScheduledHandle` / `AsyncScheduledHandle`
- `periodic(cb, *args, interval=..., overlap="skip", initial_delay=None, max_parallel=16, name=None, **kwargs)`

### Örtüşme Politikaları (`overlap`: `skip`, `queue`, `parallel`)

Periyodik bir iş her `1.0` saniyede çalışmak üzere kurulmuşken bir çalıştırma `1.5` saniye sürerse ne olmalı? `overlap` bunu belirler:

| Politika | Davranış | Ne Zaman Seçilir? |
| --- | --- | --- |
| `"skip"` *(varsayılan)* | Önceki çalışma hala sürüyorsa yeni tetiği atlar. | Health-check, periyodik senkronizasyon (birikme istenmez). |
| `"queue"` | Yeni çalıştırmayı mevcut iş bitene kadar sıraya bekletir. | Her döngünün ardışık olarak mutlaka çalışması gerektiğinde. |
| `"parallel"` | `max_parallel` (varsayılan `16`) sınırına kadar aynı işten birden fazlasını eşzamanlı koşar. | Bağımsız periyodik iş parçaları paralel yürütülebiliyorsa. |

[Yukarı dön](#konu-dizini)

---

## `KeyedCoalescer`: Tekrarlayan Güncellemeleri Birleştirme

Bir cihazın veya belgenin saniyede 200 kez durum güncellemesi ürettiği senaryolarda her değişimi tek tek veritabanına veya UI'a göndermek yerine `KeyedCoalescer` kullanılabilir:

- `interval` penceresi içinde aynı `key` için yeni değer geldiğinde `merge(mevcut, yeni)` fonksiyonu ile tek değere birleştirir (`merged` sayacı artar; kuyruk büyümez).
- Yalnızca aktif bekleyen benzersiz anahtar sayısı `max_pending_keys` (varsayılan `128`) ile sınırlıdır.
- Sahip olduğu tek işçi thread üzerinden her pencere sonunda birleşmiş değerleri `callback(merged_value)` fonksiyonuna iletir (`flush()` ve `close(flush=True)` desteklenir).

```python
from flowcoord import KeyedCoalescer

delivered: list[tuple[str, int]] = []
with KeyedCoalescer[tuple[str, int], str](
    interval=0.02,
    key=lambda item: item[0],
    merge=lambda old, new: (old[0], old[1] + new[1]),
    callback=delivered.append,
) as coalescer:
    coalescer.push(("cpu", 10))
    coalescer.push(("cpu", 15))
    coalescer.flush()

assert delivered == [("cpu", 25)]
```

[Yukarı dön](#konu-dizini)

---

## `OperationStats`: Kardinalite Patlaması Olmadan Metrik Toplama

Eğer bir operasyon adı yanlışlıkla kullanıcı ID'si veya rastgele istek anahtarı içerirse (`"fetch_user_981234"`), sınırsız sözlük tutan sayaçlar zamanla belleği şişirir (cardinality explosion).

**`OperationStats(max_operations=128)`** bunu önlemek için tasarlanmıştır:

- Farklı operasyon adı sayısı `max_operations` sınırını aştığında yeni bilinmeyen isimler otomatik olarak **`"other"`** kovasında toplanır (katlanır).
- Her operasyon için `OperationMetrics` (`calls`, `successful`, `failed`, `cancelled`, `total_seconds`, `max_seconds`, `queue_depth`, `max_queue_depth`, `dropped`, `retries`, `rejected`, `timeouts`, `shutdown_seconds`) alanlarını tutar.
- `stats.snapshot()` çağrısı salt-okunur `MappingProxyType` içeren donmuş `OperationStatsSnapshot` döndürür; sonraki `record()` veya `reset()` çağrılarından etkilenmez.

[Yukarı dön](#konu-dizini)

---

## `flowcoord._core` C Çekirdeği ve Free-Threaded Python

`flowcoord` içinde `Scheduler`, `RetryPolicy`, `CircuitBreaker`, limitörler vb. koordinasyon mantığı **tamamen saf Python** olarak kalır. Yalnızca sık çağrılan **`OperationStats`** sayaç kayıt/birleştirme motoru (`OperationStatsCore`) isteğe bağlı **`flowcoord._core`** (`flowcoord/_core.c`) C uzantısı ile hızlandırılır:

- `setup.py` varsayılan olarak `optional=True` kullanır; C derleyicisi yoksa paket saf Python olarak kurulur ve çalışır.
- Free-threaded Python (`3.13t–3.15t`) için `Py_MOD_GIL_NOT_USED` beyan eder ve yükleme sırasında `sys._is_gil_enabled()` durumunun değişmediğini doğrulayarak fail-closed güvenlik sağlar.

| Ortam Değişkeni | Aşama | Etkisi |
| --- | --- | --- |
| `FLOWCOORD_NO_EXTENSIONS=1` | Çalışma zamanı | `_core` yüklü olsa bile devre dışı bırakıp saf Python `OperationStats` kullanır. |
| `FLOWCOORD_PURE_PYTHON=1` | Paket derleme / kurulum | C uzantısını derlemeden kurulum yapar. |
| `FLOWCOORD_REQUIRE_NATIVE=1` | Paket derleme / kurulum | `optional=False` yapar; derleme başarısızsa hata verir. |

[Yukarı dön](#konu-dizini)

---

**Önceki:** [04 · Görev Sahipliği, Kaynak Scope'ları ve Portal](tasks-scopes-and-portal.md) · **Sonraki:** [06 · API Başvuru Kartı →](api-reference.md)
