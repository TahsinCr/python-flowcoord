[← Belge Haritası](README.md) · [Önceki: 03 · Dayanıklılık](resilience-and-recovery.md) · [Sonraki: 05 · Zamanlama, Coalescing ve Metrikler →](scheduling-coalescing-and-observability.md)

# 04 · Görev Sahipliği, Kaynak Scope'ları ve Portal

Bu rehber başıboş arka plan görevlerini önleyen `TaskScope` ve `LatestTaskCoordinator`, ters sıra kaynak temizliği sağlayan `ResourceScope`, sınırlı iş havuzu `BoundedWorkPool` ve thread ↔ event loop köprüsü `AsyncLoopPortal` yapılarını anlatır.

---

## Konu Dizini

- [Problem: Fire-and-Forget Task'lar ve Yetim İşler](#problem-fire-and-forget-tasklar-ve-yetim-i%CC%87şler)
- [`TaskScope` ve `AsyncTaskScope`](#taskscope-ve-asynctaskscope)
- [`LatestTaskCoordinator` ve `AsyncLatestTaskCoordinator`](#latesttaskcoordinator-ve-asynclatesttaskcoordinator)
- [`ResourceScope` ve `AsyncResourceScope`](#resourcescope-ve-asyncresourcescope)
- [`BoundedWorkPool` — Neden Standart `ThreadPoolExecutor` Yetmez?](#boundedworkpool--neden-standart-threadpoolexecutor-yetmez)
- [`AsyncLoopPortal` — Thread'den Asyncio Loop'a Güvenli Köprü](#asyncloopportal--threadden-asyncio-loopa-güvenli-köprü)

---

## Problem: Fire-and-Forget Task'lar ve Yetim İşler

Python'da `asyncio.create_task(job())` veya `threading.Thread(target=job).start()` yazıp referansı unutmak üç büyük soruna yol açar:

1. **Sessiz çökme ve GC iptali:** Task referansı tutulmadığında Python çöp toplayıcısı görevi beklenmedik anda yok edebilir; hatalar da kimse `await` etmediği için sessizce loglara düşer.
2. **Kapanışta yarım kalmış temizlik:** Uygulama veya istek kapandığında arka planda çalışan işlerin tamamlanması beklenmez.
3. **Sınırsız kuyruk bellek taşması (OOM):** Standart `concurrent.futures.ThreadPoolExecutor` iş kuyruğunu sınırlamaz; işçiler yetişemezse milyonlarca `Future` ve argüman RAM'i doldurur.

[Yukarı dön](#konu-dizini)

---

## `TaskScope` ve `AsyncTaskScope`

Başlatılan tüm arka plan işlerini tek bir sahip altında toplar:

- **`max_pending`** (varsayılan `128`): Bekleyen/çalışan görev sınırını belirler; doluysa yeni `start()` çağrısı hemen `RuntimeError` fırlatır.
- **`shutdown="cancel"` vs `"drain"`:** `scope.start(op, shutdown=...)` çağrısı scope kapanırken görevin iptal mi edileceğini (`cancel`) yoksa doğal bitişinin mi bekleneceğini (`drain`) görev bazında belirler.
- **İptal slot koruması (`AsyncTaskScope`):** İptal edilen bir `asyncio` görevi, içindeki `finally:` temizliği bitene kadar `max_pending` slotunu tutar; böylece yavaş temizliklerin birikip kapasiteyi sessizce aşması önlenir.
- `TaskHandle` / `AsyncTaskHandle` üzerinden `.join(timeout=...)`, `.cancel()`, `.done` kullanılabilir; başarısız işler için scope kurucusuna `on_error(handle, exc)` verilebilir.

Senkron `join()`, beklerken eklenen görevler de dahil olmak üzere kapsamın boşalmasını bekler. Asenkron `join()` yalnızca çağrı başında kapsamın sahip olduğu görevleri bekler. Yeni görev kabulünü durdurup kapanış politikalarını uygulamak için `close()` veya `aclose()` kullan.

[Yukarı dön](#konu-dizini)

---

## `LatestTaskCoordinator` ve `AsyncLatestTaskCoordinator`

Kullanıcı arama kutusuna hızla `"a"`, `"ab"`, `"abc"` yazarken veya aynı cihaz kimliği için art arda yenileme istekleri gelirken, **eski isteğin cevabı ağdan daha geç gelip yeni cevabın üstüne yazabilir** ("out-of-order stale response").

`LatestTaskCoordinator` her anahtar için tek bir nesil (generation) izler:

- `submit(key, factory, on_result, *, cancel_previous=True, on_error=None)`: Aynı `key` için yeni iş geldiğinde önceki işi iptal etmeyi deneyebilir (`cancel_previous=True`) ve **yalnızca en son gönderilen nesil hala güncelse `on_result(result)` callback'ini uygular**. Eski işin geç gelen sonucu çöpe atılır (`snapshot().discarded_results` sayacı artar).

[Yukarı dön](#konu-dizini)

---

## `ResourceScope` ve `AsyncResourceScope`

Bir iş akışında birden fazla dosya, veritabanı bağlantısı, geçici kilit veya abonelik açtığında `ExitStack` benzeri ama yeniden denenebilir ve iptal-dayanıklı temizlik sağlar:

- `scope.add(resource, close="close", owned=True)`: Kaynağı kaydeder (async sürümde varsayılan `close="aclose"`; senkron veya asenkron kapatma otomatik desteklenir). `owned=False` ile ödünç kaynak eklenirse çıkışta kapatılmaz.
- `scope.defer(fn)`: Çıkışta çalışacak özel temizlik fonksiyonu ekler.
- `scope.detach(resource)`: Kaynağın sahipliğini çıkarır (ör. dışarı devredilecek nesne).
- **Ters sıra (LIFO) + yeniden deneme:** Tüm kayıtlı kaynaklar kapatılmaya çalışılır; birinde hata çıksa bile diğerleri atlanmaz, başarısız olanlar scope içinde kalır (`pending_cleanup() > 0`) ve hatalar `ExceptionGroup` ile bildirilir. `AsyncResourceScope.aclose()` dışarıdan iptal gelse dahi temizliği yarıda bırakmaz.

[Yukarı dön](#konu-dizini)

---

## `BoundedWorkPool` — Neden Standart `ThreadPoolExecutor` Yetmez?

Standart `ThreadPoolExecutor` sınırsız dahili kuyruğa sahiptir ve kabul kapasitesini ölçmez. **`BoundedWorkPool(*, max_workers, max_pending, observer=None, thread_name_prefix="FlowCoordWorker")`** ise:

- En fazla `max_workers` çalışan + `max_pending` bekleyen iş kabul eder; sınır aşılırsa beklemeden hata fırlatır (`rejected` sayacı ve `OperationStats` kaydı güncellenir).
- `submit(fn, *args, **kwargs)` aktif `ContextVar` bağlamını işçi thread'e otomatik kopyalar.
- `with BoundedWorkPool(...) as pool:` kapanışta güvenli kapatma (`shutdown`/`close`) yapar ve `pool.stats()` ile `WorkPoolStats` verir.

[Yukarı dön](#konu-dizini)

---

## `AsyncLoopPortal` — Thread'den Asyncio Loop'a Güvenli Köprü

Senkron bir uygulamadan (CLI, Flask/Django, GUI veya worker thread) bir `async` kütüphane/SDK'yı çağırmak gerektiğinde her seferinde `asyncio.run(...)` açmak pahalıdır ve bağlantı havuzlarını kırar (çünkü her `asyncio.run` yeni loop açıp kapatır).

**`AsyncLoopPortal`**, kendi özel arka plan thread'inde tek bir uzun ömürlü `asyncio` event loop çalıştırır ve tüm thread'lerden gelen asenkron işleri güvenle bu loop'a yönlendirir:

```python
import asyncio
from flowcoord import AsyncLoopPortal


async def async_add(a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a + b


with AsyncLoopPortal(max_pending=32) as portal:
    value = portal.call(async_add, 19, 23, timeout=1.0)
    future = portal.submit(async_add, 1, 2)
    assert value == 42
    assert future.result() == 3
```

- `max_pending` (varsayılan `128`) kabul kapasitesini sınırlar.
- `portal.call(..., timeout=...)` zaman aşımı veya iptal olursa karşı taraftaki `asyncio.Task`'ı da iptal sinyaliyle temizler; `coroutine` oluşturulup gönderilemezse sızdırmadan kapatır (`close_awaitable`).
- Kapanmış portal'a gönderim `AsyncLoopPortalClosedError` yükseltir.

[Yukarı dön](#konu-dizini)

---

**Önceki:** [03 · Dayanıklılık: Retry, Circuit Breaker ve Cooldown](resilience-and-recovery.md) · **Sonraki:** [05 · Zamanlama, Coalescing, Metrikler ve C Çekirdeği →](scheduling-coalescing-and-observability.md)
