[← Belge Haritası](README.md) · [Sonraki: 02 · Anahtar Bazlı Sınırlama ve SingleFlight →](concurrency-and-deduplication.md)

# 01 · Başlangıç ve Mimari Felsefe

Bu rehber `flowcoord` kurulumunu, kütüphanenin senkron/asenkron tasarım felsefesini, CPython GIL sınırlarını ve paylaşılan `Deadline` zaman bütçesi modelini anlatır.

---

## Konu Dizini

- [Kurulum](#kurulum)
- [Mimari Tercihler ("Neden Böyle Tasarlandı?")](#mimari-tercihler-neden-böyle-tasarlandı)
  - [Bağımsız Senkron Import Yolu (`asyncio` Yüklemeden)](#bağımsız-senkron-import-yolu-asyncio-yüklemeden)
  - [CPython GIL Gerçeği: Thread Havuzları Ne Zaman Kullanılmalı?](#cpython-gil-gerçeği-thread-havuzları-ne-zaman-kullanılmalı)
  - [Kooperatif İptal (Neden Zorla Thread/Task Öldürülmez?)](#kooperatif-iptal-neden-zorla-threadtask-öldürülmez)
  - [Kapasite Sınırları ve Hızlı Reddetme (Fail-Fast)](#kapasite-sınırları-ve-hızlı-reddetme-fail-fast)
- [Paylaşılan Zaman Bütçesi: `Deadline`](#paylaşılan-zaman-bütçesi-deadline)
- [İlk Çalışan Örnek](#ilk-çalışan-örnek)

---

## Kurulum

`flowcoord` **Python 3.10 veya üzeri** gerektirir ve harici runtime bağımlılığı yoktur. Desteklenen sistemlerde isteğe bağlı `flowcoord._core` C uzantısı önceden derlenmiş olarak gelir; yoksa saf Python uygulaması devreye girer.

```bash
# PyPI üzerinden kurulum
python -m pip install flowcoord

# Depo kökünden geliştirme kurulumu
python -m pip install -e ".[dev]"
```

[Yukarı dön](#konu-dizini)

---

## Mimari Tercihler ("Neden Böyle Tasarlandı?")

### Bağımsız Senkron Import Yolu (`asyncio` Yüklemeden)

`flowcoord` kurulduğunda `from flowcoord import RetryPolicy, TaskScope, OperationStats` yaptığında Python **`asyncio` modülünü bile import etmez**. `Async*` sınıfları yalnızca talep edildiğinde `__getattr__` üzerinden yüklenir. Bu sayede CLI araçları, senkron worker süreçleri veya WSGI uygulamaları gereksiz bellek ve import maliyeti ödemez.

### CPython GIL Gerçeği: Thread Havuzları Ne Zaman Kullanılmalı?

`TaskScope`, `Scheduler`, `BoundedWorkPool` gibi senkron bileşenler arka planda Python thread'leri kullanır:

- Standart GIL açık CPython yorumlayıcısında, thread havuzları **bloklayan I/O (ağ/dosya/DB sürücüleri)** ve **GIL'i bırakan C/C++ uzantıları (NumPy vb.)** için paralel hız kazancı sağlar.
- Saf Python CPU hesaplamalarında GIL nedeniyle thread'ler paralel CPU kullanmaz (CPU ağırlıklı saf Python işleri için `ProcessPoolExecutor` veya free-threaded CPython `3.13t+` gerekir).

### Kooperatif İptal (Neden Zorla Thread/Task Öldürülmez?)

Bir Python thread'i veya iptali yutan (`except CancelledError:`) bir coroutine dışarıdan zorla kesilemez. Bazı kütüphaneler "timeout doldu" diyerek işi gizlice terk edip yetim thread biriktirir. `flowcoord` bunu bilerek **yapmaz**:

- Kuyrukta henüz başlamamış işler anında iptal edilir.
- Başlamış işler kooperatif biçimde bittiğinde slotunu serbest bırakır.
- `AsyncTaskScope` içinde iptal edilmiş ama temizliği (`finally`) henüz bitmemiş bir görev, **temizlik bitene kadar kapasite slotunu tutmaya devam eder**; böylece kaynak sızıntısı sessizce gizlenmez.

### Kapasite Sınırları ve Hızlı Reddetme (Fail-Fast)

`AsyncTaskScope(max_pending=128)`, `AsyncLoopPortal(max_pending=128)`, `BoundedWorkPool(max_workers=..., max_pending=...)` ve `AsyncScheduler(max_jobs=4096)` varsayılan veya açık üst sınırlara sahiptir. Kapasite dolduğunda yeni gönderimler sonsuz kuyruğa girip bellek patlatmak yerine hemen `RuntimeError` ile reddedilir.

[Yukarı dön](#konu-dizini)

---

## Paylaşılan Zaman Bütçesi: `Deadline`

Bir HTTP isteğinin toplam `2.0` saniye zaman bütçesi varken içinde 3 kez retry yapıp sonra bir kilit beklemek her adıma ayrı `timeout=2.0` vermekle çözülmez (toplam süre `6+` saniyeye taşar).

**`Deadline`**, mutlak monotonik zaman (`time.monotonic`) üzerinden ortak kalan süreyi izler:

- `Deadline.after(timeout)`: Şimdiden itibaren `timeout` saniyelik bütçe (`None` verilirse sınırsız bütçe).
- `deadline.remaining` ve `deadline.expired`: Kalan süre ve süre dolumu kontrolü.
- `deadline.child(sub_timeout)`: Üst bütçeyi asla aşmayan bir alt bütçe üretir (`min(now + sub_timeout, parent_expires_at)`).

```python
from flowcoord import Deadline

total_budget = Deadline.after(5.0)
db_step_budget = total_budget.child(2.0)  # En fazla 2.0 sn (ve asla 5.0'ı geçmez)
assert db_step_budget.remaining is not None and db_step_budget.remaining <= 2.0
```

[Yukarı dön](#konu-dizini)

---

## İlk Çalışan Örnek

```python
from flowcoord import CircuitBreaker, Deadline, OperationStats, RetryPolicy

stats = OperationStats(max_operations=16)
breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10.0)
retry = RetryPolicy(max_attempts=3, backoff="exponential", initial_delay=0.01)

attempts = 0


def flaky_call() -> str:
    global attempts
    attempts += 1
    if attempts < 2:
        raise TimeoutError("transient network glitch")
    return "ok"


budget = Deadline.after(1.0)
result = retry.run(
    lambda: breaker.call(flaky_call),
    should_retry=lambda exc: isinstance(exc, TimeoutError),
    deadline=budget,
)
stats.record("api.fetch", status="success")

assert result == "ok"
assert stats.snapshot().operations["api.fetch"].calls == 1
```

[Yukarı dön](#konu-dizini)

---

**Önceki:** [Belge Haritası](README.md) · **Sonraki:** [02 · Anahtar Bazlı Sınırlama ve SingleFlight →](concurrency-and-deduplication.md)
