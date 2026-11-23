[![CI durumu][ci-shield]][ci]
[![PyPI sürümü][pypi-shield]][pypi]
[![Python 3.10+][python-shield]][python]
[![MIT Lisansı][license-shield]][license]
<br>
[English][readme-en] | **Türkçe**

# FlowCoord

[Repository][repository] · [Changelog][changelog] · [MIT License][license]

Eşzamanlı işleri, geçici hataları ve görevlerle kaynakların yaşam döngüsünü yönetmek için Python bileşenleri.

## İçindekiler

- [Başlangıç](#getting-started)
- [Temel kavramlar](#core-concepts)
- [Eşzamanlılık ve görev sahipliği](#concurrency)
- [Operasyon ölçümleri](#operation-metrics)
- [Performans ve C uzantısı](#performance)
- [Geliştirme ve lisans](#development)

<a id="getting-started"></a>

## Başlangıç

### Kurulum

Proje kökünde Python 3.10 veya üzeriyle kurun.

~~~sh
python -m pip install .
~~~

### Geçici bir hatadan sonra yeniden deneyin

Yeniden deneme politikası deneme sayısını sınırlar. Bu örnekte zaman aşımı yeniden denenir, diğer hatalar çağırana iletilir.

~~~python
from flowcoord import RetryPolicy

outcomes = iter([False, True])


def fetch() -> int:
    if not next(outcomes):
        raise TimeoutError("temporary")
    return 42


retry = RetryPolicy(max_attempts=2)
result = retry.run(fetch, should_retry=lambda error: isinstance(error, TimeoutError))
assert result == 42
~~~

<a id="core-concepts"></a>

## Temel kavramlar

Eşzamanlılığı veya hızı sınırlamak için anahtar bazlı limitleri, aynı anda gelen isteklerin tek bir işi paylaşması için `SingleFlight`, temizlik için görev ve kaynak scope’larını kullanın. Kütüphane zamanlama, yeniden deneme, cooldown ve circuit breaker bileşenleri de sunar.

Senkron sınıflar thread’ler arasında kullanılmak için tasarlandı. Bunları içe aktarmak `asyncio` yüklemez. `Async*` sınıfları asyncio için tasarlandı ve ilk kullanımda yüklenir. Retry, circuit breaker ve cooldown için iki kipte de sırasıyla `run()`, `call()` ve `wait()` adları kullanılır. Async çağrıları `await` ile çalıştırın.

| Thread | Asyncio |
| --- | --- |
| `KeyedConcurrencyLimiter`, `KeyedRateLimiter` | `AsyncKeyedConcurrencyLimiter`, `AsyncKeyedRateLimiter` |
| `SingleFlight` | `AsyncSingleFlight` |
| `RetryPolicy`, `CircuitBreaker`, `CooldownRegistry` | `AsyncRetryPolicy`, `AsyncCircuitBreaker`, `AsyncCooldownRegistry` |
| `Scheduler` | `AsyncScheduler` |
| `TaskScope`, `LatestTaskCoordinator` | `AsyncTaskScope`, `AsyncLatestTaskCoordinator` |
| `ResourceScope` | `AsyncResourceScope` |

`AsyncLoopPortal`, bir thread üzerinden asyncio işlerine erişim sağlar. Sadece thread tabanlı arka plan çalışması için `TaskScope` veya `BoundedWorkPool` kullanın.

`AsyncTaskScope(max_pending=128)` ve `AsyncLoopPortal(max_pending=128)` kabul edilen iş sayısını sınırlar. İptal edilen bir iş, temizliği tamamlanana kadar kapasitedeki yerini korur. `AsyncScheduler(max_jobs=4096)` tek seferlik ve periyodik iş sayısını sınırlar. Paralel çalışan her periyodik işin ayrıca kendi `max_parallel` sınırı vardır. Kapasite doluyken yeni iş göndermek hemen `RuntimeError` üretir.

Thread havuzları beklemeli I/O ve GIL’i serbest bırakan native işlemler için uygundur. GIL açıkken CPython, işlemci ağırlıklı saf Python kodunu birden fazla thread’de paralel çalıştırmaz.

<a id="concurrency"></a>

## Eşzamanlılık ve görev sahipliği

### Thread havuzunda anahtar bazlı izin kullanın

Bekleyen işler iptal edilebilir. Çalışmaya başlamış Python callback’leri kendi işi tamamlandığında durur. `close(timeout=...)` zaman aşımına uğrarsa scope kalan işleri izlemeye devam eder.

~~~python
from flowcoord import KeyedConcurrencyLimiter, TaskScope

limiter = KeyedConcurrencyLimiter[str](1)


def process(account: str) -> str:
    with limiter.acquire(account, timeout=1):
        return account


with TaskScope(max_workers=4, max_pending=16) as scope:
    first = scope.start(lambda: process("account-42"), shutdown="drain")
    second = scope.start(lambda: process("account-55"), shutdown="drain")
    assert (first.join(), second.join()) == ("account-42", "account-55")
~~~

### İşleri anahtara göre sınırlayın

Anahtar bazlı limit, farklı anahtarlara ait işlerin birbirinden bağımsız ilerlemesini sağlar. Görev scope’u çalışan görevleri yönetir ve bağlam kapanırken tamamlanmalarını bekler.

~~~python
import asyncio

from flowcoord import AsyncKeyedConcurrencyLimiter, AsyncTaskScope


async def main() -> None:
    limiter = AsyncKeyedConcurrencyLimiter[str](1)

    async def process(account: str) -> str:
        async with limiter.acquire(account):
            await asyncio.sleep(0)
            return account

    try:
        async with AsyncTaskScope() as scope:
            task = scope.start(lambda: process("account-42"), shutdown="drain")
            assert await task.join() == "account-42"
    finally:
        await limiter.aclose()


asyncio.run(main())
~~~

<a id="operation-metrics"></a>

## Operasyon ölçümleri

### İş havuzunun ölçümlerini alın

Operasyon sayaçlarına ihtiyaç duyduğunuz yerde `OperationStats` kullanın. Sınırlı iş havuzu, çalışan ve bekleyen işler için ayrı kapasite tanımlamanıza olanak verir.

~~~python
from flowcoord import BoundedWorkPool, OperationStats

metrics = OperationStats(max_operations=16)
with BoundedWorkPool(max_workers=2, max_pending=4, observer=metrics) as pool:
    result = pool.submit(abs, -42).result()
    assert result == 42

assert metrics.snapshot().operations
~~~

<a id="performance"></a>

## Performans ve C uzantısı

İsteğe bağlı C uzantısı, `OperationStats` sık sayaç kaydedip topladığında ölçüm yükünü azaltır. Görevler, zamanlama ve yeniden denemeler Python’da çalışır. İki çalışma biçimi de aynı thread-safe ölçüm arka ucunu paylaşır. Saf Python ölçüm altyapısı aynı API’yi destekler. Kendi makinenizde ölçmek için `python -m benchmarks.benchmark_observability` komutunu çalıştırın.

<a id="development"></a>

## Geliştirme ve lisans

~~~sh
python -m pip install -e ".[dev]"
python run_tests.py
~~~

C uzantısını yeniden derlemek ve testlerde zorunlu tutmak için `python run_tests.py --native` çalıştırın.

Proje [MIT Lisansı][license] ile yayımlanır.

## Diğer projeler

[FlowEvent][repo-flowevent] · [FlowPipeline][repo-flowpipeline]

[ci-shield]: https://img.shields.io/github/actions/workflow/status/TahsinCr/python-flowcoord/ci.yml?branch=main&event=push&label=CI&style=for-the-badge&logo=github&logoColor=white
[pypi-shield]: https://img.shields.io/pypi/v/flowcoord?label=PyPI&style=for-the-badge&logo=pypi&logoColor=white
[python-shield]: https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?style=for-the-badge&logo=python&logoColor=white
[license-shield]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
[ci]: https://github.com/TahsinCr/python-flowcoord/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush
[pypi]: https://pypi.org/project/flowcoord/
[python]: https://www.python.org/downloads/
[readme-en]: https://github.com/TahsinCr/python-flowcoord/blob/main/README.md
[repository]: https://github.com/TahsinCr/python-flowcoord
[changelog]: https://github.com/TahsinCr/python-flowcoord/blob/main/CHANGELOG.md
[license]: https://github.com/TahsinCr/python-flowcoord/blob/main/LICENSE
[repo-flowevent]: https://github.com/TahsinCr/python-flowevent
[repo-flowpipeline]: https://github.com/TahsinCr/python-flowpipeline
