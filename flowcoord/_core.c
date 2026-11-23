#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <math.h>
#include <pythread.h>
#include <stdint.h>
#include <string.h>

typedef struct {
    PyTypeObject *op_stats_core_type;
    PyObject *operation_metrics_type;
    PyObject *operation_stats_snapshot_type;
    PyObject *mapping_proxy_type;
    PyObject *zero_int;
    Py_hash_t h_other;
    PyObject *k_success;
    PyObject *k_error;
    PyObject *k_cancelled;
    PyObject *k_other;
} CoreState;

static inline CoreState *get_core_state(PyObject *module) {
    return (CoreState *)PyModule_GetState(module);
}

/* ======================== OperationStatsCore ======================== */

typedef struct {
    int64_t small;
    PyObject *large;
} OpCounter;

typedef struct {
    int64_t small;
    PyObject *large;
    PyObject *owned;
} OpCounterDelta;

typedef struct {
    int64_t small;
    PyObject *large;
    int has_update;
} OpCounterUpdate;

typedef struct {
    PyObject *name;
    Py_hash_t hash;
    int64_t calls;
    int64_t successful;
    int64_t failed;
    int64_t cancelled;
    double total_seconds;
    double max_seconds;
    OpCounter queue_depth;
    OpCounter max_queue_depth;
    OpCounter dropped;
    OpCounter retries;
    OpCounter rejected;
    OpCounter timeouts;
    double shutdown_seconds;
} OpEntry;

typedef struct {
    PyObject_HEAD
    CoreState *state;
    PyThread_type_lock lock;
    Py_ssize_t max_operations;
    Py_ssize_t count;
    Py_ssize_t capacity;
    OpEntry *entries;
    Py_ssize_t *buckets;
    Py_ssize_t mask;
} OperationStatsCoreObject;

static inline void op_stats_lock_acquire(OperationStatsCoreObject *self) {
    if (!PyThread_acquire_lock(self->lock, NOWAIT_LOCK)) {
        Py_BEGIN_ALLOW_THREADS
        PyThread_acquire_lock(self->lock, WAIT_LOCK);
        Py_END_ALLOW_THREADS
    }
}

static inline void op_stats_lock_release(OperationStatsCoreObject *self) {
    PyThread_release_lock(self->lock);
}

static int ef_parse_duration(PyObject *v, const char *name, double *out) {
    if (PyFloat_CheckExact(v)) {
        double d = PyFloat_AS_DOUBLE(v);
        if (!(d >= 0.0 && isfinite(d))) {
            PyErr_Format(PyExc_ValueError, "%s must be a finite non-negative number", name);
            return -1;
        }
        *out = d;
        return 0;
    }
    if (PyBool_Check(v) || (!PyLong_Check(v) && !PyFloat_Check(v))) {
        PyErr_Format(PyExc_TypeError, "%s must be a finite non-negative number", name);
        return -1;
    }
    double d = PyFloat_AsDouble(v);
    if (d == -1.0 && PyErr_Occurred()) {
        return -1;
    }
    if (!(d >= 0.0 && isfinite(d))) {
        PyErr_Format(PyExc_ValueError, "%s must be a finite non-negative number", name);
        return -1;
    }
    *out = d;
    return 0;
}

static int ef_parse_nonneg_counter_delta(
    PyObject *value,
    const char *err_msg,
    OpCounterDelta *out
) {
    if (PyBool_Check(value) || !PyLong_Check(value)) {
        PyErr_SetString(PyExc_ValueError, err_msg);
        return -1;
    }

    PyObject *integer = value;
    if (!PyLong_CheckExact(value)) {
        /* Normalize before the metrics lock; subclasses may run Python in __int__. */
        integer = PyNumber_Long(value);
        if (integer == NULL) return -1;
        if (!PyLong_CheckExact(integer)) {
            Py_DECREF(integer);
            PyErr_SetString(PyExc_TypeError, "integer normalization must return an exact int");
            return -1;
        }
        out->owned = integer;
    }

    int overflow = 0;
    long long parsed = PyLong_AsLongLongAndOverflow(integer, &overflow);
    if (overflow < 0 || (overflow == 0 && parsed < 0)) {
        if (PyErr_Occurred()) PyErr_Clear();
        Py_CLEAR(out->owned);
        PyErr_SetString(PyExc_ValueError, err_msg);
        return -1;
    }
    if (overflow > 0) {
        if (PyErr_Occurred()) PyErr_Clear();
        out->small = 0;
        out->large = integer;
        return 0;
    }
    if (PyErr_Occurred()) {
        Py_CLEAR(out->owned);
        return -1;
    }
    out->small = (int64_t)parsed;
    out->large = NULL;
    Py_CLEAR(out->owned);
    return 0;
}

static void op_counter_deltas_clear(OpCounterDelta *deltas, Py_ssize_t count) {
    for (Py_ssize_t i = 0; i < count; ++i) {
        Py_CLEAR(deltas[i].owned);
    }
}

static int op_counter_prepare_add(
    const OpCounter *counter,
    const OpCounterDelta *delta,
    OpCounterUpdate *update
) {
    if (delta->large == NULL && delta->small == 0) return 0;

    if (
        counter->large == NULL
        && delta->large == NULL
        && counter->small <= INT64_MAX - delta->small
    ) {
        update->small = counter->small + delta->small;
        update->has_update = 1;
        return 0;
    }

    PyObject *base = counter->large != NULL
        ? Py_NewRef(counter->large)
        : PyLong_FromLongLong(counter->small);
    PyObject *increment = delta->large != NULL
        ? Py_NewRef(delta->large)
        : PyLong_FromLongLong(delta->small);
    if (base == NULL || increment == NULL) {
        Py_XDECREF(base);
        Py_XDECREF(increment);
        return -1;
    }
    update->large = PyNumber_Add(base, increment);
    Py_DECREF(base);
    Py_DECREF(increment);
    if (update->large == NULL) return -1;
    update->has_update = 1;
    return 0;
}

static int op_counter_prepare_set(const OpCounterDelta *value, OpCounterUpdate *update) {
    update->small = value->small;
    update->large = value->large != NULL ? Py_NewRef(value->large) : NULL;
    update->has_update = 1;
    return 0;
}

static int op_counter_prepare_max(
    const OpCounter *counter,
    const OpCounterDelta *value,
    OpCounterUpdate *update
) {
    if (value->large == NULL) {
        if (counter->large != NULL || value->small <= counter->small) return 0;
    } else if (counter->large == NULL) {
        return op_counter_prepare_set(value, update);
    } else {
        int greater = PyObject_RichCompareBool(value->large, counter->large, Py_GT);
        if (greater < 0) return -1;
        if (!greater) return 0;
    }
    return op_counter_prepare_set(value, update);
}

static void op_counter_apply_add(OpCounter *counter, OpCounterUpdate *update) {
    if (!update->has_update) return;
    if (update->large != NULL) {
        Py_XSETREF(counter->large, update->large);
        update->large = NULL;
        counter->small = 0;
    } else {
        counter->small = update->small;
    }
    update->has_update = 0;
}

static void op_counter_apply_set(OpCounter *counter, OpCounterUpdate *update) {
    if (!update->has_update) return;
    if (update->large != NULL) {
        Py_XSETREF(counter->large, update->large);
        update->large = NULL;
        counter->small = 0;
    } else {
        Py_CLEAR(counter->large);
        counter->small = update->small;
    }
    update->has_update = 0;
}

static void op_counter_update_clear(OpCounterUpdate *update) {
    Py_CLEAR(update->large);
    update->has_update = 0;
}

static void op_counter_clear(OpCounter *counter) {
    counter->small = 0;
    Py_CLEAR(counter->large);
}

static void op_entry_clear_counters(OpEntry *entry) {
    op_counter_clear(&entry->queue_depth);
    op_counter_clear(&entry->max_queue_depth);
    op_counter_clear(&entry->dropped);
    op_counter_clear(&entry->retries);
    op_counter_clear(&entry->rejected);
    op_counter_clear(&entry->timeouts);
}

static PyObject *op_counter_to_python(const OpCounter *counter) {
    return counter->large != NULL
        ? Py_NewRef(counter->large)
        : PyLong_FromLongLong(counter->small);
}

static void op_entry_snapshot_incref(OpEntry *entry) {
    Py_INCREF(entry->name);
    Py_XINCREF(entry->queue_depth.large);
    Py_XINCREF(entry->max_queue_depth.large);
    Py_XINCREF(entry->dropped.large);
    Py_XINCREF(entry->retries.large);
    Py_XINCREF(entry->rejected.large);
    Py_XINCREF(entry->timeouts.large);
}

static void op_entry_snapshot_decref(OpEntry *entry) {
    Py_XDECREF(entry->name);
    Py_XDECREF(entry->queue_depth.large);
    Py_XDECREF(entry->max_queue_depth.large);
    Py_XDECREF(entry->dropped.large);
    Py_XDECREF(entry->retries.large);
    Py_XDECREF(entry->rejected.large);
    Py_XDECREF(entry->timeouts.large);
}

static void op_entries_snapshot_decref(OpEntry *entries, Py_ssize_t count) {
    for (Py_ssize_t i = 0; i < count; ++i) {
        op_entry_snapshot_decref(&entries[i]);
    }
}

static int op_stats_grow(OperationStatsCoreObject *self) {
    Py_ssize_t new_cap;
    if (self->capacity == 0) {
        new_cap = self->max_operations < 8 ? self->max_operations : 8;
    } else {
        if (self->capacity > (PY_SSIZE_T_MAX / 2)) {
            PyErr_NoMemory();
            return -1;
        }
        new_cap = self->capacity * 2;
        if (new_cap > self->max_operations) {
            new_cap = self->max_operations;
        }
    }
    if (new_cap <= self->capacity) {
        return 0;
    }
    if (new_cap > PY_SSIZE_T_MAX / 2 || (size_t)new_cap > SIZE_MAX / sizeof(OpEntry)) {
        PyErr_NoMemory();
        return -1;
    }
    Py_ssize_t bucket_cap = 16;
    while (bucket_cap < new_cap * 2) {
        if (bucket_cap > (PY_SSIZE_T_MAX / 2)) {
            PyErr_NoMemory();
            return -1;
        }
        bucket_cap *= 2;
    }
    Py_ssize_t *new_buckets = (Py_ssize_t *)PyMem_Calloc((size_t)bucket_cap, sizeof(Py_ssize_t));
    if (new_buckets == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    OpEntry *new_entries = (OpEntry *)PyMem_Realloc(self->entries, (size_t)new_cap * sizeof(OpEntry));
    if (new_entries == NULL) {
        PyMem_Free(new_buckets);
        PyErr_NoMemory();
        return -1;
    }
    self->entries = new_entries;
    self->capacity = new_cap;

    Py_ssize_t new_mask = bucket_cap - 1;
    for (Py_ssize_t i = 0; i < self->count; ++i) {
        Py_ssize_t slot = (Py_ssize_t)((size_t)self->entries[i].hash & (size_t)new_mask);
        while (new_buckets[slot] != 0) {
            slot = (slot + 1) & new_mask;
        }
        new_buckets[slot] = i + 1;
    }
    PyMem_Free(self->buckets);
    self->buckets = new_buckets;
    self->mask = new_mask;
    return 0;
}

static inline Py_ssize_t op_stats_find(
    OperationStatsCoreObject *self,
    PyObject *key,
    Py_hash_t h,
    Py_ssize_t *empty_slot_out
) {
    if (self->buckets == NULL) {
        *empty_slot_out = -1;
        return -1;
    }
    Py_ssize_t mask = self->mask;
    Py_ssize_t slot = (Py_ssize_t)((size_t)h & (size_t)mask);
    Py_ssize_t idx;
    while ((idx = self->buckets[slot]) != 0) {
        OpEntry *e = &self->entries[idx - 1];
        if (
            e->name == key
            || (
                e->hash == h
                && PyUnicode_GET_LENGTH(e->name) == PyUnicode_GET_LENGTH(key)
                && PyUnicode_Compare(e->name, key) == 0
            )
        ) {
            return idx - 1;
        }
        slot = (slot + 1) & mask;
    }
    *empty_slot_out = slot;
    return -1;
}

static int OperationStatsCore_traverse(OperationStatsCoreObject *self, visitproc visit, void *arg) {
    Py_VISIT(Py_TYPE(self));
    for (Py_ssize_t i = 0; i < self->count; ++i) {
        Py_VISIT(self->entries[i].name);
        Py_VISIT(self->entries[i].queue_depth.large);
        Py_VISIT(self->entries[i].max_queue_depth.large);
        Py_VISIT(self->entries[i].dropped.large);
        Py_VISIT(self->entries[i].retries.large);
        Py_VISIT(self->entries[i].rejected.large);
        Py_VISIT(self->entries[i].timeouts.large);
    }
    return 0;
}

static int OperationStatsCore_clear(OperationStatsCoreObject *self) {
    for (Py_ssize_t i = 0; i < self->count; ++i) {
        Py_CLEAR(self->entries[i].name);
        op_entry_clear_counters(&self->entries[i]);
    }
    self->count = 0;
    if (self->buckets != NULL) {
        memset(self->buckets, 0, (size_t)(self->mask + 1) * sizeof(Py_ssize_t));
    }
    return 0;
}

static void OperationStatsCore_dealloc(OperationStatsCoreObject *self) {
    PyTypeObject *type = Py_TYPE(self);
    PyObject_GC_UnTrack(self);
    if (self->lock != NULL) {
        PyThread_free_lock(self->lock);
        self->lock = NULL;
    }
    OperationStatsCore_clear(self);
    if (self->entries != NULL) {
        PyMem_Free(self->entries);
        self->entries = NULL;
    }
    if (self->buckets != NULL) {
        PyMem_Free(self->buckets);
        self->buckets = NULL;
    }
    type->tp_free((PyObject *)self);
    Py_DECREF(type);
}

static int OperationStatsCore_init(OperationStatsCoreObject *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {"max_operations", NULL};
    Py_ssize_t max_ops = 128;
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|n:OperationStatsCore", kwlist, &max_ops)) {
        return -1;
    }
    if (max_ops < 1) {
        PyErr_SetString(PyExc_ValueError, "max_operations must be positive");
        return -1;
    }
    if (self->lock == NULL) {
        self->state = get_core_state(PyType_GetModule(Py_TYPE(self)));
        self->lock = PyThread_allocate_lock();
        if (self->lock == NULL) {
            PyErr_NoMemory();
            return -1;
        }
        self->max_operations = max_ops;
    } else {
        op_stats_lock_acquire(self);
        OperationStatsCore_clear(self);
        self->max_operations = max_ops;
        op_stats_lock_release(self);
    }
    return 0;
}

static inline void op_entry_record_outcome(
    OpEntry *entry, int status_code, double elapsed, double shutdown_seconds
) {
    entry->calls += 1;
    if (status_code == 0) {
        entry->successful += 1;
    } else if (status_code == 1) {
        entry->failed += 1;
    } else {
        entry->cancelled += 1;
    }
    entry->total_seconds += elapsed;
    if (elapsed > entry->max_seconds) {
        entry->max_seconds = elapsed;
    }
    entry->shutdown_seconds += shutdown_seconds;
}

static PyObject *OperationStatsCore_record(
    OperationStatsCoreObject *self,
    PyObject *const *args,
    Py_ssize_t nargs
) {
    if (nargs != 9) {
        PyErr_SetString(
            PyExc_TypeError,
            "OperationStatsCore.record expects exactly 9 positional arguments"
        );
        return NULL;
    }
    CoreState *st = self->state;
    PyObject *orig_name = args[0];
    if (!PyUnicode_Check(orig_name) || PyUnicode_GET_LENGTH(orig_name) == 0) {
        PyErr_SetString(PyExc_ValueError, "operation name must be a non-empty string");
        return NULL;
    }

    PyObject *status_obj = args[1];
    int status_code;
    if (status_obj == st->k_success) {
        status_code = 0;
    } else if (status_obj == st->k_error) {
        status_code = 1;
    } else if (status_obj == st->k_cancelled) {
        status_code = 2;
    } else if (PyUnicode_Check(status_obj)) {
        int r = PyObject_RichCompareBool(status_obj, st->k_success, Py_EQ);
        if (r < 0) return NULL;
        if (r) {
            status_code = 0;
        } else {
            r = PyObject_RichCompareBool(status_obj, st->k_error, Py_EQ);
            if (r < 0) return NULL;
            if (r) {
                status_code = 1;
            } else {
                r = PyObject_RichCompareBool(status_obj, st->k_cancelled, Py_EQ);
                if (r < 0) return NULL;
                if (r) {
                    status_code = 2;
                } else {
                    PyErr_SetString(PyExc_ValueError, "status must be success, error, or cancelled");
                    return NULL;
                }
            }
        }
    } else {
        PyErr_SetString(PyExc_ValueError, "status must be success, error, or cancelled");
        return NULL;
    }

    double elapsed = 0.0;
    if (ef_parse_duration(args[2], "elapsed", &elapsed) < 0) {
        return NULL;
    }
    double shutdown_seconds = 0.0;
    if (ef_parse_duration(args[8], "shutdown_seconds", &shutdown_seconds) < 0) {
        return NULL;
    }

    const char *count_err = "drop, retry, rejection and timeout counts must be non-negative integers";
    OpCounterDelta deltas[5] = {{0}};
    if (
        (args[4] != st->zero_int && ef_parse_nonneg_counter_delta(args[4], count_err, &deltas[0]) < 0)
        || (args[5] != st->zero_int && ef_parse_nonneg_counter_delta(args[5], count_err, &deltas[1]) < 0)
        || (args[6] != st->zero_int && ef_parse_nonneg_counter_delta(args[6], count_err, &deltas[2]) < 0)
        || (args[7] != st->zero_int && ef_parse_nonneg_counter_delta(args[7], count_err, &deltas[3]) < 0)
    ) {
        op_counter_deltas_clear(deltas, 5);
        return NULL;
    }

    int has_queue_depth = 0;
    if (args[3] != Py_None) {
        has_queue_depth = 1;
        if (
            args[3] != st->zero_int
            && ef_parse_nonneg_counter_delta(
                args[3], "queue_depth must be a non-negative integer or None", &deltas[4]
            ) < 0
        ) {
            op_counter_deltas_clear(deltas, 5);
            return NULL;
        }
    }

    PyObject *key;
    Py_hash_t h;
    if (PyUnicode_GET_LENGTH(orig_name) > 128) {
        key = st->k_other;
        h = st->h_other;
    } else {
        key = orig_name;
        h = PyObject_Hash(key);
        if (h == -1) {
            op_counter_deltas_clear(deltas, 5);
            return NULL;
        }
    }

    op_stats_lock_acquire(self);

    Py_ssize_t slot = -1;
    Py_ssize_t idx = op_stats_find(self, key, h, &slot);
    if (idx < 0) {
        if (self->max_operations == 1 || self->count >= self->max_operations - 1) {
            if (key != st->k_other) {
                key = st->k_other;
                h = st->h_other;
                idx = op_stats_find(self, key, h, &slot);
            }
        }
    }

    /* Already-known operation names usually record timing and status only. */
    if (
        idx >= 0 && !has_queue_depth
        && args[4] == st->zero_int && args[5] == st->zero_int
        && args[6] == st->zero_int && args[7] == st->zero_int
    ) {
        op_entry_record_outcome(&self->entries[idx], status_code, elapsed, shutdown_seconds);
        op_stats_lock_release(self);
        Py_RETURN_NONE;
    }

    OpCounter zero_counter = {0, NULL};
    OpEntry *existing = idx >= 0 ? &self->entries[idx] : NULL;
    OpCounterUpdate updates[6] = {{0}};
    if (
        op_counter_prepare_add(existing != NULL ? &existing->dropped : &zero_counter, &deltas[0], &updates[0]) < 0
        || op_counter_prepare_add(existing != NULL ? &existing->retries : &zero_counter, &deltas[1], &updates[1]) < 0
        || op_counter_prepare_add(existing != NULL ? &existing->rejected : &zero_counter, &deltas[2], &updates[2]) < 0
        || op_counter_prepare_add(existing != NULL ? &existing->timeouts : &zero_counter, &deltas[3], &updates[3]) < 0
        || (
            has_queue_depth
            && (
                op_counter_prepare_set(&deltas[4], &updates[4]) < 0
                || op_counter_prepare_max(
                    existing != NULL ? &existing->max_queue_depth : &zero_counter,
                    &deltas[4],
                    &updates[5]
                ) < 0
            )
        )
    ) {
        for (int i = 0; i < 6; ++i) op_counter_update_clear(&updates[i]);
        op_stats_lock_release(self);
        op_counter_deltas_clear(deltas, 5);
        return NULL;
    }

    if (idx < 0) {
        if (self->count >= self->capacity) {
            if (op_stats_grow(self) < 0) {
                for (int i = 0; i < 6; ++i) op_counter_update_clear(&updates[i]);
                op_stats_lock_release(self);
                op_counter_deltas_clear(deltas, 5);
                return NULL;
            }
            slot = (Py_ssize_t)((size_t)h & (size_t)self->mask);
            while (self->buckets[slot] != 0) {
                slot = (slot + 1) & self->mask;
            }
        }
        idx = self->count++;
        OpEntry *e = &self->entries[idx];
        memset(e, 0, sizeof(OpEntry));
        e->name = Py_NewRef(key);
        e->hash = h;
        self->buckets[slot] = idx + 1;
    }

    OpEntry *e = &self->entries[idx];
    op_entry_record_outcome(e, status_code, elapsed, shutdown_seconds);
    op_counter_apply_add(&e->dropped, &updates[0]);
    op_counter_apply_add(&e->retries, &updates[1]);
    op_counter_apply_add(&e->rejected, &updates[2]);
    op_counter_apply_add(&e->timeouts, &updates[3]);
    if (has_queue_depth) {
        op_counter_apply_set(&e->queue_depth, &updates[4]);
        op_counter_apply_set(&e->max_queue_depth, &updates[5]);
    }
    for (int i = 0; i < 6; ++i) op_counter_update_clear(&updates[i]);

    op_stats_lock_release(self);
    op_counter_deltas_clear(deltas, 5);
    Py_RETURN_NONE;
}

static PyObject *OperationStatsCore_snapshot(
    OperationStatsCoreObject *self,
    PyObject *Py_UNUSED(ignored)
) {
    CoreState *st = self->state;
    if (
        st->operation_metrics_type == NULL
        || st->operation_stats_snapshot_type == NULL
        || st->mapping_proxy_type == NULL
    ) {
        PyErr_SetString(PyExc_RuntimeError, "flowcoord._core observability is not configured");
        return NULL;
    }

    OpEntry stack_buf[16];
    OpEntry *snap = stack_buf;
    Py_ssize_t snap_cap = 16;

    op_stats_lock_acquire(self);
    Py_ssize_t n = self->count;
    if (n > snap_cap) {
        OpEntry *heap_buf = (OpEntry *)PyMem_Malloc((size_t)n * sizeof(OpEntry));
        if (heap_buf == NULL) {
            op_stats_lock_release(self);
            return PyErr_NoMemory();
        }
        snap = heap_buf;
    }
    if (n > 0) {
        memcpy(snap, self->entries, (size_t)n * sizeof(OpEntry));
        for (Py_ssize_t i = 0; i < n; ++i) {
            op_entry_snapshot_incref(&snap[i]);
        }
    }
    op_stats_lock_release(self);

    PyObject *dict = PyDict_New();
    if (dict == NULL) {
        op_entries_snapshot_decref(snap, n);
        if (snap != stack_buf) PyMem_Free(snap);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject *args_tuple = PyTuple_New(13);
        if (args_tuple == NULL) {
            op_entries_snapshot_decref(snap + i, n - i);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
        PyObject *vals[13];
        vals[0] = PyLong_FromLongLong(snap[i].calls);
        vals[1] = PyLong_FromLongLong(snap[i].successful);
        vals[2] = PyLong_FromLongLong(snap[i].failed);
        vals[3] = PyLong_FromLongLong(snap[i].cancelled);
        vals[4] = PyFloat_FromDouble(snap[i].total_seconds);
        vals[5] = PyFloat_FromDouble(snap[i].max_seconds);
        vals[6] = op_counter_to_python(&snap[i].queue_depth);
        vals[7] = op_counter_to_python(&snap[i].max_queue_depth);
        vals[8] = op_counter_to_python(&snap[i].dropped);
        vals[9] = op_counter_to_python(&snap[i].retries);
        vals[10] = op_counter_to_python(&snap[i].rejected);
        vals[11] = op_counter_to_python(&snap[i].timeouts);
        vals[12] = PyFloat_FromDouble(snap[i].shutdown_seconds);
        int ok = 1;
        for (int k = 0; k < 13; ++k) {
            if (vals[k] == NULL) {
                ok = 0;
            } else {
                PyTuple_SET_ITEM(args_tuple, k, vals[k]);
            }
        }
        if (!ok) {
            Py_DECREF(args_tuple);
            op_entries_snapshot_decref(snap + i, n - i);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
        PyObject *metrics = PyObject_CallObject(st->operation_metrics_type, args_tuple);
        Py_DECREF(args_tuple);
        if (metrics == NULL) {
            op_entries_snapshot_decref(snap + i, n - i);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
        int set_rc = PyDict_SetItem(dict, snap[i].name, metrics);
        Py_DECREF(metrics);
        op_entry_snapshot_decref(&snap[i]);
        if (set_rc < 0) {
            op_entries_snapshot_decref(snap + i + 1, n - i - 1);
            if (snap != stack_buf) PyMem_Free(snap);
            Py_DECREF(dict);
            return NULL;
        }
    }
    if (snap != stack_buf) {
        PyMem_Free(snap);
    }

    PyObject *proxy = PyObject_CallOneArg(st->mapping_proxy_type, dict);
    Py_DECREF(dict);
    if (proxy == NULL) {
        return NULL;
    }
    PyObject *result = PyObject_CallOneArg(st->operation_stats_snapshot_type, proxy);
    Py_DECREF(proxy);
    return result;
}

static PyObject *OperationStatsCore_reset(
    OperationStatsCoreObject *self,
    PyObject *Py_UNUSED(ignored)
) {
    PyObject *stack_names[16];
    PyObject **old_names = stack_names;
    Py_ssize_t n = 0;
    Py_ssize_t heap_allocated = 0;

    op_stats_lock_acquire(self);
    n = self->count;
    if (n > 0) {
        if (n <= 16) {
            for (Py_ssize_t i = 0; i < n; ++i) {
                old_names[i] = self->entries[i].name;
                self->entries[i].name = NULL;
                op_entry_clear_counters(&self->entries[i]);
            }
        } else {
            PyObject **heap_names = (PyObject **)PyMem_Malloc((size_t)n * sizeof(PyObject *));
            if (heap_names != NULL) {
                old_names = heap_names;
                heap_allocated = 1;
                for (Py_ssize_t i = 0; i < n; ++i) {
                    old_names[i] = self->entries[i].name;
                    self->entries[i].name = NULL;
                    op_entry_clear_counters(&self->entries[i]);
                }
            } else {
                for (Py_ssize_t i = 0; i < n; ++i) {
                    Py_CLEAR(self->entries[i].name);
                    op_entry_clear_counters(&self->entries[i]);
                }
                n = 0;
            }
        }
        self->count = 0;
        if (self->buckets != NULL) {
            memset(self->buckets, 0, (size_t)(self->mask + 1) * sizeof(Py_ssize_t));
        }
    }
    op_stats_lock_release(self);

    for (Py_ssize_t i = 0; i < n; ++i) {
        Py_DECREF(old_names[i]);
    }
    if (heap_allocated) {
        PyMem_Free(old_names);
    }
    Py_RETURN_NONE;
}

static PyMethodDef OperationStatsCore_methods[] = {
    {"record", (PyCFunction)(void (*)(void))OperationStatsCore_record, METH_FASTCALL, NULL},
    {"snapshot", (PyCFunction)OperationStatsCore_snapshot, METH_NOARGS, NULL},
    {"reset", (PyCFunction)OperationStatsCore_reset, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL},
};

static PyType_Slot OperationStatsCore_slots[] = {
    {Py_tp_doc, "Native storage and validation core for OperationStats."},
    {Py_tp_init, (void *)OperationStatsCore_init},
    {Py_tp_dealloc, (void *)OperationStatsCore_dealloc},
    {Py_tp_traverse, (void *)OperationStatsCore_traverse},
    {Py_tp_clear, (void *)OperationStatsCore_clear},
    {Py_tp_methods, OperationStatsCore_methods},
    {0, NULL},
};

static PyType_Spec OperationStatsCore_spec = {
    .name = "flowcoord._core.OperationStatsCore",
    .basicsize = sizeof(OperationStatsCoreObject),
    .flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .slots = OperationStatsCore_slots,
};

static PyObject *configure_observability(PyObject *module, PyObject *args) {
    CoreState *st = get_core_state(module);
    PyObject *metrics_tp, *snapshot_tp, *proxy_tp;
    if (!PyArg_ParseTuple(
            args, "OOO:configure_observability", &metrics_tp, &snapshot_tp, &proxy_tp
        )) {
        return NULL;
    }
    Py_XSETREF(st->operation_metrics_type, Py_NewRef(metrics_tp));
    Py_XSETREF(st->operation_stats_snapshot_type, Py_NewRef(snapshot_tp));
    Py_XSETREF(st->mapping_proxy_type, Py_NewRef(proxy_tp));
    Py_RETURN_NONE;
}

static int core_traverse(PyObject *m, visitproc visit, void *arg) {
    CoreState *st = get_core_state(m);
    Py_VISIT(st->op_stats_core_type);
    Py_VISIT(st->operation_metrics_type);
    Py_VISIT(st->operation_stats_snapshot_type);
    Py_VISIT(st->mapping_proxy_type);
    Py_VISIT(st->zero_int);
    Py_VISIT(st->k_success);
    Py_VISIT(st->k_error);
    Py_VISIT(st->k_cancelled);
    Py_VISIT(st->k_other);
    return 0;
}

static int core_clear(PyObject *m) {
    CoreState *st = get_core_state(m);
    Py_CLEAR(st->op_stats_core_type);
    Py_CLEAR(st->operation_metrics_type);
    Py_CLEAR(st->operation_stats_snapshot_type);
    Py_CLEAR(st->mapping_proxy_type);
    Py_CLEAR(st->zero_int);
    Py_CLEAR(st->k_success);
    Py_CLEAR(st->k_error);
    Py_CLEAR(st->k_cancelled);
    Py_CLEAR(st->k_other);
    return 0;
}

static void core_free(void *m) {
    core_clear((PyObject *)m);
}

static int core_exec(PyObject *m) {
    CoreState *st = get_core_state(m);
    st->k_success = PyUnicode_InternFromString("success");
    st->k_error = PyUnicode_InternFromString("error");
    st->k_cancelled = PyUnicode_InternFromString("cancelled");
    st->k_other = PyUnicode_InternFromString("other");
    st->zero_int = PyLong_FromLong(0);
    if (st->k_success == NULL || st->k_error == NULL || st->k_cancelled == NULL
        || st->k_other == NULL || st->zero_int == NULL) {
        return -1;
    }
    st->h_other = PyObject_Hash(st->k_other);
    if (st->h_other == -1) return -1;
    st->op_stats_core_type = (PyTypeObject *)PyType_FromModuleAndSpec(
        m, &OperationStatsCore_spec, NULL
    );
    if (st->op_stats_core_type == NULL) return -1;
    return PyModule_AddObjectRef(m, "OperationStatsCore", (PyObject *)st->op_stats_core_type);
}

static PyMethodDef core_methods[] = {
    {"configure_observability", configure_observability, METH_VARARGS, "Bind operation metric classes."},
    {NULL, NULL, 0, NULL},
};

static PyModuleDef_Slot core_slots[] = {
    {Py_mod_exec, core_exec},
#if PY_VERSION_HEX >= 0x030D0000
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL},
};

static struct PyModuleDef core_module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_core",
    .m_doc = "Optional C metrics storage for FlowCoord.",
    .m_size = sizeof(CoreState),
    .m_methods = core_methods,
    .m_slots = core_slots,
    .m_traverse = core_traverse,
    .m_clear = core_clear,
    .m_free = core_free,
};

PyMODINIT_FUNC PyInit__core(void) {
    return PyModuleDef_Init(&core_module);
}
