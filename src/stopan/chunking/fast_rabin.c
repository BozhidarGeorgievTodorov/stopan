#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>

/*
 * CDC basado en fingerprint "gear-like".
 *
 * Semántica:
 *   - pre_avg_mask: máscara usada antes de alcanzar avg_size (más estricta)
 *   - post_avg_mask: máscara usada después de avg_size (más relajada)
 */

#if defined(__GNUC__) || defined(__clang__)
    #define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
    #define UNLIKELY(x) (x)
#endif


/* ============================================================================
 * Tabla de entropía
 * ========================================================================== */

static uint64_t gear_table[256];

static void init_tables(void) {
    uint64_t value = 0x123456789abcdef0ULL;

    for (int i = 0; i < 256; i++) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        gear_table[i] = value;
    }
}


/* ============================================================================
 * Iterador de fronteras
 * ========================================================================== */

typedef struct {
    PyObject_HEAD
    Py_buffer view;
    uint64_t pre_avg_mask;
    uint64_t post_avg_mask;
    Py_ssize_t min_size;
    Py_ssize_t avg_size;
    Py_ssize_t max_size;
    Py_ssize_t current_pos;
    Py_ssize_t last_split;
    uint64_t fingerprint;
    int buffer_active;
} ChunkIterator;


static void ChunkIterator_dealloc(ChunkIterator *self) {
    if (self->buffer_active) {
        PyBuffer_Release(&self->view);
        self->buffer_active = 0;
    }

    Py_TYPE(self)->tp_free((PyObject *)self);
}


static PyObject *ChunkIterator_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}


static PyObject *ChunkIterator_iternext(PyObject *self_obj) {
    ChunkIterator *self = (ChunkIterator *)self_obj;
    const uint8_t *data = (const uint8_t *)self->view.buf;
    Py_ssize_t data_len = self->view.len;

    if (self->last_split >= data_len) {
        return NULL;  /* StopIteration */
    }

    if (data_len - self->last_split <= self->min_size) {
        Py_ssize_t end = data_len;
        self->last_split = end;
        self->current_pos = end;
        return PyLong_FromSsize_t(end);
    }

    self->fingerprint = 0;
    self->current_pos = self->last_split;

    Py_ssize_t min_split = self->last_split + self->min_size;
    Py_ssize_t max_split = self->last_split + self->max_size;
    if (max_split > data_len) {
        max_split = data_len;
    }

    /* ------------------------------------------------------------------------
     * Fase 1: tramo ciego hasta min_size
     * --------------------------------------------------------------------- */

    Py_ssize_t blind_limit = min_split - 3;

    for (; self->current_pos < blind_limit; self->current_pos += 4) {
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos]];
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos + 1]];
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos + 2]];
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos + 3]];
    }

    for (; self->current_pos < min_split; self->current_pos++) {
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos]];
    }

    /* ------------------------------------------------------------------------
     * Fase 2A: antes de avg_size, máscara estricta
     * --------------------------------------------------------------------- */

    Py_ssize_t avg_split = self->last_split + self->avg_size;
    if (avg_split > max_split) {
        avg_split = max_split;
    }

    for (; self->current_pos < avg_split; self->current_pos++) {
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos]];

        if (UNLIKELY((self->fingerprint & self->pre_avg_mask) == 0)) {
            Py_ssize_t split_point = self->current_pos + 1;
            self->last_split = split_point;
            self->current_pos = split_point;
            return PyLong_FromSsize_t(split_point);
        }
    }

    /* ------------------------------------------------------------------------
     * Fase 2B: después de avg_size, máscara relajada
     * --------------------------------------------------------------------- */

    for (; self->current_pos < max_split; self->current_pos++) {
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos]];

        if (UNLIKELY((self->fingerprint & self->post_avg_mask) == 0)) {
            Py_ssize_t split_point = self->current_pos + 1;
            self->last_split = split_point;
            self->current_pos = split_point;
            return PyLong_FromSsize_t(split_point);
        }
    }

    /* ------------------------------------------------------------------------
     * Fase 3: corte forzado en max_size
     * --------------------------------------------------------------------- */

    self->last_split = max_split;
    self->current_pos = max_split;
    return PyLong_FromSsize_t(max_split);
}


static PyTypeObject ChunkIteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "core.fast_rabin.ChunkIterator",
    .tp_basicsize = sizeof(ChunkIterator),
    .tp_dealloc = (destructor)ChunkIterator_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Lazy iterator over CDC chunk boundaries",
    .tp_iter = ChunkIterator_iter,
    .tp_iternext = ChunkIterator_iternext,
};


/* ============================================================================
 * Factory
 * ========================================================================== */

static PyObject *get_chunk_boundaries(PyObject *self, PyObject *args) {
    PyObject *buffer_obj = NULL;
    unsigned long long pre_avg_mask;
    unsigned long long post_avg_mask;
    Py_ssize_t min_size, avg_size, max_size;

    if (!PyArg_ParseTuple(
            args,
            "OKKnnn",
            &buffer_obj,
            &pre_avg_mask,
            &post_avg_mask,
            &min_size,
            &avg_size,
            &max_size)) {
        return NULL;
    }

    if (min_size <= 0) {
        PyErr_SetString(PyExc_ValueError, "min_size must be > 0");
        return NULL;
    }

    if (avg_size < min_size) {
        PyErr_SetString(PyExc_ValueError, "avg_size must be >= min_size");
        return NULL;
    }

    if (max_size < avg_size) {
        PyErr_SetString(PyExc_ValueError, "max_size must be >= avg_size");
        return NULL;
    }

    ChunkIterator *iterator = PyObject_New(ChunkIterator, &ChunkIteratorType);
    if (iterator == NULL) {
        return NULL;
    }

    /* Inicializar antes de cualquier camino que pueda decref/dealloc. */
    iterator->buffer_active = 0;
    iterator->view.buf = NULL;
    iterator->view.obj = NULL;

    if (PyObject_GetBuffer(buffer_obj, &iterator->view, PyBUF_CONTIG_RO) != 0) {
        Py_DECREF(iterator);
        return NULL;
    }

    iterator->pre_avg_mask = (uint64_t)pre_avg_mask;
    iterator->post_avg_mask = (uint64_t)post_avg_mask;
    iterator->min_size = min_size;
    iterator->avg_size = avg_size;
    iterator->max_size = max_size;
    iterator->current_pos = 0;
    iterator->last_split = 0;
    iterator->fingerprint = 0;
    iterator->buffer_active = 1;

    return (PyObject *)iterator;
}


/* ============================================================================
 * Módulo
 * ========================================================================== */

static PyMethodDef FastRabinMethods[] = {
    {"get_chunk_boundaries", get_chunk_boundaries, METH_VARARGS, "Return a lazy CDC iterator."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef fast_rabin_module = {
    PyModuleDef_HEAD_INIT,
    "fast_rabin",
    "High-performance FastCDC-style iterator",
    -1,
    FastRabinMethods
};

PyMODINIT_FUNC PyInit_fast_rabin(void) {
    init_tables();

    if (PyType_Ready(&ChunkIteratorType) < 0) {
        return NULL;
    }

    PyObject *module = PyModule_Create(&fast_rabin_module);
    if (module == NULL) {
        return NULL;
    }

    Py_INCREF(&ChunkIteratorType);
    if (PyModule_AddObject(module, "ChunkIterator", (PyObject *)&ChunkIteratorType) < 0) {
        Py_DECREF(&ChunkIteratorType);
        Py_DECREF(module);
        return NULL;
    }

    return module;
}
