#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>

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

typedef struct {
    PyObject_HEAD
    Py_buffer view;
    unsigned long long mask;
    Py_ssize_t min_size;
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
        return NULL;
    }

    Py_ssize_t remaining = data_len - self->last_split;
    if (remaining <= self->min_size) {
        self->last_split = data_len;
        self->current_pos = data_len;
        return PyLong_FromSsize_t(data_len);
    }

    self->fingerprint = 0;
    self->current_pos = self->last_split;

    Py_ssize_t first_check = self->last_split + self->min_size - 1;
    Py_ssize_t max_split = self->last_split + self->max_size;

    if (max_split > data_len) {
        max_split = data_len;
    }

    while (self->current_pos < first_check) {
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos]];
        self->current_pos++;
    }

    while (self->current_pos < max_split) {
        self->fingerprint = (self->fingerprint << 1) + gear_table[data[self->current_pos]];

        if ((self->fingerprint & self->mask) == 0) {
            Py_ssize_t split_point = self->current_pos + 1;
            self->last_split = split_point;
            self->current_pos = split_point;
            return PyLong_FromSsize_t(split_point);
        }

        self->current_pos++;
    }

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
    .tp_iter = ChunkIterator_iter,
    .tp_iternext = ChunkIterator_iternext,
};

static PyObject *get_chunk_boundaries(PyObject *self, PyObject *args) {
    PyObject *buffer_obj = NULL;
    unsigned long long mask;
    Py_ssize_t min_size;
    Py_ssize_t max_size;

    if (!PyArg_ParseTuple(args, "OKnn", &buffer_obj, &mask, &min_size, &max_size)) {
        return NULL;
    }

    if (min_size <= 0 || max_size < min_size) {
        PyErr_SetString(PyExc_ValueError, "Invalid chunk size limits.");
        return NULL;
    }

    ChunkIterator *iterator = PyObject_New(ChunkIterator, &ChunkIteratorType);
    if (iterator == NULL) {
        return NULL;
    }

    iterator->buffer_active = 0;

    if (PyObject_GetBuffer(buffer_obj, &iterator->view, PyBUF_CONTIG_RO) != 0) {
        Py_DECREF(iterator);
        return NULL;
    }

    iterator->mask = mask;
    iterator->min_size = min_size;
    iterator->max_size = max_size;
    iterator->current_pos = 0;
    iterator->last_split = 0;
    iterator->fingerprint = 0;
    iterator->buffer_active = 1;

    return (PyObject *)iterator;
}

static PyMethodDef FastRabinMethods[] = {
    {"get_chunk_boundaries", get_chunk_boundaries, METH_VARARGS, "Return an iterator with CDC chunk boundaries."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef fast_rabin_module = {
    PyModuleDef_HEAD_INIT,
    "core.fast_rabin",
    NULL,
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
