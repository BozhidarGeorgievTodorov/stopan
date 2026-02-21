#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>

static uint64_t gear_table[256];
static int table_ready = 0;

static void init_tables(void) {
    uint64_t value = 0x123456789abcdef0ULL;

    for (int i = 0; i < 256; i++) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        gear_table[i] = value;
    }

    table_ready = 1;
}

static PyObject *get_chunk_boundaries(PyObject *self, PyObject *args) {
    PyObject *buffer_obj = NULL;
    Py_buffer view;
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

    if (PyObject_GetBuffer(buffer_obj, &view, PyBUF_CONTIG_RO) != 0) {
        return NULL;
    }

    const uint8_t *data = (const uint8_t *)view.buf;
    Py_ssize_t data_len = view.len;

    PyObject *boundaries = PyList_New(0);
    if (boundaries == NULL) {
        PyBuffer_Release(&view);
        return NULL;
    }

    if (data_len == 0) {
        PyBuffer_Release(&view);
        return boundaries;
    }

    if (!table_ready) {
        init_tables();
    }

    uint64_t fingerprint = 0;
    Py_ssize_t last_split = 0;

    for (Py_ssize_t i = 0; i < data_len; i++) {
        fingerprint = (fingerprint << 1) + gear_table[data[i]];
        Py_ssize_t current_size = i - last_split + 1;

        if (current_size < min_size) {
            continue;
        }

        if (((fingerprint & mask) == 0) || current_size >= max_size) {
            PyObject *boundary = PyLong_FromSsize_t(i + 1);
            if (boundary == NULL || PyList_Append(boundaries, boundary) < 0) {
                Py_XDECREF(boundary);
                Py_DECREF(boundaries);
                PyBuffer_Release(&view);
                return NULL;
            }

            Py_DECREF(boundary);
            last_split = i + 1;
            fingerprint = 0;
        }
    }

    if (last_split < data_len) {
        PyObject *boundary = PyLong_FromSsize_t(data_len);
        if (boundary == NULL || PyList_Append(boundaries, boundary) < 0) {
            Py_XDECREF(boundary);
            Py_DECREF(boundaries);
            PyBuffer_Release(&view);
            return NULL;
        }
        Py_DECREF(boundary);
    }

    PyBuffer_Release(&view);
    return boundaries;
}

static PyMethodDef FastRabinMethods[] = {
    {"get_chunk_boundaries", get_chunk_boundaries, METH_VARARGS, "Calculate CDC chunk boundaries."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef fast_rabin_module = {
    PyModuleDef_HEAD_INIT,
    "fast_rabin",
    NULL,
    -1,
    FastRabinMethods
};

PyMODINIT_FUNC PyInit_fast_rabin(void) {
    init_tables();
    return PyModule_Create(&fast_rabin_module);
}
