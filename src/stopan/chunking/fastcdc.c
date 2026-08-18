#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdint.h>

/*
 * FastCDC parametrizado para Stopan.
 *
 * Implementa las tres técnicas que definen FastCDC:
 *   1) juicio Gear mediante (fingerprint & mask) == 0;
 *   2) cut-point skipping: no se calcula el fingerprint antes de min_size;
 *   3) normalized chunking: máscara más selectiva antes de avg_size y
 *      menos selectiva después, con nivel de normalización 2.
 *
 * Las máscaras publicadas por FastCDC están fijadas al caso de 8 KiB y fueron
 * obtenidas empíricamente. Para conservar la parametrización del sistema, este
 * módulo reproduce exactamente esas máscaras cuando avg_size == 8 KiB y,
 * para otros tamaños potencia de dos, genera una máscara determinista con el
 * mismo número de bits efectivos distribuidos sobre una ventana Gear de 48 bits.
 *
 * El iterador devuelve offsets absolutos de final de chunk, en orden creciente.
 */

#if defined(__GNUC__) || defined(__clang__)
    #define UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
    #define UNLIKELY(x) (x)
#endif

#define FASTCDC_WINDOW_BITS 48
#define FASTCDC_NORMALIZATION_LEVEL 2
#define FASTCDC_MASK_S_8K 0x0000d9f003530000ULL
#define FASTCDC_MASK_A_8K 0x0000d93003530000ULL
#define FASTCDC_MASK_L_8K 0x0000d90003530000ULL

/* ============================================================================
 * Tabla Gear
 * ========================================================================== */

static uint64_t gear_table[256];

static void init_tables(void) {
    /*
     * Tabla pseudoaleatoria fija. FastCDC solo exige una correspondencia estable
     * de los 256 valores de byte a enteros de 64 bits. Se conserva la tabla que
     * ya utilizaba el chunker anterior para aislar el cambio algorítmico.
     */
    uint64_t value = 0x123456789abcdef0ULL;

    for (int i = 0; i < 256; i++) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        gear_table[i] = value;
    }
}

/* ============================================================================
 * Máscaras FastCDC
 * ========================================================================== */

static int power_of_two_log2(Py_ssize_t value) {
    if (value <= 0) {
        return -1;
    }

    uint64_t v = (uint64_t)value;
    if ((v & (v - 1ULL)) != 0) {
        return -1;
    }

    int log2 = 0;
    while (v > 1ULL) {
        v >>= 1;
        log2++;
    }
    return log2;
}

static uint64_t spread_mask(int effective_bits) {
    /*
     * El paper distribuye casi uniformemente bits cero entre los bits efectivos
     * para ampliar la ventana de Gear hasta 48 bytes. Para tamaños distintos del
     * caso publicado de 8 KiB hacemos explícita esa generalización: colocamos los
     * bits efectivos uniformemente entre las posiciones 0 y 47.
     */
    if (effective_bits <= 0 || effective_bits > FASTCDC_WINDOW_BITS) {
        return 0;
    }

    if (effective_bits == 1) {
        return 1ULL << (FASTCDC_WINDOW_BITS - 1);
    }

    uint64_t mask = 0;
    for (int i = 0; i < effective_bits; i++) {
        int pos = (i * (FASTCDC_WINDOW_BITS - 1)) / (effective_bits - 1);
        mask |= 1ULL << pos;
    }
    return mask;
}

static int build_masks(
        Py_ssize_t avg_size,
        uint64_t *pre_avg_mask,
        uint64_t *post_avg_mask) {
    int avg_bits = power_of_two_log2(avg_size);
    if (avg_bits < 0) {
        return -1;
    }

    int strong_bits = avg_bits + FASTCDC_NORMALIZATION_LEVEL;
    int relaxed_bits = avg_bits - FASTCDC_NORMALIZATION_LEVEL;
    if (relaxed_bits <= 0 || strong_bits > FASTCDC_WINDOW_BITS) {
        return -1;
    }

    if (avg_size == 8192) {
        *pre_avg_mask = FASTCDC_MASK_S_8K;
        *post_avg_mask = FASTCDC_MASK_L_8K;
        return 0;
    }

    *pre_avg_mask = spread_mask(strong_bits);
    *post_avg_mask = spread_mask(relaxed_bits);
    return (*pre_avg_mask != 0 && *post_avg_mask != 0) ? 0 : -1;
}

/* ============================================================================
 * Iterador de puntos de corte
 * ========================================================================== */

typedef struct {
    PyObject_HEAD
    Py_buffer view;
    uint64_t pre_avg_mask;
    uint64_t post_avg_mask;
    Py_ssize_t min_size;
    Py_ssize_t avg_size;
    Py_ssize_t max_size;
    Py_ssize_t last_split;
    int buffer_active;
    int in_next;
} BoundaryIterator;

static void BoundaryIterator_dealloc(BoundaryIterator *self) {
    if (self->buffer_active) {
        PyBuffer_Release(&self->view);
        self->buffer_active = 0;
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *BoundaryIterator_iter(PyObject *self) {
    Py_INCREF(self);
    return self;
}

static Py_ssize_t BoundaryIterator_next_boundary(BoundaryIterator *self) {
    const uint8_t *data = (const uint8_t *)self->view.buf;
    const Py_ssize_t data_len = self->view.len;
    const Py_ssize_t start = self->last_split;

    Py_ssize_t max_split = start + self->max_size;
    if (max_split > data_len) {
        max_split = data_len;
    }

    Py_ssize_t normal_split = start + self->avg_size;
    if (normal_split > max_split) {
        normal_split = max_split;
    }

    /*
     * FastCDC cut-point skipping: los cortes inferiores a min_size se omiten
     * por completo. El pseudocódigo del artículo expresa el breakpoint i como
     * longitud del chunk. Nuestra API devuelve offsets absolutos exclusivos,
     * por lo que el candidato inicial es start + min_size y el byte que lo
     * determina es data[candidate - 1]. Así un chunk de exactamente min_size
     * bytes puede ser una frontera dependiente del contenido.
     */
    Py_ssize_t candidate = start + self->min_size;
    uint64_t fingerprint = 0;

    /* Región normalizada estricta: longitudes menores que avg_size. */
    for (; candidate < normal_split; candidate++) {
        fingerprint = (fingerprint << 1) + gear_table[data[candidate - 1]];
        if (UNLIKELY((fingerprint & self->pre_avg_mask) == 0)) {
            return candidate;
        }
    }

    /* Región normalizada relajada: desde avg_size hasta antes de max_size. */
    for (; candidate < max_split; candidate++) {
        fingerprint = (fingerprint << 1) + gear_table[data[candidate - 1]];
        if (UNLIKELY((fingerprint & self->post_avg_mask) == 0)) {
            return candidate;
        }
    }

    /* Ninguna coincidencia: el máximo o EOF actúa como frontera forzada. */
    return max_split;
}

static PyObject *BoundaryIterator_iternext(PyObject *self_obj) {
    BoundaryIterator *self = (BoundaryIterator *)self_obj;
    const Py_ssize_t data_len = self->view.len;

    if (self->last_split >= data_len) {
        return NULL;  /* StopIteration */
    }

    if (data_len - self->last_split <= self->min_size) {
        self->last_split = data_len;
        return PyLong_FromSsize_t(data_len);
    }

    if (self->in_next) {
        PyErr_SetString(PyExc_RuntimeError, "BoundaryIterator no admite next() concurrente");
        return NULL;
    }

    self->in_next = 1;
    Py_ssize_t split_point;
    if (self->view.readonly) {
        Py_BEGIN_ALLOW_THREADS
        split_point = BoundaryIterator_next_boundary(self);
        Py_END_ALLOW_THREADS
    } else {
        /* Un búfer mutable conserva el GIL para impedir mutaciones Python
         * concurrentes durante el cálculo de una frontera. */
        split_point = BoundaryIterator_next_boundary(self);
    }
    self->in_next = 0;

    self->last_split = split_point;
    return PyLong_FromSsize_t(split_point);
}

static PyTypeObject BoundaryIteratorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "stopan.chunking.fastcdc.BoundaryIterator",
    .tp_basicsize = sizeof(BoundaryIterator),
    .tp_dealloc = (destructor)BoundaryIterator_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "Lazy iterator over FastCDC chunk boundaries",
    .tp_iter = BoundaryIterator_iter,
    .tp_iternext = BoundaryIterator_iternext,
};

/* ============================================================================
 * API Python
 * ========================================================================== */

static PyObject *iter_boundaries(PyObject *self, PyObject *args) {
    PyObject *buffer_obj = NULL;
    Py_ssize_t min_size, avg_size, max_size;

    if (!PyArg_ParseTuple(args, "Onnn", &buffer_obj, &min_size, &avg_size, &max_size)) {
        return NULL;
    }

    if (min_size <= 0) {
        PyErr_SetString(PyExc_ValueError, "min_size debe ser > 0");
        return NULL;
    }
    if (avg_size < min_size) {
        PyErr_SetString(PyExc_ValueError, "avg_size debe ser >= min_size");
        return NULL;
    }
    if (max_size < avg_size) {
        PyErr_SetString(PyExc_ValueError, "max_size debe ser >= avg_size");
        return NULL;
    }

    uint64_t pre_avg_mask, post_avg_mask;
    if (build_masks(avg_size, &pre_avg_mask, &post_avg_mask) != 0) {
        PyErr_SetString(
            PyExc_ValueError,
            "avg_size debe ser una potencia de 2 compatible con las máscaras FastCDC"
        );
        return NULL;
    }

    BoundaryIterator *iterator = PyObject_New(BoundaryIterator, &BoundaryIteratorType);
    if (iterator == NULL) {
        return NULL;
    }

    iterator->buffer_active = 0;
    iterator->view.buf = NULL;
    iterator->view.obj = NULL;

    if (PyObject_GetBuffer(buffer_obj, &iterator->view, PyBUF_CONTIG_RO) != 0) {
        Py_DECREF(iterator);
        return NULL;
    }

    iterator->pre_avg_mask = pre_avg_mask;
    iterator->post_avg_mask = post_avg_mask;
    iterator->min_size = min_size;
    iterator->avg_size = avg_size;
    iterator->max_size = max_size;
    iterator->last_split = 0;
    iterator->buffer_active = 1;
    iterator->in_next = 0;

    return (PyObject *)iterator;
}

static PyObject *mask_values(PyObject *self, PyObject *args) {
    Py_ssize_t avg_size;
    if (!PyArg_ParseTuple(args, "n", &avg_size)) {
        return NULL;
    }

    uint64_t pre_avg_mask, post_avg_mask;
    if (build_masks(avg_size, &pre_avg_mask, &post_avg_mask) != 0) {
        PyErr_SetString(
            PyExc_ValueError,
            "avg_size debe ser una potencia de 2 compatible con las máscaras FastCDC"
        );
        return NULL;
    }

    return Py_BuildValue("KK", (unsigned long long)pre_avg_mask, (unsigned long long)post_avg_mask);
}

static PyMethodDef FastCDCMethods[] = {
    {"iter_boundaries", iter_boundaries, METH_VARARGS, "Devuelve un iterador lazy de fronteras FastCDC."},
    {"mask_values", mask_values, METH_VARARGS, "Devuelve las máscaras (pre, post) derivadas del tamaño esperado."},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef fastcdc_module = {
    PyModuleDef_HEAD_INIT,
    "fastcdc",
    "FastCDC nativo con cut-point skipping y normalized chunking",
    -1,
    FastCDCMethods
};

PyMODINIT_FUNC PyInit_fastcdc(void) {
    init_tables();

    if (PyType_Ready(&BoundaryIteratorType) < 0) {
        return NULL;
    }

    PyObject *module = PyModule_Create(&fastcdc_module);
    if (module == NULL) {
        return NULL;
    }

    Py_INCREF(&BoundaryIteratorType);
    if (PyModule_AddObject(module, "BoundaryIterator", (PyObject *)&BoundaryIteratorType) < 0) {
        Py_DECREF(&BoundaryIteratorType);
        Py_DECREF(module);
        return NULL;
    }

    return module;
}
