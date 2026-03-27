#include <Python.h>
#include <stdint.h>

// Arrow C Data Interface structures
struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

extern int32_t execute_query_to_arrow(
    const char* query,
    const char* data_path,
    const struct ArrowArray** array,
    const struct ArrowSchema** schema
);

// GPU detection functions from Rust
typedef struct {
    char* name;
    char* backend;
    char* device_type;
    char* driver;
    char* driver_info;
    int32_t available;
} CGpuInfo;

extern int32_t check_gpu_available(void);
extern CGpuInfo* get_gpu_information(void);
extern void free_gpu_info(CGpuInfo* info);

// Destructor for ArrowSchema PyCapsule
static void release_arrow_schema_capsule(PyObject* capsule) {
    struct ArrowSchema* schema = 
        (struct ArrowSchema*)PyCapsule_GetPointer(capsule, "arrow_schema");
    if (schema != NULL && schema->release != NULL) {
        schema->release(schema);
    }
}

// Destructor for ArrowArray PyCapsule
static void release_arrow_array_capsule(PyObject* capsule) {
    struct ArrowArray* array = 
        (struct ArrowArray*)PyCapsule_GetPointer(capsule, "arrow_array");
    if (array != NULL && array->release != NULL) {
        array->release(array);
    }
}

static PyObject* execute_query(PyObject* self, PyObject* args) {
    const char* query;
    if (!PyArg_ParseTuple(args, "s", &query)) {
        return NULL;
    }

    // Get data path from environment or use default
    const char* data_path = getenv("DATA_PATH");
    if (data_path == NULL) {
        data_path = "/opt/data";
    }

    const struct FFI_ArrowArray* array_ptr = NULL;
    const struct FFI_ArrowSchema* schema_ptr = NULL;

    int32_t result = execute_query_to_arrow(query, data_path, &array_ptr, &schema_ptr);

    if (result != 0) {
        char error_msg[256];
        snprintf(error_msg, sizeof(error_msg), 
                 "Failed to execute query in Rust (error code: %d)", result);
        PyErr_SetString(PyExc_RuntimeError, error_msg);
        return NULL;
    }

    if (array_ptr == NULL || schema_ptr == NULL) {
        Py_RETURN_NONE;
    }

    // Create PyCapsules for Arrow C Data Interface
    // The capsules now own the pointers and will call release callbacks
    PyObject* schema_capsule = PyCapsule_New(
        (void*)schema_ptr, 
        "arrow_schema", 
        release_arrow_schema_capsule
    );
    if (!schema_capsule) {
        // Manually release if capsule creation fails
        struct ArrowSchema* s = (struct ArrowSchema*)schema_ptr;
        if (s && s->release) s->release(s);
        struct ArrowArray* a = (struct ArrowArray*)array_ptr;
        if (a && a->release) a->release(a);
        return NULL;
    }

    PyObject* array_capsule = PyCapsule_New(
        (void*)array_ptr, 
        "arrow_array", 
        release_arrow_array_capsule
    );
    if (!array_capsule) {
        Py_DECREF(schema_capsule);  // This will call release_arrow_schema_capsule
        struct ArrowArray* a = (struct ArrowArray*)array_ptr;
        if (a && a->release) a->release(a);
        return NULL;
    }

    // Import pyarrow and create RecordBatch from capsules
    PyObject* pyarrow_module = PyImport_ImportModule("pyarrow");
    if (!pyarrow_module) {
        Py_DECREF(schema_capsule);
        Py_DECREF(array_capsule);
        PyErr_SetString(PyExc_ImportError, "pyarrow could not be imported.");
        return NULL;
    }

    // Get the RecordBatch class
    PyObject* recordbatch_class = PyObject_GetAttrString(pyarrow_module, "RecordBatch");
    if (!recordbatch_class) {
        Py_DECREF(pyarrow_module);
        Py_DECREF(schema_capsule);
        Py_DECREF(array_capsule);
        PyErr_SetString(PyExc_AttributeError, "RecordBatch class not found in pyarrow.");
        return NULL;
    }

    // Call RecordBatch._import_from_c_capsule(schema_capsule, array_capsule)
    // Note: order is schema first, then array
    PyObject* import_method = PyObject_GetAttrString(recordbatch_class, "_import_from_c_capsule");
    if (!import_method) {
        Py_DECREF(recordbatch_class);
        Py_DECREF(pyarrow_module);
        Py_DECREF(schema_capsule);
        Py_DECREF(array_capsule);
        PyErr_SetString(PyExc_AttributeError, "_import_from_c_capsule not found.");
        return NULL;
    }

    PyObject* arrow_recordbatch = PyObject_CallFunctionObjArgs(
        import_method,
        schema_capsule,
        array_capsule,
        NULL
    );

    // Clean up
    Py_DECREF(import_method);
    Py_DECREF(recordbatch_class);
    Py_DECREF(pyarrow_module);
    Py_DECREF(schema_capsule);
    Py_DECREF(array_capsule);

    return arrow_recordbatch;
}

static PyObject* check_gpu(PyObject* self, PyObject* args) {
    int32_t result = check_gpu_available();
    if (result == 1) {
        Py_RETURN_TRUE;
    } else {
        Py_RETURN_FALSE;
    }
}

static PyObject* get_gpu_info(PyObject* self, PyObject* args) {
    CGpuInfo* info = get_gpu_information();

    if (info == NULL) {
        Py_RETURN_NONE;
    }

    // Create Python dictionary with GPU info
    PyObject* dict = PyDict_New();
    if (dict == NULL) {
        free_gpu_info(info);
        return NULL;
    }

    // Add name field
    if (info->name != NULL) {
        PyObject* name = PyUnicode_FromString(info->name);
        if (name == NULL) {
            Py_DECREF(dict);
            free_gpu_info(info);
            return NULL;
        }
        PyDict_SetItemString(dict, "name", name);
        Py_DECREF(name);
    }

    // Add backend field
    if (info->backend != NULL) {
        PyObject* backend = PyUnicode_FromString(info->backend);
        if (backend == NULL) {
            Py_DECREF(dict);
            free_gpu_info(info);
            return NULL;
        }
        PyDict_SetItemString(dict, "backend", backend);
        Py_DECREF(backend);
    }

    // Add device_type field
    if (info->device_type != NULL) {
        PyObject* device_type = PyUnicode_FromString(info->device_type);
        if (device_type == NULL) {
            Py_DECREF(dict);
            free_gpu_info(info);
            return NULL;
        }
        PyDict_SetItemString(dict, "device_type", device_type);
        Py_DECREF(device_type);
    }

    // Add driver field
    if (info->driver != NULL) {
        PyObject* driver = PyUnicode_FromString(info->driver);
        if (driver == NULL) {
            Py_DECREF(dict);
            free_gpu_info(info);
            return NULL;
        }
        PyDict_SetItemString(dict, "driver", driver);
        Py_DECREF(driver);
    }

    // Add driver_info field
    if (info->driver_info != NULL) {
        PyObject* driver_info = PyUnicode_FromString(info->driver_info);
        if (driver_info == NULL) {
            Py_DECREF(dict);
            free_gpu_info(info);
            return NULL;
        }
        PyDict_SetItemString(dict, "driver_info", driver_info);
        Py_DECREF(driver_info);
    }

    // Add available field
    PyObject* available = info->available ? Py_True : Py_False;
    Py_INCREF(available);
    PyDict_SetItemString(dict, "available", available);
    Py_DECREF(available);

    // Free the C struct
    free_gpu_info(info);

    return dict;
}

static PyMethodDef ArrowBridgeMethods[] = {
    {"execute_query", execute_query, METH_VARARGS, "Execute a SQL query and return a pyarrow.Table"},
    {"check_gpu", check_gpu, METH_NOARGS, "Check if GPU is available"},
    {"get_gpu_info", get_gpu_info, METH_NOARGS, "Get detailed GPU information"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef arrow_bridge_module = {
    PyModuleDef_HEAD_INIT,
    "arrow_bridge",
    "A bridge to call Rust functions that return Arrow data.",
    -1,
    ArrowBridgeMethods
};

PyMODINIT_FUNC PyInit_arrow_bridge(void) {
    return PyModule_Create(&arrow_bridge_module);
}
