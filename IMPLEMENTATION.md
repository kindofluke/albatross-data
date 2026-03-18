Architecture Plan: GPU-Accelerated Composable Data Pipeline

Core Pipeline: User SQL → DataFusion (Parses & Optimizes) → Outputs Substrait → Sirius (GPU Execution) → Results
Overview

This architecture decouples the "brain" (query planning) from the "muscle" (query execution). By bypassing a monolithic database like PostgreSQL or Apache Doris entirely, we eliminate CPU bottlenecks, storage engine overhead, and complex MVCC logic. Instead, we use Apache DataFusion purely as a SQL frontend and optimizer, serialize the plan using the open Substrait standard, and pass it to Sirius to execute relational algebra directly on NVIDIA GPUs using libcudf.
Component 1: User SQL (The Interface)

This is the entry point for the user or application. The user provides standard SQL queries to analyze data resting in columnar formats (like Apache Parquet or Arrow IPC).

    Role: Define the analytical workload.

    Implementation Details:

        The frontend can be a simple Python script or a Rust application.

        We will define local file paths or cloud URIs (S3) pointing to the raw Parquet/Arrow datasets.

        Requirement: The data must be stored purely columnarly. No row-oriented (CSV/JSON) data should be used in the benchmark to ensure zero-copy GPU memory transfers.

Component 2: Apache DataFusion (Parser & Optimizer)

DataFusion acts as the "Control Plane." It does not execute the query or touch the data payload; it only requires the table schemas to validate and optimize the SQL.

    Role: SQL parsing, schema validation, and cost/rule-based query optimization (e.g., predicate pushdown, join reordering).

    Implementation Details (Rust / Python):

        Initialize Context: Create a DataFusion SessionContext.

        Register Schemas: Register the target Parquet/Arrow files with the context (ctx.register_parquet(...)). This loads the metadata (column names, types, statistics) without loading the actual data into CPU memory.

        Parse SQL: Pass the user's SQL string to DataFusion (ctx.sql(query)).

        Optimize: DataFusion automatically generates an optimized LogicalPlan.

Component 3: Substrait (The Intermediate Representation)

Substrait is the universal translator between DataFusion (written in Rust) and Sirius (written in C++/CUDA).

    Role: Serialize the optimized DataFusion LogicalPlan into a standardized Protobuf message representing relational algebra.

    Implementation Details:

        Dependency: Use the datafusion-substrait crate (if building in Rust) or the equivalent Python bindings.

        Serialization: Call the Substrait producer to convert the DataFusion plan into a Substrait Plan message.

        Handoff: Write the Protobuf message to an in-memory byte buffer or a temporary .pb (Protobuf) file to be ingested by Sirius.

Component 4: Sirius (GPU Execution)

Sirius is the execution engine. It intercepts the Substrait plan, manages the GPU VRAM, and executes the math using NVIDIA's libcudf.

    Role: Direct GPU execution of the analytical workload.

    Implementation Details:

        Ingestion: The Sirius C++ binary/service reads the Substrait .pb plan.

        Data Loading (GPUDirect): Sirius reads the file paths defined in the Substrait plan. Using libcudf's I/O readers, it streams the Parquet/Arrow files directly into GPU VRAM (bypassing the CPU heap wherever possible).

        Execution: Sirius maps the Substrait relational operators (e.g., Filter, Project, HashJoin) to highly optimized libcudf kernels.

        Memory Management: Relies on RMM (RAPIDS Memory Manager) to handle VRAM allocations and prevent out-of-memory (OOM) crashes during complex joins.

Component 5: Results

Once the GPU finishes crunching the data, the highly condensed result set needs to be returned to the user.

    Role: Delivering the final dataset back to the host environment.

    Implementation Details:

        Sirius holds the final result as a cuDF Table in GPU memory.

        This table is copied back to host (CPU) memory as an Apache Arrow RecordBatch.

        The Python/Rust frontend ingests this Arrow data, which can then be displayed to the user, converted to a Pandas DataFrame, or written back to disk.

Action Items for the Next Session

To physically build this PoC, we will need to execute the following steps in our next session:

    Environment Setup:

        Provision a machine with an NVIDIA GPU (e.g., AWS G4dn/G5 instance or local RTX card).

        Install the NVIDIA CUDA Toolkit, Docker (with NVIDIA Container Toolkit), and Python/Rust environments.

    Generate Test Data:

        Write a script to generate a 10GB+ dataset (e.g., TPC-H lineitem table) and save it as partitioned Parquet files.

    Write the DataFusion Frontend:

        Draft the Python/Rust code to register the Parquet files, parse a test SQL query, and output the plan.pb (Substrait file).

    Compile & Run Sirius:

        Pull the Sirius repository, compile it against the local CUDA/cuDF environment, and feed it the plan.pb file.

    Benchmark:

        Measure the end-to-end execution time and compare it against standard PostgreSQL and CPU-only DataFusion.




# Other Implemenation Details 

The Frontend & Planner (Rust): You write a standard Cargo binary. It uses the native datafusion crate to read the Parquet schemas and parse the SQL, and the datafusion-substrait crate to generate the query plan.

    The FFI Boundary (Rust → C++): This is now your only bridge. You pass the Substrait byte array from Rust into Sirius.

    The Execution (C++/CUDA): Sirius executes the plan on the GPU.

    The Results (C++ → Rust): Sirius passes the results back across the FFI boundary using the Arrow C Data Interface, which the Rust arrow crate natively understands.

The Remaining Challenge: The Rust-to-C++ Bridge

Because you are staying in Rust, the only heavy lifting left is getting Rust to talk to Sirius (C++). Fortunately, the Rust ecosystem has excellent tools for this:

    For the Function Calls (cxx or bindgen): You will use a crate like cxx (which provides safe interop between Rust and C++) or bindgen (which automatically generates Rust FFI bindings from C++ headers) to call the Sirius execution function and pass it your Substrait protobuf bytes.

    For the Zero-Copy Data Return (arrow::ffi): When Sirius finishes executing on the GPU, it will return an Apache Arrow dataset. The Rust arrow crate has a built-in ffi module. You can take the raw C pointers that Sirius hands back and instantly convert them into a native Rust RecordBatch without copying a single byte of the underlying data.
