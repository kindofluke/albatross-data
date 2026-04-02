"""
Simple SQL execution interface for data-kernel package.
"""
import pandas as pd
from typing import Optional, Dict, Any


def execute(sql: str):
    """
    Execute a SQL query and return results as a pandas DataFrame.

    Args:
        sql: SQL query string to execute

    Returns:
        pandas.DataFrame: Query results as DataFrame, or None if empty

    Raises:
        RuntimeError: If query execution fails

    Example:
        >>> from data_kernel import execute
        >>> result = execute("SELECT * FROM my_table")
    """
    from . import arrow_bridge
    arrow_recordbatch = arrow_bridge.execute_query(sql)

    if arrow_recordbatch is None:
        return None

    # Convert Arrow RecordBatch directly to pandas DataFrame
    # This preserves the schema and column names from the query result
    return arrow_recordbatch.to_pandas()


def is_gpu_available() -> bool:
    """
    Check if a GPU is available for computation.

    Returns:
        bool: True if GPU is available, False otherwise

    Example:
        >>> from data_kernel import is_gpu_available
        >>> if is_gpu_available():
        ...     print("GPU is ready for computation")
    """
    from . import arrow_bridge
    return arrow_bridge.check_gpu()


def get_gpu_info() -> Optional[Dict[str, Any]]:
    """
    Get detailed information about the available GPU.

    This function confirms both GPU hardware presence AND that the required
    software stack (Vulkan, Metal, DirectX, etc.) is properly installed and working.

    Returns:
        dict: Dictionary with GPU information containing:
            - name (str): Name of the GPU device
            - backend (str): GPU backend type (Vulkan, Metal, Dx12, etc.)
            - device_type (str): Device type (DiscreteGpu, IntegratedGpu, VirtualGpu, etc.)
            - driver (str): Driver name
            - driver_info (str): Driver version and additional information
            - available (bool): Whether the GPU is available
        None: If no GPU is available

    Example:
        >>> from data_kernel import get_gpu_info
        >>> info = get_gpu_info()
        >>> if info:
        ...     print(f"GPU: {info['name']}")
        ...     print(f"Backend: {info['backend']}")
        ...     print(f"Type: {info['device_type']}")
        ...     print(f"Driver: {info['driver']} ({info['driver_info']})")

    Notes:
        - Backend "Vulkan" confirms Vulkan runtime is working
        - Backend "Metal" confirms Metal framework is available (macOS)
        - device_type "DiscreteGpu" indicates dedicated GPU hardware
        - device_type "IntegratedGpu" indicates integrated graphics
        - Returns None if GPU hardware or software stack is unavailable
    """
    from . import arrow_bridge
    return arrow_bridge.get_gpu_info()


def list_tables() -> Optional[Dict[str, Any]]:
    """
    Get metadata about all available Parquet files.

    This function discovers all Parquet files in the DATA_PATH directory
    and returns comprehensive metadata including schema information, row counts,
    and file sizes without executing any queries.

    Returns:
        dict: Dictionary with table metadata containing:
            - tables (list): List of table metadata, where each table has:
                - name (str): Table name (derived from filename)
                - file_path (str): Full path to the Parquet file
                - num_rows (int): Total number of rows in the table
                - file_size_bytes (int): File size in bytes
                - columns (list): List of column metadata, where each column has:
                    - name (str): Column name
                    - data_type (str): Arrow data type
                    - nullable (bool): Whether column allows null values
        None: If no Parquet files are found or an error occurs

    Example:
        >>> from data_kernel import list_tables
        >>> metadata = list_tables()
        >>> if metadata:
        ...     for table in metadata['tables']:
        ...         print(f"Table: {table['name']}")
        ...         print(f"  Rows: {table['num_rows']:,}")
        ...         print(f"  Size: {table['file_size_bytes']:,} bytes")
        ...         print(f"  Columns: {len(table['columns'])}")
        ...         for col in table['columns']:
        ...             print(f"    - {col['name']}: {col['data_type']}")

    Notes:
        - Reads from DATA_PATH environment variable (defaults to /opt/data)
        - Does not execute any queries, only reads metadata
        - Row counts are obtained via COUNT(*) queries
        - Returns empty tables list if no Parquet files found
    """
    import json
    from . import arrow_bridge

    json_str = arrow_bridge.list_tables()

    if json_str is None:
        return None

    return json.loads(json_str)
