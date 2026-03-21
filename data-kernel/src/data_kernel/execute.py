"""
Simple SQL execution interface for data-kernel package.
"""
from . import arrow_bridge
import pandas as pd


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
    arrow_array = arrow_bridge.execute_query(sql)
    
    if arrow_array is None:
        return None
    
    # Convert Arrow array to pandas DataFrame
    result_list = arrow_array.to_pylist()
    return pd.DataFrame({'result': result_list})
