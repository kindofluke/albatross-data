from ipykernel.kernelbase import Kernel
import os
from . import arrow_bridge

try:
    from jupyter_mimetypes._proxy import _ProxyObject
    HAS_JUPYTER_MIMETYPES = True
except ImportError:
    HAS_JUPYTER_MIMETYPES = False

class DataKernel(Kernel):
    implementation = 'data-kernel'
    implementation_version = '0.1.0'
    banner = "Data Kernel"
    language = 'sql'
    language_version = '0.1'
    language_info = {
        'name': 'sql',
        'mimetype': 'text/x-sql',
        'file_extension': '.sql',
    }

    def do_execute(self, code, silent, store_history=True, user_expressions=None,
                   allow_stdin=False):
        if not silent:
            try:
                arrow_array = arrow_bridge.execute_query(code)

                if arrow_array is not None:
                    # Convert Arrow array to pandas for rich display
                    import pyarrow as pa
                    import pandas as pd
                    
                    # Create a RecordBatch with proper schema
                    # For now, we get a single column array, so create a simple table
                    result_list = arrow_array.to_pylist()
                    
                    # Create a pandas Series/DataFrame from the result
                    if len(result_list) == 1:
                        # Single value - create a simple display
                        df = pd.DataFrame({'result': result_list})
                    else:
                        # Multiple values - create a DataFrame
                        df = pd.DataFrame({'result': result_list})
                    
                    # Use jupyter-mimetypes if available for Arrow IPC stream
                    if HAS_JUPYTER_MIMETYPES:
                        from jupyter_mimetypes._io._pandas import _serialize_pandas
                        from jupyter_mimetypes._constants import _DEFAULT_ARROW_MIMETYPE
                        
                        # Get standard pandas representations (text/html)
                        proxy = _ProxyObject(df)
                        data, metadata = proxy._repr_mimebundle_()
                        
                        # Add Arrow IPC stream manually
                        arrow_bytes = _serialize_pandas(df)
                        data[_DEFAULT_ARROW_MIMETYPE] = arrow_bytes
                        metadata[_DEFAULT_ARROW_MIMETYPE] = {
                            'type': ('pandas.core.frame', 'DataFrame')
                        }
                    else:
                        # Fallback to simple text representation
                        if len(result_list) == 1:
                            output = str(result_list[0])
                        elif len(result_list) <= 100:
                            output = '\n'.join(str(row) for row in result_list)
                        else:
                            output = '\n'.join(str(row) for row in result_list[:100])
                            output += f'\n... ({len(result_list) - 100} more rows)'
                        data = {'text/plain': output}
                        metadata = {}
                    
                    # Send display data
                    display_data = {
                        'data': data,
                        'metadata': metadata
                    }
                    self.send_response(self.iopub_socket, 'display_data', display_data)
                else:
                    stream_content = '(empty result)'
                    self.send_response(self.iopub_socket, 'stream', {'name': 'stdout', 'text': stream_content})

            except Exception as e:
                import traceback
                stream_content = f"Error executing query: {e}\n{traceback.format_exc()}"
                self.send_response(self.iopub_socket, 'stream', {'name': 'stderr', 'text': stream_content})
                return {'status': 'error', 'execution_count': self.execution_count,
                        'ename': 'ExecutionError', 'evalue': str(e), 'traceback': []}

        return {'status': 'ok', 'execution_count': self.execution_count,
                'payload': [], 'user_expressions': {}}
