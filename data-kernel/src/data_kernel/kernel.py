from ipykernel.kernelbase import Kernel

class DataKernel(Kernel):
    implementation = 'data-kernel'
    implementation_version = '0.1.0'
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
            import subprocess
            import pandas as pd
            from io import StringIO
            import os

            # For now, hardcode the path to the parquet file
            parquet_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../data-embed/data/orders_10m.parquet'))
            table_name = os.path.splitext(os.path.basename(parquet_file))[0]

            executor_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../bin/data-run'))

            if not os.path.exists(executor_path):
                stream_content = 'Executor not found. Please run "make build" in the root directory.'
                self.send_response(self.iopub_socket, 'stream', {'name': 'stdout', 'text': stream_content})
                return {'status': 'error', 'execution_count': self.execution_count,
                        'ename': 'ExecutorNotFound', 'evalue': stream_content, 'traceback': []}

            try:
                result = subprocess.run(
                    [executor_path, '-f', parquet_file, '-t', table_name, '-q', code],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                output = result.stdout
                df = pd.read_csv(StringIO(output))
                html_table = df.to_html()

                display_data = {
                    'data': {
                        'text/html': html_table
                    },
                    'metadata': {}
                }
                self.send_response(self.iopub_socket, 'display_data', display_data)

            except subprocess.CalledProcessError as e:
                stream_content = f"Error executing query: {e.stderr}"
                self.send_response(self.iopub_socket, 'stream', {'name': 'stderr', 'text': stream_content})
                return {'status': 'error', 'execution_count': self.execution_count,
                        'ename': 'ExecutionError', 'evalue': e.stderr, 'traceback': []}


        return {'status': 'ok', 'execution_count': self.execution_count,
                'payload': [], 'user_expressions': {}}
