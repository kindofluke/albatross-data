from .kernel import DataKernel
from .execute import execute, is_gpu_available, get_gpu_info
from ipykernel.kernelapp import IPKernelApp

def main() -> None:
    """Entry point for the data-kernel console script"""
    IPKernelApp.launch_instance(kernel_class=DataKernel)

__all__ = ['DataKernel', 'execute', 'is_gpu_available', 'get_gpu_info', 'main']
