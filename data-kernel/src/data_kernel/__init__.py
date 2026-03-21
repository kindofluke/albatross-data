from .kernel import DataKernel
from .execute import execute
from ipykernel.kernelapp import IPKernelApp

def main() -> None:
    """Entry point for the data-kernel console script"""
    IPKernelApp.launch_instance(kernel_class=DataKernel)

__all__ = ['DataKernel', 'execute', 'main']
