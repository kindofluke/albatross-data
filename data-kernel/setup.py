from setuptools import setup, Extension
import os

# Use library from package directory
package_dir = os.path.join('src', 'data_kernel')

arrow_bridge = Extension(
    'data_kernel.arrow_bridge',
    sources=['src/data_kernel/arrow_bridge.c'],
    library_dirs=[package_dir],
    libraries=['executor'],
)

setup(
    name='data-kernel',
    version='0.1.0',
    packages=['data_kernel'],
    package_dir={'': 'src'},
    package_data={'data_kernel': ['data-run', 'libexecutor.dylib']},
    ext_modules=[arrow_bridge],
)
