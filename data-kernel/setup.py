from setuptools import setup, Extension

arrow_bridge = Extension(
    'data_kernel.arrow_bridge',
    sources=['src/data_kernel/arrow_bridge.c'],
    library_dirs=['../data-embed/target/release'],
    libraries=['executor'],
)

setup(
    name='data-kernel',
    version='0.1.0',
    packages=['data_kernel'],
    package_dir={'': 'src'},
    package_data={'data_kernel': ['data-run']},
    ext_modules=[arrow_bridge],
)
