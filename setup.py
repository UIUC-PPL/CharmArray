import sys
from setuptools import setup, find_packages

try:
    from Cython.Distutils import Extension
    from Cython.Build import cythonize
except ImportError:
    from distutils.core import Extension

    def cythonize(*args, **kw):
        return args[0]


def get_version():
    import os
    data = {}
    fname = os.path.join('charm.numpy', '__init__.py')
    exec(compile(open(fname).read(), fname, 'exec'), data)
    return data.get('__version__')


install_requires = ['cython', 'numpy', 'charm4py']
tests_require = ['pytest']
if sys.version_info[0] < 3:
    tests_require += ['mock>=1.0']
docs_require = ['sphinx']

classes = '''
Development Status :: 4 - Beta
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: BSD License
Natural Language :: English
Operating System :: MacOS :: MacOS X
Operating System :: Microsoft :: Windows
Operating System :: POSIX
Operating System :: Unix
Programming Language :: Python
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Topic :: Scientific/Engineering
Topic :: Software Development :: Libraries
Topic :: Utilities
'''
classifiers = [x.strip() for x in classes.splitlines() if x]

ext_modules = [
        Extension(
            name="charm.numpy.core",
            sources=["charm/numpy/core.pyx"],
            language="c++"
            ),
        ]

setup(
    name='charm.numpy',
    version=get_version(),
    author='Aditya Bhosale',
    author_email='adityapb1546@gmail.com',
    description='A python library for distributed array computations',
    long_description=open('README.rst').read(),
    license="BSD",
    url='https://github.com/adityapb/charm.numpy',
    classifiers=classifiers,
    packages=find_packages(),
    ext_modules=cythonize(ext_modules, language="c++"),
    install_requires=install_requires,
    extras_require={
        "docs": docs_require,
        "tests": tests_require,
        "dev": docs_require + tests_require,
    },
)
