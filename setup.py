import shutil
import sys

from setuptools import Extension, find_packages, setup


def get_extensions():
    """Build the optional Cython extension only when a native compiler is available."""
    if sys.platform.startswith("win") and shutil.which("cl") is None:
        print(
            "No MSVC compiler detected; building without the Cython extension "
            "and using the pure-Python fallback implementation."
        )
        return []

    try:
        from Cython.Build import cythonize
    except ImportError:
        print("Cython is unavailable; using the pure-Python fallback implementation.")
        return []

    extensions = [
        Extension(
            "chronosmatch.engine.matching_core",
            ["src/chronosmatch/engine/matching_core.pyx"],
        )
    ]
    return cythonize(extensions, compiler_directives={"language_level": "3"})


setup(
    name="chronosmatch",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    ext_modules=get_extensions(),
)
