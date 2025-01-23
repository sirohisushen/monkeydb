from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='monkeydb',
    version='0.1.0',
    author='Sushen Sirohi',
    author_email='sushensirohi@gmail.com',
    description='A lightweight Python database engine',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/shanesirohi7/monkeydb',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires='>=3.7',
    keywords='database json storage lightweight',
)