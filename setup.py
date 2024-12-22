from setuptools import setup, find_packages

setup(
    name="klm_data_analysis",  
    author='Depankar Sarkar',
    version="0.1",     # Version of your package
    packages=find_packages(),  # Automatically find packages
    long_description=open('README.md').read(),
    install_requires=['pyspark','coverage','discover'],  # List of dependencies
    python_requires='>=3.6'
)