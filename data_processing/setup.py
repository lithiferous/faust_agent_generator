from setuptools import setup, find_packages

requires = [
    "colorlog>=3.1.4",
    "dataclasses-avroschema",
    "faust",
    "python-schema-registry-client",
    "yarl<1.6.0,>=1.0",
    "multidict<5.0,>=4.5",
    "simple-settings",
    "typing-extensions",
]

setup(
    name='data_processing',
    version='1.0.0',
    description='integrated data processing Faust with Docker Compose',
    long_description='''
    Example running Faust with Docker Compose (zookeeper, kafka and schema-registry)
    ''',
    classifiers=[
        "Programming Language :: Python",
    ],
    author='Lithiferous',
    author_email='lithiferous2@gmail.com',
    url='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    tests_require=[],
    setup_requires=[],
    dependency_links=[],
    entry_points={
        'console_scripts': [
            'processing_module = processing_module.app:main',
        ],
        'faust.codecs': [
            'avro_users = processing_module.codecs.avro:avro_user_codec',
            'avro_advance_users = processing_module.codecs.avro:avro_advance_user_codec',
        ],
    },
)
