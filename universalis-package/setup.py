import setuptools

setuptools.setup(
    name="universalis",
    version="0.0.1",
    author="Kyriakos Psarakis",
    packages=setuptools.find_packages(),
    install_requires=[
        'cloudpickle>=2.1.0>,<3.0.0',
        'msgpack>=1.0.3,<2.0.0',
        'aiokafka>=0.7.2,<1.0',
        'confluent_kafka>=1.9.0,<2.0.0 ',
    ],
    python_requires='>=3.7',
)
