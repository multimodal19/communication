from setuptools import setup

setup(
    name='communication',
    version='1.0.0',
    description='Simple Pub-Sub communication with ZeroMQ',
    url='https://github.com/multimodal19/communication.git',
    author='Martin Disch',
    author_email='martindisch@gmail.com',
    license='MIT',
    packages=['communication'],
    install_requires=['pyzmq >= 18.0.0'],
    zip_safe=False
)
