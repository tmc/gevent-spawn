from setuptools import setup

setup(name='gevent-spawn',
      version='0.1',
      description='Simple wsgi server on top of gevent',
      author='Ludvig Ericson',
      author_email='ludvig@lericson.se',
      url='http://bitbucket.org/lericson/gevent-spawn',
      packages=['gevent_spawn'],
      scripts=['gevent-spawn'],
      zip_safe=False,
      install_requires=['gevent', 'python-daemon'],
     )
