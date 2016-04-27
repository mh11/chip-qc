__author__ = 'mh719'

from setuptools import setup,find_packages
from codecs import open
from os import path

import chipqc

here = path.abspath(path.dirname(__file__))

setup(name='chipqc',
      version=chipqc.__version__,
      description='comprehensive ChIP-seq QC package',
      url='http://github.com/mh11/chipqc',
      author='Matthias Haimel',
      author_email='mh719+git@cam.ac.uk',
      license='MIT',
      packages=find_packages(),
      zip_safe=False,

      entry_points={
        'console_scripts':[
#            'chipqc = chipqc.ChipQc:main',
            'chipqc=chipqc:main',
        ],
        },
)

