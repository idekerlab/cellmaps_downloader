===================
cellmaps_downloader
===================


.. image:: https://img.shields.io/pypi/v/cellmaps_downloader.svg
        :target: https://pypi.python.org/pypi/cellmaps_downloader

.. image:: https://img.shields.io/travis/idekerlab/cellmaps_downloader.svg
        :target: https://travis-ci.com/idekerlab/cellmaps_downloader

.. image:: https://readthedocs.org/projects/cellmaps-downloader/badge/?version=latest
        :target: https://cellmaps-downloader.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




Downloads IF and AP-MS data needed for CM4AI MuSIC pipeline


* Free software: MIT license
* Documentation: https://cellmaps-downloader.readthedocs.io.



Dependencies
------------

* TODO add

Compatibility
-------------

* Python 3.3+

Installation
------------

.. code-block::

   git clone https://github.com/idekerlab/cellmaps_downloader
   cd cellmaps_downloader
   make dist
   pip install dist/cellmaps_downloadercmd*whl


Run **make** command with no arguments to see other build/deploy options including creation of Docker image 

.. code-block::

   make

Output:

.. code-block::

   clean                remove all build, test, coverage and Python artifacts
   clean-build          remove build artifacts
   clean-pyc            remove Python file artifacts
   clean-test           remove test and coverage artifacts
   lint                 check style with flake8
   test                 run tests quickly with the default Python
   test-all             run tests on every Python version with tox
   coverage             check code coverage quickly with the default Python
   docs                 generate Sphinx HTML documentation, including API docs
   servedocs            compile the docs watching for changes
   testrelease          package and upload a TEST release
   release              package and upload a release
   dist                 builds source and wheel package
   install              install the package to the active Python's site-packages
   dockerbuild          build docker image and store in local repository
   dockerpush           push image to dockerhub




Needed files
------------

**TODO:** Add description of needed files


Usage
-----

For information invoke :code:`cellmaps_downloadercmd.py -h`

**Example usage**

**TODO:** Add information about example usage

.. code-block::

   cellmaps_downloadercmd.py # TODO Add other needed arguments here


Via Docker
~~~~~~~~~~~~~~~~~~~~~~

**Example usage**

**TODO:** Add information about example usage


.. code-block::

   docker run -v `pwd`:`pwd` -w `pwd` idekerlab/cellmaps_downloader:0.1.0 cellmaps_downloadercmd.py # TODO Add other needed arguments here


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _NDEx: http://www.ndexbio.org
