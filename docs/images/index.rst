Airflow for Remindo ETL
==============================

.. toctree::
   :hidden:
   :maxdepth: 1

   reference


Project description


Installation
------------

Guide to get a local copy of this project up and running.

Prerequisites
^^^^^^^^^^^^^^^

To install and run this project you need to have the following
prerequisites installed.

* Airflow
* PostgreSQL

If you want to run the ETL jobs using Spark, you will also
need to set up the following:

* Hadoop
* Spark

Airflow setup
^^^^^^^^^^^^^^^

Detailed instruction on how to setup Airflow are available
`here<https://towardsdatascience.com/an-apache-airflow-mvp-complete-guide-for-a-basic-production-installation-using-localexecutor-beb10e4886b2>`_
and `here`<https://levelup.gitconnected.com/deploying-scalable-production-ready-airflow-in-10-easy-steps-using-kubernetes-4f449d01f47a>`_
if a containarized solution is preferred.

In both recommendations, you need to modify the following when installing Airflow:

.. code-block:: console
    # from
    $ pip install apache-airflow['postgresql']
    # to
    $ pip install 'apache-airflow[crypto, password, oracle]'

* Use a PostgreSQL database to manage Airflow's dags.

Finally, copy the dag and plugin [folder](dags) inside the Airflow home directory.

Hadoop setup
^^^^^^^^^^^^^^^

On `MacOS`:

* Install `brew` if you do not already have it
  * `/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`
* Install Java (1.8)
  * `brew cask install homebrew/cask-versions/adoptopenjdk8`
* Install `Hadoop` using `brew`
  + `brew install hadoop`
* **Configure** Hadoop following this `guide from 
TowardsDataScience<https://towardsdatascience.com/installing-hadoop-on-a-mac-ec01c67b003c>`_

Set up Spark
^^^^^^^^^^^^^^^

* From terminal, install `Pyspark`
  * `pip install Pyspark`
* Setup environment variables. From terminal navigate to
your `./zshrc` or `./bashrc` or `./bash_profile` depending on your OS.
Insert the following:

.. code-block:: console
    $ export JAVA_HOME=<'path-to-apache-spark-bin'>
    $ export JRE_HOME=<'path-to-apache-spark-bin'>
    $ export SPARK_HOME=<'path-to-apache-spark-bin'>
    $ export PATH=<'path-to-apache-spark-bin'>
    $ export PYSPARK_DRIVER_PYTHON=python
    $ export PYSPARK_PYTHON=python

Installation
^^^^^^^^^^^^^^^

To run the project, ensure to install the project's dependencies.

.. code-block:: console
    $ pip install -r requirements.txt

Start PostgreSQL
^^^^^^^^^^^^^^^^^

Start the `PostgreSQL` server on which Airflow depends to run and schedule the jobs.

If you used `brew` to install PostgreSQL:

.. code-block:: console
    $ brew services start postgres

else:

.. code-block:: console
    $ services start postgres


Start Airflow
^^^^^^^^^^^^^^^^^

Start Airflow `scheduler`, `webserver` (and `worker` if
you set it up) with the following commands on your Terminal.

.. code-block:: console
  $ airflow start scheduler
  $ airflow start webserver


Stopping services
^^^^^^^^^^^^^^^^^

.. code-block:: console
    $ airflow stop scheduler
    $ airflow stop webserver
    $ #airflow stop worker
  
    $ # Now for the Server
    $ services stop postgres
    $ # OR
    $ brew services stop postgres


Usage
------

This section will describe how to run the project.

Hypermodern Python's usage looks like:

.. code-block:: console

   $ hypermodern-python [OPTIONS]

.. option:: -l <language>, --language <language>

   The Wikipedia language edition,
   as identified by its subdomain on
   `wikipedia.org <https://www.wikipedia.org/>`_.
   By default, the English Wikipedia is selected.

.. option:: --version

   Display the version and exit.

.. option:: --help

   Display a short usage message and exit.