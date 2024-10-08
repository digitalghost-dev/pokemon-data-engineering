#!/bin/bash
export AIRFLOW_HOME=/Users/$USER/PycharmProjects/pokemon-data-engineering/etl/airflow
echo "AIRFLOW_HOME set to $AIRFLOW_HOME"
echo "------------"
echo " "

AIRFLOW_VERSION=2.9.2
echo "Airflow version set to $AIRFLOW_VERSION"
echo "------------"
echo " "

PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
echo "Python version set to $PYTHON_VERSION"
echo "------------"
echo " "

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
echo "Constraint URL set to $CONSTRAINT_URL"
echo "------------"
echo " "

pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
echo "------------"
echo " "

echo "Airflow installed with version $AIRFLOW_VERSION using constraints from $CONSTRAINT_URL"

pip install psycopg2-binary==2.9.9