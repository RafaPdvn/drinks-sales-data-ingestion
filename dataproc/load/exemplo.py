from coe_dataproc.monitoria import Monitoria
from coe_dataproc.decorator import dataproc
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import sys

_ENV = sys.argv[5]
_PROJETO = sys.argv[6]
_WORKFLOW = sys.argv[7]

monitoria = Monitoria(
    project = _PROJETO,
    workflow_id = _WORKFLOW.split('-')[0],
    workflow_name = _WORKFLOW,
    step = '', # TODO preencher o nome do step
    env = _ENV
)

@dataproc(monitoria)
def run(data: datetime = None):
    spark = (
        SparkSession.builder
        .appName(f'{monitoria.workflow_id}_{monitoria.step}')
        .getOrCreate()
    )
 # TODO implementar


if __name__ == '__main__':
    run()