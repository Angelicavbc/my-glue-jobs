import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Inicialización
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Leer datos desde S3
input_path = "s3://test-version-control-glue/versionControl.csv"  # Reemplaza con el bucket y carpeta que ya tengas
df = spark.read.format("csv").option("header", "true").load(input_path) #Funciono el control version de github 

# Imprimir los datos en el log
df.show()

# Finalizar el job
job.commit()
