import socket
import os

def _patch_hostname():

    hostname = socket.gethostname()
    entry = f"127.0.0.1\t{hostname}\n"
    try:
        with open("/etc/hosts", "r") as f:
            if hostname in f.read():
                return # already patched
        with open("/etc/hosts", "a") as f:
            f.write(entry)
    except PermissionError:
    # Scoring system may mount /etc read-only — fall back to env vars only
        pass

# Call BEFORE pyspark imports
#_patch_hostname()

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

# Tell the JVM to skip hostname lookup entirely and use loopback
os.environ["JAVA_TOOL_OPTIONS"] = (
    "-Djava.net.preferIPv4Stack=true "
    "-Djava.rmi.server.hostname=127.0.0.1"
    "-Djava.io.tmpdir=/tmp"
)
os.environ["SPARK_LOCAL_DIRS"] = "/tmp/spark"


from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session():
    return(
        SparkSession.builder
        .appName("nedbank-de-pipeline")
        .master("local[2]")\
        .config("spark.local.dir", "/tmp/spark")
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "512m") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.jars.packages", "")\
        .config("spark.jars.repositories", "")\
        .config("spark.sql.parquet.compression.codec", "uncompressed")\
        .config("spark.io.compression.codec", "lz4")\
        .config("spark.hadoop.dfs.checksum.type", "NULL")
        .config("spark.hadoop.dfs.client.write.checksum", "false")
        .getOrCreate()
    )

