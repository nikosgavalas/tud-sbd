
let ALPHA=1
let NUM_PROC=$(getconf _NPROCESSORS_ONLN)
let DEFAULT_PARALLELISM=$((NUM_PROC / $ALPHA))
let SHUFFLE_PARALLELISM=$((NUM_PROC / $ALPHA))

echo "RUNNING WITH PARALLELISM: " + $DEFAULT_PARALLELISM

ORC_PATH="./data/*.orc"
PARQUET_PATH="./data/parquet/*.parquet"
SEA_LEVEL=12
DIR="./results/"


docker run -it --rm -v "`pwd`":/root sbt sbt package
docker run -it --rm -v "`pwd`":/io -v "`pwd`"/spark-events:/spark-events spark-submit --jars h3-4.0.0.jar target/scala-2.12/lab-1_2.12-1.0.jar $DEFAULT_PARALLELISM $SHUFFLE_PARALLELISM "$ORC_PATH" "$PARQUET_PATH" $SEA_LEVEL "$DIR"

