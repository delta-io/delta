##
## This script runs the Delta Kernel example programs using the golden
## tables located in the <repo-root>/connectors/golden-tables/src/main/resources/golden
## directory.
##
## Make sure to run this script from <repo-root> in order for the relative
## paths used for referring to the golden tables work.

BASEDIR=`pwd`
echo $BASEDIR
GOLDEN_TABLE_DIR="${BASEDIR}/connectors//golden-tables/src/main/resources/golden/"

cd kernel/examples/table-reader

SINGLE_THREAD_READER="io.delta.kernel.examples.SingleThreadedTableReader"
MULTI_THREADED_READER="io.delta.kernel.examples.MultiThreadedTableReader"

declare -a tests_single_threaded=(
  "--table=${GOLDEN_TABLE_DIR}/data-reader-primitives --columns=as_int,as_long --limit=5"
  "--table=${GOLDEN_TABLE_DIR}/data-reader-primitives --columns=as_int,as_long,as_double,as_string --limit=20"
  "--table=${GOLDEN_TABLE_DIR}/data-reader-partition-values --columns=as_string,as_byte,as_list_of_records,as_nested_struct --limit=20"
)

for test in "${tests_single_threaded[@]}"
do 
  mvn package exec:java \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.mainClass=${SINGLE_THREAD_READER} \
    -Dstaging.repo.url=${EXTRA_MAVEN_REPO:-"___"} \
    -Ddelta-kernel.version=${STANDALONE_VERSION:-"3.0.0-SNAPSHOT"} \
    -Dexec.args="${test}"
done

declare -a tests_multi_threaded=(
  "--table=${GOLDEN_TABLE_DIR}/data-reader-primitives --columns=as_int,as_long --limit=5 --parallelism=5"
  "--table=${GOLDEN_TABLE_DIR}/data-reader-primitives --columns=as_int,as_long,as_double,as_string --limit=20 --parallelism=20"
  "--table=${GOLDEN_TABLE_DIR}/data-reader-partition-values --columns=as_string,as_byte,as_list_of_records,as_nested_struct --limit=20 --parallelism=2"
)

for test in "${tests_single_threaded[@]}"
do
  mvn package exec:java \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.mainClass=${MULTI_THREADED_READER} \
    -Dstaging.repo.url=${EXTRA_MAVEN_REPO:-"___"} \
    -Ddelta-kernel.version=${STANDALONE_VERSION:-"3.0.0-SNAPSHOT"} \
    -Dexec.args="${test}"
done

