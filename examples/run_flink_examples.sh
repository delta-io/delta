# We expect env variables STANDALONE_VERSION and EXTRA_MAVEN_REPO
# e.g.
# export STANDALONE_VERSION=0.6.0
# export EXTRA_MAVEN_REPO=___
#
# We also expect the connectors repo to be cloned at ~/connectors

declare -a source_tests=(
	"org.example.source.bounded.DeltaBoundedSourceExample"
	"org.example.source.bounded.DeltaBoundedSourceUserColumnsExample"
	"org.example.source.bounded.DeltaBoundedSourceVersionAsOfExample"
	"org.example.source.continuous.DeltaContinuousSourceExample"
	"org.example.source.continuous.DeltaContinuousSourceStartingVersionExample"
	"org.example.source.continuous.DeltaContinuousSourceUserColumnsExample"
)

declare -a sink_tests=(
	"org.example.sink.DeltaSinkExample"
	"org.example.sink.DeltaSinkPartitionedTableExample"
)

echo "============= Running Delta/Flink Integration Tests ============="

echo "============= Clearing any existing maven downloads ============="
rm -rf ~/.m2/repository/io/delta/delta-standalone_2.12/$STANDALONE_VERSION
echo "Cleared delta-standalone artifacts"
rm -rf ~/.m2/repository/io/delta/delta-flink/$STANDALONE_VERSION
echo "Cleared delta-flink artifacts"
rm -rf ~/.m2/repository/org/apache/flink
echo "Cleared org.apache.flink artifacts"

echo "============= Testing Delta Source ============="
echo "============= Testing Delta Source -- Maven ============="
cd ~/connectors/examples/flink-example/

for i in "${source_tests[@]}"
do
	echo "============= Testing Delta Source -- Maven - $i ============="
    timeout 70s mvn package exec:java \
		-Dexec.cleanupDaemonThreads=false \
		-Dexec.mainClass=$i \
		-Dstaging.repo.url=$EXTRA_MAVEN_REPO \
		-Dconnectors.version=$STANDALONE_VERSION
done

echo "============= Testing Delta Source -- SBT ============="
cd ~/connectors/examples/
for i in "${source_tests[@]}"
do
	echo "============= Testing Delta Source -- SBT - $i ============="
    timeout 70s build/sbt "flinkExample/runMain $i"
done

echo "============= Testing Delta Sink ============="
echo "============= Testing Delta Sink -- Maven ============="
cd ~/connectors/examples/flink-example/

for i in "${sink_tests[@]}"
do
	echo "============= Testing Delta Sink -- Maven - $i ============="
    timeout 70s mvn package exec:java \
		-Dexec.cleanupDaemonThreads=false \
		-Dexec.mainClass=$i \
		-Dstaging.repo.url=$EXTRA_MAVEN_REPO \
		-Dconnectors.version=$STANDALONE_VERSION
done

echo "============= Testing Delta Sink -- SBT ============="
cd ~/connectors/examples/
for i in "${sink_tests[@]}"
do
	echo "============= Testing Delta Sink -- SBT - $i ============="
    timeout 70s build/sbt "flinkExample/runMain $i"
done
