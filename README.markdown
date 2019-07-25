# Flink Streaming Benchmarks 

### Setup
./bin/setup.sh

### Generate Experiments
./bin/gen-exp.sh <configurations> (Leave it empty)

### Run Experiments
./bin/run-exp.sh <exp_name>


### Scheduling Algorithms

1 -> round robin

2 -> longest queue first

3 -> shortest remaining processing time

4 -> rate based

5 -> shortest emitting watermark processing
