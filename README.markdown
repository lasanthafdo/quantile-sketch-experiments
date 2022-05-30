# Flink Streaming Benchmarks 

### Setup
./bin/setup.sh flink

### Generate Experiments
./bin/gen-exp.sh <configurations>
<configurations> are: 
<experiment_name> <workload_type> <num_instances> <throughput> <watermark_frequency> <window_size_in_seconds>"

### Run Experiments
./bin/run-exp.sh <exp_name>

