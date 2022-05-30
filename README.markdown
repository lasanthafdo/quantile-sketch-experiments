# Flink Streaming Benchmarks 

If you compile flink (ie. get a new flink java file), you must run `./bin/setup syn` for changes to be integrated with flink-benchmarks. 

### Compling Flink

1. Clone the fork of the Apache Flink repository at https://github.com/ChasonPickles/flink.git and checkout the branch 'quantile'
2. Run ./install_script. This script compiles only flink-streaming-java and flink-dist, and copies the build-target to ~/flink-binary  
3. './bin/setup.sh syn' will copy the flink binary from ~/flink-binary to the flinkBenchmarks repository and setup other tools like kafka. 

### Setup
./bin/setup.sh syn 

### Generate Experiments
./bin/gen-exp.sh <configurations>
<configurations> are: 
<experiment_name> <workload_type> <num_instances> <throughput> <watermark_frequency> <window_size_in_seconds>"

  
You need to run an instance of *workload* AND an instance of *processor*. You can run these on seperate machines or the same one .The *workload* instance will generate the data required for the experiment and puts it into kafka and the *processor* runs the benchmark/experiment. The processor also sets up the instances of kafka that the generator feeds into.  
  
### Run Experiments
./bin/run-exp-tembo.sh <exp_name> <workload|processor> <running_time>
