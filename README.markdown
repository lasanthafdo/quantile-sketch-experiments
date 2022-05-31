## Using UDDSketch

The original UDDSketch algorithm [1] was written in C. We provide a Java implementation that reuses the API provided by DataDog's DDSketch [2].
In order to use this implementation, you need to build and install the jar file to the local maven repository of the machine(s) where the experiments are being run.

### Build and install the Java implementation of UDDSketch

1. Clone the fork of DataDog's sketches-java repository at https://github.com/lasanthafdo/sketches-java and checkout the branch 'uddsketch'
2. Run `./gradlew clean build publishToMavenLocal -x test`. This builds the project and installs to the local Maven repository

## Flink Streaming Benchmarks 

If you compile Flink (ie. get a new Flink binary), you must run `./bin/setup syn` for changes to be integrated with flink-benchmarks. 

### Compling Flink

1. Clone the fork of the Apache Flink repository at https://github.com/ChasonPickles/flink.git and checkout the branch 'quantile'
2. Run `./install_script`. This script compiles only flink-streaming-java and flink-dist, and copies the build-target to ~/flink-binary  
3. Run `./bin/setup.sh syn`. This will copy the flink binary from **~/flink-binary** to the **quantile-sketch-experiments** local repository directory and setup other tools like Kafka. 

### Setup
`./bin/setup.sh syn` 

### Generate Experiments
`./bin/gen-exp.sh <configurations>`

Configurations are: 
`<experiment_name> <workload_type> <num_instances> <throughput> <watermark_frequency> <window_size_in_seconds> <algorithm>`

  
You need to run an instance of *workload* AND an instance of *processor*. You can run these on seperate machines or the same one .The *workload* instance will generate the data required for the experiment and puts it into kafka and the *processor* runs the benchmark/experiment. The processor also sets up the instances of kafka that the generator feeds into.  
  
### Run Experiments
`./bin/run-exp-tembo.sh <exp_name> <workload|processing> <running_time_in_seconds>`

## Standalone Experiments

The standalone experiments are included under the standalone-benchmarks directory of this repository. 
You can adjust the parameters of the StreamingQuantileBenchmarks class, build and run any of the experiments if the sketches-java repository has been already built and installed.

### Run experiments

`java -jar standalone-benchmarks/target/standalone-benchmarks-0.5.0.jar <run_mode_id>`

Possible run modes IDs are integers corresponding to test scenarios as follows: 

1. Query tests
2. Insertion tests
3. Merge tests
4. Adaptability tests
5. Kurtosis tests

### References

[1] Italo Epicoco, Catiuscia Melle, Massimo Cafaro, Marco Pulimeno, and Giuseppe Morleo. 2020. UDDSketch: Accurate Tracking of Quantiles in Data Streams. IEEE
Access 8 (2020), 147604–147617. https://doi.org/10.1109/ACCESS.2020.3015599

[2] Charles Masson, Jee E. Rim, and Homin K. Lee. 2019. DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees. Proc. VLDB
Endow. 12, 12 (2019), 2195–2205. https://doi.org/10.14778/3352063.3352135