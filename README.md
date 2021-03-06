# Caution

**The open-sourced MapReduce Executor plugin is NO LONGER SUPPORTED since Embulk v0.9.18.**

If you want to use the open-sourced MapReduce Executor:

* Wait for Embulk v0.10 series, **and**
* Contribute to this MapReduce Executor plugin to catch up with the Embulk core changes to be done toward Embulk v0.10
* Note that **Embulk v0.10 will not support the MapReduce Executor by default unless we receive special contributions**.

See also:

* https://github.com/embulk/embulk/issues/1119
* https://groups.google.com/d/msg/embulk-announce/i26O7kT__OY/0yTFJ8SBAAAJ

----

# Hadoop MapReduce executor plugin for Embulk

`embulk-executor-embulk` runs bulk load tasks of Embulk on a [Hadoop YARN](https://hadoop.apache.org/) cluster, a distributed computing environment.

This executor plugin can partition data by a column before passing records to output plugins. This enables you to partition output files by day, hour or other factors when you load them to a storage.

## Configuration

- **config_files** list of paths to Hadoop's configuration files (array of strings, default: `[]`)
- **config** overwrites configuration parameters (hash, default: `{}`)
- **job_name** name of the job (string, default: `"embulk"`)
- **reducers** number of reduce tasks. This parameter is used only when `partitioning` parameter is set (integer, default: same number with input tasks)
- **retry_tasks** retries failed tasks automatically (boolean, default: `false`)
    - when partitioning is not used: if retry_tasks is true, map tasks are retried at most `mapreduce.map.maxattempts` times (default is 4). Reduce tasks don't run.
    - when partitioning is used: if retry_tasks is true, reduce tasks are retried at most `mapreduce.reduce.maxattempts` times (default is 4). Mapper tasks are retried regardless of this option.
- **libjars** additional jar files to run this MapReduce application (array of strings, default: `[]`)
- **state_path** path to a directory on the default filesystem (usually HDFS) to store temporary progress report files (string, default: `"/tmp/embulk"`)
- **partitioning** partitioning strategy. see below (hash, default: no partitioning)
    - **type: timestamp** only `timestamp` is supported for now. (enum, required)
    - **column** name of timestamp or long column used for partitioning. (string, required)
    - **unit** "hour" or "day" (enum, required)
    - **timezone: UTC** only "UTC" is supported for now. (string, optional)
    - **unix_timestamp_unit** unit of the unix timestamp if type of the column is long. "sec", "milli" (for milliseconds), "micro" (for micorseconds), or "nano" (for nanoseconds). (enum, default: `"sec"`)
    - **map_side_partition_split** distributes one partition into multiple reducers. This option is helpful when only a few reducers out of many reducers take long time. This splits one partition into smaller chunks. (integer, default: `1`)
- **exclude_jars**: glob pattern to exclude jar files. e.g. `[log4j-over-slf4j.jar, log4j-core-*]` (array of strings, default: `[]`)
- **local_mode_input_tasks**: executes tasks using local threads instead of distributed MapReduce if number of input tasks is equal or less than this parameter (integer, default: 0). Setting this to 1 or 2 is useful to improve performance because starting a MapReduce job can take longer time than running a small transaction. Note: `partitioning` option is local mode is enabled. This will be fixed in a future release by implementing pseudo MapReduce execution that performs partitioning using local threads.


### Partitioning

You can optionally set `partitioning` parameter to partition records before writing them to output.

If you set `partitioning` parameter, mappers run input and filter plugins, and reducers run output plugins.
If you don't set `partitioning` parameter, mappers run all of input, filter and output plugins. Reducers do nothing.

## Hadoop versions

It is built for Hadoop YARN 2.6.0 but also works with Hadoop YARN 2.4.0.


## Example

Non-partitioning example:

```yaml
exec:
    type: mapreduce
    config_files:
        - /etc/hadoop/conf/core-site.xml
        - /etc/hadoop/conf/hdfs-site.xml
        - /etc/hadoop/conf/mapred-site.xml
    config:
        fs.defaultFS: "hdfs://my-hdfs.example.net:8020"
        yarn.resourcemanager.hostname: "my-yarn.example.net"
        dfs.replication: 1
        mapreduce.client.submit.file.replication: 1
    state_path: embulk/
in:
   ...
out:
   ...
```

Partitioning example:

```yaml
exec:
    type: mapreduce
    config_files:
        - /etc/hadoop/conf/core-site.xml
        - /etc/hadoop/conf/hdfs-site.xml
        - /etc/hadoop/conf/mapred-site.xml
    config:
        fs.defaultFS: "hdfs://my-hdfs.example.net:8020"
        yarn.resourcemanager.hostname: "my-yarn.example.net"
        dfs.replication: 1
        mapreduce.client.submit.file.replication: 1
    state_path: embulk/
    partitioning:
        type: timestamp
        unit: hour
        column: updated_at
        unix_timestamp_unit: milli
    reducers: 3
in:
   ...
out:
   ...
```


## Build

This command builds the gem package to ./pkg/ directory:

```
./gradlew gem
```

## Install

```
embulk gem install embulk-executor-mapreduce
```

## Test

The unit tests run on hadoop with standalone mode.
```
./gradlew classpath test
```
