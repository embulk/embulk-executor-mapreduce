# Hadoop MapReduce executor plugin for Embulk

`embulk-executor-embulk` runs bulk load tasks of Embulk on Hadoop, a distributed computing environment.

This executor plugin can partition data by a column before passing the data to output plugins. This enables you to partition files by day, hour or other factors before loading them to a storage.

## Configuration

- **config_files** list of paths to Hadoop's configuration files (array of strings, default: `[]`)
- **config** overwrites configuration parameters (hash, default: `{}`)
- **job_name** name of the job (string, default: `"embulk"`)
- **reducers** number of reduce tasks. This parameter is used only when `partitioning` parameter is set (integer, default: same number with input tasks)
- **libjars** additional jar files to run the MapReduce application (array of strings, default: `[]`)
- **state_path** a directory path on the default filesystem (usually HDFS) to store temporary progress report files (string, default: `"/tmp/embulk"`)
- **partitioning** partitioning strategy. see below (hash, default: no partitioning)
    - **type: timestamp** only `timestamp` is supported for partitioning type for now.
    - **column** name of a timestamp or long column used for partitioning. (string, required)
    - **unit** "hour" or "day" (enum, required)
    - **timezone: UTC** only "UTC" is supported for now.
    - **unix_timestamp_unit** unit of the unix timestamp if type of the column is long. "sec", "milli" (for milliseconds), "micro" (for micorseconds), or "nano" (for nanoseconds). (enum, default: `"sec"`)


### Partitioning

You can optionally set `partitioning` parameter to partition records before writing them to output.

If you set `partitioning` parameter, input and filter plugins run on the mappers and output runs on the reducer.
if you don't set `partitioning` parameter, all of input, filter and output plugins run on the mappers. Reducers do nothing.

## Hadoop versions

It is built for Hadoop 2.6.0 but also works with Hadoop 2.4.0. YARN is requried.


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
        - fs.defaultFS: "hdfs://my-hdfs.example.net:8020"
        - yarn.resourcemanager.hostname: "my-yarn.example.net"
        - dfs.replication: 1
        - mapreduce.client.submit.file.replication: 1
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
        - fs.defaultFS: "hdfs://my-hdfs.example.net:8020"
        - yarn.resourcemanager.hostname: "my-yarn.example.net"
        - dfs.replication: 1
        - mapreduce.client.submit.file.replication: 1
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

