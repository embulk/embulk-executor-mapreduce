Embulk::JavaPlugin.register_executor(
  :mapreduce, "org.embulk.executor.mapreduce.MapReduceExecutor",
  File.expand_path('../../../../classpath', __FILE__))
