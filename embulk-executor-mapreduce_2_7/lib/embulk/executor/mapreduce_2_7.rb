Embulk::JavaPlugin.register_executor(
  :mapreduce_2_7, "org.embulk.executor.mapreduce.MapReduceExecutor",
  File.expand_path('../../../../classpath', __FILE__))
