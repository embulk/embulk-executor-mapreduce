package org.embulk.executor.mapreduce;

public class ReducerInputPlugin
        implements InputPlugin
{
    private static interface Input
    {
        public Page poll();
    }

    private final Input input;

    public ReducerInputPlugin(Input input)
    {
        this.input = input;
    }

    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        // won't be called
        throw new RuntimeException("");
    }

    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        // won't be called
        throw new RuntimeException("");
    }

    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<CommitReport> successCommitReports)
    {
        // won't be called
        throw new RuntimeException("");
    }

    public ConfigDiff guess(ConfigSource config)
    {
        // won't be called
        throw new RuntimeException("");
    }

    public CommitReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        while (true) {
            Page page = input.poll();
            if (page == null) {
                break;
            }
            output.add(page);
        }
        output.finish();
    }
}
