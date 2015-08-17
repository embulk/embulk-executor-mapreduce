package org.embulk.executor.mapreduce;

import org.embulk.config.ConfigSource;
import org.embulk.EmbulkEmbed;

public class DefaultEmbulkFactory
{
    public EmbulkEmbed.Bootstrap bootstrap(ConfigSource systemConfig)
    {
        return new EmbulkEmbed.Bootstrap()
            .setSystemConfig(systemConfig);
    }
}
