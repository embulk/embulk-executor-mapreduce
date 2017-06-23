package org.embulk.executor.mapreduce;

import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.util.zip.ZipInputStream;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.InvokeFailedException;

public class PluginArchive
{
    public static class GemSpec
    {
        private final String name;
        private final List<String> requirePaths;

        @JsonCreator
        public GemSpec(
                @JsonProperty("name") String name,
                @JsonProperty("requirePaths") List<String> requirePaths)
        {
            this.name = name;
            this.requirePaths = requirePaths;
        }

        @JsonProperty("name")
        public String getName()
        {
            return name;
        }

        @JsonProperty("requirePaths")
        public List<String> getRequirePaths()
        {
            return requirePaths;
        }
    }

    private static class LocalGem
            extends GemSpec
    {
        private final File localPath;

        public LocalGem(File localPath, String name, List<String> requirePaths)
        {
            super(name, requirePaths);
            this.localPath = localPath;
        }

        public File getLocalPath()
        {
            return localPath;
        }
    }

    public static class Builder
    {
        private final ImmutableList.Builder<LocalGem> localGems = ImmutableList.builder();

        @SuppressWarnings("unchecked")
        public Builder addLoadedRubyGems(ScriptingContainer jruby)
        {
            List<List<String>> tuples = (List<List<String>>) jruby.runScriptlet("Gem.loaded_specs.map {|k,v| [k, v.full_gem_path, v.require_paths].flatten }");
            for (List<String> tuple : tuples) {
                String name = tuple.remove(0);
                String fullGemPath = tuple.remove(0);
                List<String> requirePaths = ImmutableList.copyOf(tuple);
                addSpec(new File(fullGemPath), name, requirePaths);
            }
            return this;
        }

        public Builder addSpec(File localPath, String name, List<String> requirePaths)
        {
            localGems.add(new LocalGem(localPath, name, requirePaths));
            return this;
        }

        public PluginArchive build()
        {
            return new PluginArchive(localGems.build());
        }
    }

    private final List<LocalGem> localGems;

    private PluginArchive(List<LocalGem> localGems)
    {
        this.localGems = localGems;
    }

    @SuppressWarnings("unchecked")
    public void restoreLoadPathsTo(ScriptingContainer jruby)
    {
        List<String> loadPaths = (List<String>) jruby.runScriptlet("$LOAD_PATH");
        for (LocalGem localGem : localGems) {
            Path localGemPath = localGem.getLocalPath().toPath();
            for (String requirePath : localGem.getRequirePaths()) {
                loadPaths.add(localGemPath.resolve(requirePath).toString());
            }
        }
        jruby.setLoadPaths(loadPaths);
    }

    public List<GemSpec> dump(OutputStream out)
        throws IOException
    {
        ImmutableList.Builder<GemSpec> builder = ImmutableList.builder();
        try (ZipOutputStream zip = new ZipOutputStream(out)) {
            for (LocalGem localGem : localGems) {
                zipDirectory(zip, localGem.getLocalPath().toPath(), localGem.getName() + "/");
                builder.add(new GemSpec(localGem.getName(), localGem.getRequirePaths()));
            }
        }
        return builder.build();
    }

    private static void zipDirectory(ZipOutputStream zip, Path directory, String name)
        throws IOException
    {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory)) {
            for (Path path : dirStream) {
                if (Files.isDirectory(path)) {
                    zipDirectory(zip, path, name + path.getFileName() + "/");
                } else {
                    zip.putNextEntry(new ZipEntry(name + path.getFileName()));
                    try (InputStream in = Files.newInputStream(path)) {
                        ByteStreams.copy(in, zip);
                    }
                    zip.closeEntry();
                }
            }
        } catch (NoSuchFileException | NotDirectoryException ex) {
            // ignore
        }
    }

    public static PluginArchive load(File localDirectory, List<GemSpec> gemSpecs,
            InputStream in) throws IOException
    {
        try (ZipInputStream zip = new ZipInputStream(in)) {
            unzipDirectory(zip, localDirectory.toPath());
        }

        ImmutableList.Builder<LocalGem> builder = ImmutableList.builder();
        for (GemSpec gemSpec : gemSpecs) {
            builder.add(new LocalGem(
                        new File(localDirectory, gemSpec.getName()),
                        gemSpec.getName(),
                        gemSpec.getRequirePaths()));
        }
        return new PluginArchive(builder.build());
    }

    private static void unzipDirectory(ZipInputStream zip, Path directory)
        throws IOException
    {
        while (true) {
            ZipEntry entry = zip.getNextEntry();
            if (entry == null) {
                break;
            }
            Path path = directory.resolve(entry.getName());
            if (entry.getName().endsWith("/")) {
                Files.createDirectories(path);
            } else {
                Files.createDirectories(path.getParent());
                try (OutputStream out = Files.newOutputStream(path)) {
                    ByteStreams.copy(zip, out);
                }
            }
        }
    }
}
