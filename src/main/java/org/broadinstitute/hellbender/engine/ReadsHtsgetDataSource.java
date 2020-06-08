package org.broadinstitute.hellbender.engine;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SamReaderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.htsgetreader.HtsgetReaderClient;
import org.broadinstitute.hellbender.tools.htsgetreader.HtsgetRequestBuilder;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ReadsHtsgetDataSource implements ReadsDataSource {
    private static final Logger logger = LogManager.getLogger(ReadsPathDataSource.class);

    private final ReadsPathDataSource inner;

    private final HtsgetReaderClient client = new HtsgetReaderClient();

    public ReadsHtsgetDataSource(final GATKPathSpecifier source) {
        this(source, null);
    }

    public ReadsHtsgetDataSource(final GATKPathSpecifier source, final SamReaderFactory customSamReaderFactory) {
        this(Collections.singletonList(source), customSamReaderFactory);
    }

    public ReadsHtsgetDataSource(final List<GATKPathSpecifier> sources) {
        this(sources, null);
    }

    public ReadsHtsgetDataSource(final List<GATKPathSpecifier> sources, final SamReaderFactory customSamReaderFactory) {
        final List<Path> sourcesOnDisk = sources.stream()
            .map(s -> {
                try {
                    final URI sourceURI = s.getURI();
                    final HtsgetRequestBuilder req = new HtsgetRequestBuilder(new URI("//" + sourceURI.getHost()), sourceURI.getPath());
                    final Path outputPath = IOUtils.createTempPath("htsget-temp", s.getExtension().orElse(""));
                    try (final OutputStream ostream = Files.newOutputStream(outputPath)) {
                        org.apache.commons.io.IOUtils.copy(this.client.execute(req), ostream);
                    } catch (final IOException e) {
                        throw new UserException("Could not create output file", e);
                    }
                    return outputPath;
                } catch (final URISyntaxException e) {
                    throw new UserException("Endpoint of htsget request cannot be parsed as URI", e);
                }
            })
            .collect(Collectors.toList());
        inner = new ReadsPathDataSource(sourcesOnDisk, customSamReaderFactory);
    }

    public void setTraversalBounds(final List<SimpleInterval> intervals, final boolean traverseUnmapped) {
        inner.setTraversalBounds(intervals, traverseUnmapped);
    }

    public boolean traversalIsBounded() {
        return inner.traversalIsBounded();
    }

    public boolean isQueryableByInterval() {
        return inner.isQueryableByInterval();
    }

    @Override
    public Iterator<GATKRead> iterator() {
        return inner.iterator();
    }

    @Override
    public Iterator<GATKRead> query(final SimpleInterval interval) {
        return inner.query(interval);
    }

    public Iterator<GATKRead> queryUnmapped() {
        return inner.queryUnmapped();
    }

    public SAMFileHeader getHeader() {
        return inner.getHeader();
    }

    public boolean supportsSerialIteration() {
        return inner.supportsSerialIteration();
    }

    public void close() {
        inner.close();
    }
}
