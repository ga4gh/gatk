package org.broadinstitute.hellbender.engine;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SamReaderFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class ReadsHtsgetDataSource implements ReadsDataSource {
    private static final Logger logger = LogManager.getLogger(ReadsPathDataSource.class);

    private final ReadsPathDataSource inner;

    public ReadsHtsgetDataSource(final URI source) {
        this(source, (SamReaderFactory) null);
    }

    public ReadsHtsgetDataSource(final URI source, final SamReaderFactory customSamReaderFactory) {
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
