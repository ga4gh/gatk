package org.broadinstitute.hellbender.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.broadinstitute.barclay.argparser.Advanced;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.argparser.ExperimentalFeature;
import org.broadinstitute.hellbender.cmdline.CommandLineProgram;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.ExampleProgramGroup;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.htsgetreader.*;
import org.broadinstitute.hellbender.utils.HttpUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;

/**
 * A tool that downloads a file hosted on an htsget server to a local file
 * 
 * <h3>Usage example</h3>
 * <pre>
 * gatk HtsgetReader \
 *   --url htsget-server.org \
 *   --id A1.bam \
 *   --reference-name chr1
 *   -O output.bam
 * </pre>
 */

@ExperimentalFeature
@CommandLineProgramProperties(
        summary = "Download a file using htsget",
        oneLineSummary = "Download a file using htsget",
        programGroup = ExampleProgramGroup.class
)
public class HtsgetReader extends CommandLineProgram {

    public static final String URL_LONG_NAME = "url";
    public static final String ID_LONG_NAME = "id";
    public static final String FORMAT_LONG_NAME = "format";
    public static final String CLASS_LONG_NAME = "class";
    public static final String FIELDS_LONG_NAME = "field";
    public static final String TAGS_LONG_NAME = "tag";
    public static final String NOTAGS_LONG_NAME = "notag";
    public static final String NUM_THREADS_LONG_NAME = "reader-threads";
    public static final String CHECK_MD5_LONG_NAME = "check-md5";

    @Argument(doc = "Output file.",
        fullName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
        shortName = StandardArgumentDefinitions.OUTPUT_LONG_NAME)
    private File outputFile;

    @Argument(doc = "URL of htsget endpoint.",
        fullName = URL_LONG_NAME,
        shortName = URL_LONG_NAME)
    private URI endpoint;

    @Argument(doc = "ID of record to request.",
        fullName = ID_LONG_NAME,
        shortName = ID_LONG_NAME)
    private String id;

    @Argument(doc = "Format to request record data in.",
        fullName = FORMAT_LONG_NAME,
        shortName = FORMAT_LONG_NAME,
        optional = true)
    private HtsgetFormat format;

    @Argument(doc = "Class of data to request.",
        fullName = CLASS_LONG_NAME,
        shortName = CLASS_LONG_NAME,
        optional = true)
    private HtsgetClass dataClass;

    @Argument(doc = "The interval and reference sequence to request",
        fullName = StandardArgumentDefinitions.INTERVALS_LONG_NAME,
        shortName = StandardArgumentDefinitions.INTERVALS_SHORT_NAME,
        optional = true)
    private SimpleInterval interval;

    @Argument(doc = "A field to include, default: all",
        fullName = FIELDS_LONG_NAME,
        shortName = FIELDS_LONG_NAME,
        optional = true)
    private List<HtsgetRequestField> fields;

    @Argument(doc = "A tag which should be included.",
        fullName = TAGS_LONG_NAME,
        shortName = TAGS_LONG_NAME,
        optional = true)
    private List<String> tags;

    @Argument(doc = "A tag which should be excluded.",
        fullName = NOTAGS_LONG_NAME,
        shortName = NOTAGS_LONG_NAME,
        optional = true)
    private List<String> notags;

    @Advanced
    @Argument(fullName = NUM_THREADS_LONG_NAME,
        shortName = NUM_THREADS_LONG_NAME,
        doc = "How many simultaneous threads to use when reading data from an htsget response;" +
            "higher values may improve performance when network latency is an issue.",
        optional = true,
        minValue = 1)
    private int readerThreads = 1;

    @Argument(fullName = CHECK_MD5_LONG_NAME, shortName = CHECK_MD5_LONG_NAME, doc = "Boolean determining whether to calculate the md5 digest of the assembled file "
        + "and validate it against the provided md5 hash, if it exists.", optional = true)
    private boolean checkMd5 = false;

    private ExecutorService executorService;

    private HtsgetReaderClient client;

    @Override
    public void onStartup() {
        if (this.readerThreads > 1) {
            logger.info("Initializing with " + this.readerThreads + " threads");
            final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("htsgetReader-thread-%d")
                .setDaemon(true).build();
            this.executorService = Executors.newFixedThreadPool(readerThreads, threadFactory);
        }
        this.client = new HtsgetReaderClient(this.executorService);
    }

    @Override
    public void onShutdown() {
        if (this.executorService != null) {
            this.executorService.shutdownNow();
        }
        super.onShutdown();
    }

    @Override
    public Object doWork() {
        // construct request from command line args and convert to URI
        final HtsgetRequestBuilder req = new HtsgetRequestBuilder(endpoint, id)
            .withFormat(format)
            .withDataClass(dataClass)
            .withInterval(interval)
            .withFields(fields)
            .withTags(tags)
            .withNotags(notags);

        try (final OutputStream ostream = new FileOutputStream(this.outputFile)) {
            IOUtils.copy(this.client.execute(req), ostream);
        } catch (final IOException e) {
            throw new UserException("Could not create output file", e);
        }
        return null;
    }
}