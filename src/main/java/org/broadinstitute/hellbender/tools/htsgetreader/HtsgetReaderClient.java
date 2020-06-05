package org.broadinstitute.hellbender.tools.htsgetreader;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.Charsets;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.HttpUtils;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class HtsgetReaderClient {
    private static final Logger logger = LogManager.getLogger(HtsgetReaderClient.class);

    private ExecutorService executorService;

    private final CloseableHttpClient client = HttpUtils.getClient();

    public HtsgetReaderClient() {
        executorService = null;
    }

    public HtsgetReaderClient(final ExecutorService exec) {
        executorService = exec;
    }

    /**
     * Initiates download of blocks provided by response in serial
     *
     * @param response Response received from htsget server
     * @return InputStream over concatenation of all blocks received in response
     */
    private InputStream getData(final HtsgetResponse response) {
        return new SequenceInputStream(Collections.enumeration(
            response.getBlocks().stream()
                .map(HtsgetResponse.Block::getData)
                .collect(Collectors.toList())));
    }

    /**
     * Initiates download of blocks provided by response in parallel
     *
     * @param response Response received from htsget server
     * @return InputStream over concatenation of all blocks received in response
     */
    private InputStream getDataParallel(final HtsgetResponse response) {
        final List<Future<InputStream>> futures = new ArrayList<>(response.getBlocks().size());
        response.getBlocks().forEach(b -> futures.add(this.executorService.submit(b::getData)));

        final List<InputStream> streams = futures.stream()
            .map(f -> {
                try {
                    return f.get();
                } catch (final ExecutionException | InterruptedException e) {
                    throw new UserException("Error while waiting to download block", e);
                }
            })
            .collect(Collectors.toList());
        return new SequenceInputStream(Collections.enumeration(streams));
    }

    private ObjectMapper getObjectMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.enable(DeserializationFeature.UNWRAP_ROOT_VALUE);
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        return mapper;
    }

    /**
     * Execute an htsget request and return stream over concatenated contents
     * @param req htsget request to execute
     * @return InputStream over contents of request, if it can be retrieved
     */
    public InputStream execute(final HtsgetRequestBuilder req) {
        final URI reqURI = req.toURI();

        final HttpGet getReq = new HttpGet(reqURI);
        try (final CloseableHttpResponse resp = this.client.execute(getReq)) {
            // get content of response
            final HttpEntity entity = resp.getEntity();
            final Header encodingHeader = entity.getContentEncoding();
            final Charset encoding = encodingHeader == null
                ? StandardCharsets.UTF_8
                : Charsets.toCharset(encodingHeader.getValue());
            final String jsonBody = EntityUtils.toString(entity, encoding);

            final ObjectMapper mapper = this.getObjectMapper();

            if (resp.getStatusLine() == null) {
                throw new UserException("htsget server response did not contain status line");
            }
            final int statusCode = resp.getStatusLine().getStatusCode();
            if (400 <= statusCode && statusCode < 500) {
                final HtsgetErrorResponse err = mapper.readValue(jsonBody, HtsgetErrorResponse.class);
                throw new UserException("Invalid request, received error code: " + statusCode +
                    ", error type: " + err.getError() +
                    ", message: " + err.getMessage());
            } else if (statusCode == 200) {
                final HtsgetResponse response = mapper.readValue(jsonBody, HtsgetResponse.class);

                logger.info(response.getMd5() == null
                    ? "No md5 checksum received"
                    : "Received md5 checksum: " + response.getMd5());

                return this.executorService == null
                    ? this.getData(response)
                    : this.getDataParallel(response);
            } else {
                throw new UserException("Unrecognized status code: " + statusCode);
            }
        } catch (final IOException e) {
            throw new UserException("IOException during htsget download", e);
        }
    }
}
