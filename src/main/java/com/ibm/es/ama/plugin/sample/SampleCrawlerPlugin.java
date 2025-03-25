package com.ibm.es.ama.plugin.sample;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.ibm.es.ama.plugin.CrawlerPlugin;
import com.ibm.es.ama.plugin.CrawlerPluginConfiguration;
import com.ibm.es.ama.plugin.CrawlerPluginContent;
import com.ibm.es.ama.plugin.CrawlerPluginDocument;
import com.ibm.es.ama.plugin.CrawlerPluginException;

public class SampleCrawlerPlugin implements CrawlerPlugin {

    // use java.util.logging.Logger instance to output log messages
    private Logger logger = Logger.getLogger(SampleCrawlerPlugin.class.getName());

    private String crawlerName;

    @Override
    public void init(CrawlerPluginConfiguration configuration) throws CrawlerPluginException {
        Map<String, Object> generalSettings = configuration.getGeneralSettings();
        this.crawlerName = (String) generalSettings.get("crawler_name");
    }

    @Override
    public void updateDocument(CrawlerPluginDocument document) throws CrawlerPluginException {
        // Get the unique document ID
        String crawlUrl = document.getCrawlUrl();
        logger.info(String.format("Processing document: %s", crawlUrl));

        // Exclude confidential documents
        if (document.getCrawlUrl().contains("confidential")) {
            document.setExclude(true);
            document.getNoticeMessages().add(String.format("The document %s is excluded by the crawler plugin.", crawlUrl));
            return;
        }

        // Get the fields of this crawled document
        Map<String, Object> fields = document.getFields();

        // Process file name field
        Object fileNameField = fields.get("_$FileName$_");
        if (fileNameField != null) {
            fields.put("_$FileName$_", fileNameField.toString().replace("ibm", "IBM"));
        }

        // Add or update the current_date field
        fields.put("current_date", new Date());

        // Remove file size field if present
        fields.remove("_$FileSize$_");

        // Process authors field
        Object authorField = fields.get("_$Authors$_");
        if (authorField != null) {
            List<String> authorList = authorField instanceof List<?> 
                ? (List<String>) authorField 
                : Arrays.asList((String) authorField);

            authorList = authorList.stream()
                .map(String::toLowerCase)
                .filter(author -> !author.equals("tom@ibm.com"))
                .collect(Collectors.toList());

            fields.put("_$Authors$_", authorList);
        }

        // Process UpdatedDate field and create formatDate field
        Object updatedDateField = fields.get("updateddate");
        if (updatedDateField != null) {
            String updatedDate = updatedDateField.toString();
            String formatDate;

            if (!updatedDate.contains(",")) {
                formatDate = updatedDate.split(" ")[0]; // Get part before space
            } else {
                formatDate = updatedDate.split(",")[0]; // Get part before comma
            }

            // Add formatDate field while keeping original UpdatedDate
            fields.put("formatDate", formatDate);
        }

        // Code Logic to copy "formatDate" to "updateddatesec"
        Object formatDateField = fields.get("formatDate");
        if (formatDateField != null) {
        fields.put("updateddatesec", formatDateField); // Simply copy the value
        }
        
         // Code Logic to copy "effDate" to "effDateSec"
        Object formatEffDateField = fields.get("effDate");
        if (formatDateField != null) {
         fields.put("effDateSec", formatEffDateField); // Simply copy the value
        }

         // Retrieve "updateddatesec" field
    Object updatedDateSecField = fields.get("updateddatesec");

    if (updatedDateSecField != null) {
        String originalDateStr = updatedDateSecField.toString(); // Keep original format
        fields.put("updateddatesec", originalDateStr); // Retain original value

        // List of possible date formats to parse
        String[] possibleFormats = {
            "yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy", "yyyy/MM/dd",
            "MMM dd, yyyy", "dd-MMM-yyyy", "EEE, dd MMM yyyy HH:mm:ss z"
        };

        Date parsedDate = null;

        // Try parsing with different formats
        for (String format : possibleFormats) {
            try {
                SimpleDateFormat inputFormat = new SimpleDateFormat(format);
                inputFormat.setLenient(false);
                parsedDate = inputFormat.parse(originalDateStr);
                break; // Stop when parsing succeeds
            } catch (ParseException ignored) {
                // Try the next format
            }
        }

        // If parsed successfully, format it to "MM/dd/yyyy"
        if (parsedDate != null) {
            SimpleDateFormat outputFormat = new SimpleDateFormat("MM/dd/yyyy");
            fields.put("lastupdateddate", outputFormat.format(parsedDate));
        } else {
            // Log or handle the error when parsing fails
            logger.warning("Failed to parse date: " + originalDateStr);
        }
    }
    
          // Retrieve "effDateSec" field
        Object effDateSecField = fields.get("effDateSec");

        if (effDateSecField != null) {
              String originalEffDateStr = effDateSecField.toString(); // Keep original format
            fields.put("effDateSec", originalEffDateStr); // Retain original value
              // List of possible date formats to parse
            String[] possibleFormats1 = {"yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy", "yyyy/MM/dd",   "MMM dd, yyyy", "dd-MMM-yyyy", "EEE, dd MMM yyyy HH:mm:ss z" };
            Date parsedDate1 = null;
              // Try parsing with different formats
            for (String format : possibleFormats1) { try {
                    SimpleDateFormat inputFormat = new SimpleDateFormat(format);
                    inputFormat.setLenient(false);
                    parsedDate1 = inputFormat.parse(originalEffDateStr);
                    break; // Stop when parsing succeeds
                } catch (ParseException ignored) {
                      // Try the next format
            }
              // If parsed successfully, format it to "MM/dd/yyyy"
                if (parsedDate1 != null) {
                    SimpleDateFormat outputFormat = new SimpleDateFormat("MM/dd/yyyy");
                    fields.put("effectivedatefilter", outputFormat.format(parsedDate1));
                    } else {
                  // Log or handle the error when parsing fails
                    // logger.warning("Failed to parse date: " + originalEffDateStr); 
                    }  }
  
    // Extract EFFECTIVEDATE from the content field
    Object effectiveDateField = fields.get("EFFECTIVEDATE");
    String effectiveDate = (effectiveDateField != null) ? effectiveDateField.toString() : "";

    // Process the EFFECTIVEDATE similar to XSL logic
    String effDate;

    if (!effectiveDate.contains(".")) {
        // If there is NO dot (.), take the part before the first space
        int spaceIndex = effectiveDate.indexOf(" ");
        effDate = (spaceIndex != -1) ? effectiveDate.substring(0, spaceIndex) : effectiveDate;
    } else {
        // If there IS a dot (.), take the part before the first dot
        int dotIndex = effectiveDate.indexOf(".");
        effDate = effectiveDate.substring(0, dotIndex);
    }

    // Store the processed value in the document fields
    fields.put("effDate", effDate);
    logger.info("Processed Effective Date: " + effDate);

     //

    // Store "crawl-url"
    if (crawlUrl != null && !crawlUrl.isEmpty()) {
    // Create a new field "document-url" with the same value as "crawl-url"
    fields.put("document-url", crawlUrl);
    }

    // Copy "document-url" to "vse-key"
    Object documentUrlField = fields.get("document-url");
    if (documentUrlField != null) {
    fields.put("vse-key", documentUrlField);
    }

    // Set "vse-key-normalized" to "vse-key-normalized" (static value)
    fields.put("vse-key-normalized", "vse-key-normalized");

   // Ensure "content" is retained in the updated document structure
    Object contentField = fields.get("content");
    if (contentField != null) {
    fields.put("content", contentField); // Retain original content
    }



   // Process and store document title
    Object docTitle = fields.get("docTitle"); // Assuming docTitle is already available
    if (docTitle != null) {
     fields.put("title", docTitle.toString()); // Store title
     fields.put("title_weight", 4); // Store weight as metadata
    logger.info("Document Title Processed: " + docTitle.toString());
    }

        // Process document content
        CrawlerPluginContent content = document.getContent();
        if (isTextContent(fields, content)) {
            replaceContent(crawlUrl, content);
        } else if (isCSVContent(fields, content)) {
            document.setContent(null);
        } else if (content == null) {
            addNewContent(crawlUrl, document);
        }
    }
    }


    private boolean isTextContent(Map<String, Object> fields, CrawlerPluginContent content) {
        return isContent(fields, content, "text/plain", ".txt");
    }

    private boolean isCSVContent(Map<String, Object> fields, CrawlerPluginContent content) {
        return isContent(fields, content, "text/csv", ".csv");
    }

    private boolean isContent(Map<String, Object> fields, CrawlerPluginContent content, String contentType, String extension) {
        if (content == null) return false;
        if (content.getContentType() != null && content.getContentType().equals(contentType)) return true;

        Object extensionField = fields.get("__$Extension$__");
        if (extensionField != null && extensionField.toString().equals(extension)) return true;

        return false;
    }

    private void replaceContent(String crawlUrl, CrawlerPluginContent content) throws CrawlerPluginException {
        Charset charset = content.getCharset();
        if (charset == null) {
            charset = StandardCharsets.UTF_8;
        }

        try (
            InputStream inputStream = content.getInputStream();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(content.getOutputStream()))) {

            LineIterator lines = IOUtils.lineIterator(inputStream, charset);
            while (lines.hasNext()) {
                String line = lines.next().replaceAll("IBM", "International Business Machines");
                writer.println(line);
            }

            content.setCharset(StandardCharsets.UTF_8);

        } catch (IOException e) {
            throw new CrawlerPluginException(String.format("The document %s cannot be updated by the crawler plugin.", crawlUrl), e);
        }
    }

    private void addNewContent(String crawlUrl, CrawlerPluginDocument document) throws CrawlerPluginException {
        Map<String, Object> fields = document.getFields();
        if (!fields.containsKey("__$ContentURL$__")) return;

        String contentUrl = (String) fields.get("__$ContentURL$__");
        CrawlerPluginContent pluginContent = document.newContent();

        try (CloseableHttpClient httpclient = HttpClients.createDefault();
             CloseableHttpResponse response = httpclient.execute(new HttpGet(contentUrl))) {

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                try (InputStream inputStream = entity.getContent();
                     OutputStream outputStream = pluginContent.getOutputStream()) {

                    IOUtils.copy(inputStream, outputStream);
                }

                pluginContent.setContentType(entity.getContentType() != null ? entity.getContentType().getValue() : null);
                pluginContent.setCharset(entity.getContentEncoding() != null ? Charset.forName(entity.getContentEncoding().getValue()) : null);
            }

        } catch (IOException e) {
            throw new CrawlerPluginException(String.format("The document %s cannot be updated by the crawler plugin.", crawlUrl), e);
        }

        document.setContent(pluginContent);
    }
    
    @Override
    public void term() throws CrawlerPluginException {
        // Terminate the plugin (if any necessary cleanup is needed)
    }
}
