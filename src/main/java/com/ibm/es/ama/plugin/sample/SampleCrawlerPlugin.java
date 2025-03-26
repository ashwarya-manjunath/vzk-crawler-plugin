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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Logger;
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
        // if (document.getCrawlUrl().contains("confidential")) {
        //     document.setExclude(true);
        //     document.getNoticeMessages().add(String.format("The document %s is excluded by the crawler plugin.", crawlUrl));
        //     return;
        // }

        // Get the fields of this crawled document
        Map<String, Object> fields = document.getFields();

        // Process file name field
        // Object fileNameField = fields.get("_$FileName$_");
        // if (fileNameField != null) {
        //     fields.put("_$FileName$_", fileNameField.toString().replace("ibm", "IBM"));
        // }

        // Add or update the current_date field
        // fields.put("current_date", new Date());

        // Remove file size field if present
        // fields.remove("_$FileSize$_");

        // Process authors field
        // Object authorField = fields.get("_$Authors$_");
        // if (authorField != null) {
        //     List<String> authorList = authorField instanceof List<?> 
        //         ? (List<String>) authorField 
        //         : Arrays.asList((String) authorField);

        //     authorList = authorList.stream()
        //         .map(String::toLowerCase)
        //         .filter(author -> !author.equals("tom@ibm.com"))
        //         .collect(Collectors.toList());

        //     fields.put("_$Authors$_", authorList);
        // }

        // Process UpdatedDate field and create formatDate field
        Object updatedDateField = fields.get("updateddate");
        if (updatedDateField != null) {
            String updatedDate = updatedDateField.toString();
            String formatDate;

            if (!updatedDate.contains(".")) {
                formatDate = updatedDate.split(" ")[0]; // Get part before space
            } else {
                formatDate = updatedDate.split(".")[0]; // Get part before comma
            }

            // Add formatDate field while keeping original UpdatedDate
            fields.put("formatDate", formatDate);
        }

        // Retrieve the "formatDate" field
    Object formatDateField = fields.get("formatDate");

    if (formatDateField != null) {
        String formatDateStr = formatDateField.toString(); // Keep original value

        // Store original field
        fields.put("formatDate", formatDateStr);

        // Convert "formatDate" to epoch timestamp
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setLenient(false);
        try {
            Date parsedDate = dateFormat.parse(formatDateStr);
            long epochTime = parsedDate.getTime() / 1000; // Convert to seconds
            
            // Store epoch timestamp in "updateddatesec"
            fields.put("updateddatesec", epochTime);
            
            logger.info("Converted formatDate to epoch: " + epochTime);
        } catch (ParseException e) {
            logger.warning("Failed to parse formatDate: " + formatDateStr);
        }
    }

      
        
         // Code Logic to copy "effDate" to "effDateSec"
        Object formatEffDateField = fields.get("effDate");
        if (formatDateField != null) {
         fields.put("effDateSec", formatEffDateField); // Simply copy the value
        }

      // Retrieve "updateddatesec" field
    Object updatedDateSecField = fields.get("updateddatesec");

    if (updatedDateSecField != null) {
        try {
            // Convert updateddatesec from String/Number to long
            long epochTime = Long.parseLong(updatedDateSecField.toString());

            // Convert epoch time to MM-dd-yyyy format
            SimpleDateFormat outputFormat = new SimpleDateFormat("MM-dd-yyyy");
            outputFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Ensure consistency in time zone
            String formattedDate = outputFormat.format(new Date(epochTime * 1000)); // Convert to milliseconds

            // Store in lastupdateddate while retaining updateddatesec
            fields.put("lastupdateddate", formattedDate);
            logger.info("Converted updateddatesec: " + epochTime + " to lastupdateddate: " + formattedDate);

        } catch (NumberFormatException e) {
            logger.warning("Invalid epoch format for updateddatesec: " + updatedDateSecField);
        }
    }
    
       // Retrieve "effDateSec" field
    Object effDateSecField = fields.get("effDateSec");

    if (effDateSecField != null) {
        try {
            // Convert effDateSec from String/Number to long
            long epochTime = Long.parseLong(effDateSecField.toString());

            // Convert epoch time to dd/MM/yyyy format
            SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MM/yyyy");
            outputFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Ensure consistency in time zone
            String formattedDate = outputFormat.format(new Date(epochTime * 1000)); // Convert to milliseconds

            // Store in effectivedatefilter while retaining effDateSec
            fields.put("effectivedatefilter", formattedDate);
            logger.info("Converted effDateSec: " + epochTime + " to effectivedatefilter: " + formattedDate);

        } catch (NumberFormatException e) {
            logger.warning("Invalid epoch format for effDateSec: " + effDateSecField);
        }
    }
     //document-url field
    Object urlField = fields.get("url");
    Object contentField = fields.get("content"); // Assuming <content> is stored as "content"

    if (urlField != null && contentField != null) {
        String url = urlField.toString();
        String documentUrl = null;

        // Define the patterns to match
        String[] urlPatterns = {
            ".*ewdtpaovp11.*",
            ".*ewdtpaovp01.*",
            ".*ewdsacovn58.*",
            ".*Ewdfdcovp11.*",
            ".*ewdfdcovp11.*"
        };

        // Check if the URL matches any of the predefined patterns
        for (String pattern : urlPatterns) {
            if (Pattern.matches(pattern, url)) {
                documentUrl = url; // If matched, store the URL in document-url

                // Copy the entire content node structure
                fields.put("document-content", contentField);
                break;
            }
        }

        // Add document-url field while keeping the original url field
        fields.put("document-url", documentUrl);
    }
    // Retrieve the EFFECTIVEDATE field
    Object effectiveDateField = fields.get("EFFECTIVEDATE");

    if (effectiveDateField != null) {
        String effectiveDate = effectiveDateField.toString();
        String effDate;

        if (!effectiveDate.contains(".")) {
            effDate = effectiveDate.split(" ")[0]; // Get the part before the first space
        } else {
            effDate = effectiveDate.split("\\.")[0]; // Get the part before the first period
        }

        // Store the extracted date while retaining the original EFFECTIVEDATE field
        fields.put("effDate", effDate);
    } 

   // Process and store document title
   Object docTitleField = fields.get("docTitle");
if (docTitleField != null) {
    String docTitle = docTitleField.toString();

    // Add title field while keeping original docTitle
    fields.put("title", docTitle);
}

//last-modified date
Object lastUpdatedDateField = fields.get("lastupdateddate");
if (lastUpdatedDateField != null) {
    String lastUpdatedDate = lastUpdatedDateField.toString();

    // Add last-modified field while keeping original lastupdateddate
    fields.put("last-modified", lastUpdatedDate);
}
//xxrole field
Object roleField = fields.get("role");
if (roleField != null) {
    String role = roleField.toString();

    // Add xxrole field while keeping original role
    fields.put("xxrole", role);
}

//xxbgorup field
Object businessGroupField = fields.get("businessgroup");
if (businessGroupField != null) {
    String businessGroup = businessGroupField.toString();

    // Add xxbgroup field while keeping original businessgroup
    fields.put("xxbgroup", businessGroup);
}
// Creating a new field "enqueueurl" with crawl URL options
Map<String, Object> enqueueUrl = new HashMap<>();

// Adding curl options
Map<String, String> curlOptions = new HashMap<>();
curlOptions.put("default-allow", "allow");
curlOptions.put("max-hops", "0");

// Wrapping the curl options inside a crawl-url structure
enqueueUrl.put("crawl-url", Collections.singletonMap("curl-options", curlOptions));

// Storing the enqueueUrl field while retaining existing fields
fields.put("enqueueurl", enqueueUrl);

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
