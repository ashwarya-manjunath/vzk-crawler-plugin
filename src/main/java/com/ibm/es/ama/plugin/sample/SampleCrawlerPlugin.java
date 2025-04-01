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
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
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

        // "formatDate" field
        Object updatedDateField = fields.get("LASTUPDATEDDATE");

        if (updatedDateField != null) {
            try {
                logger.info("Received lastupdateddate: " + updatedDateField);

                long epochTime = Long.parseLong(updatedDateField.toString()); // Convert epoch to long
                String updatedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(epochTime)); // Convert to formatted date

                logger.info("Converted epoch to formatted date: " + updatedDate);

                String formatDate;
                if (!updatedDate.contains(".")) {
                    formatDate = updatedDate.split(" ")[0]; // Get part before space
                } else {
                    formatDate = updatedDate.split("\\.")[0]; // Get part before period
                }

                logger.info("Extracted formatDate: " + formatDate);

                // Add formatted date while keeping original updatedDate
                fields.put("FORMATDATE", formatDate);
                logger.info("Updated fields map with formatDate: " + fields);
            } catch (NumberFormatException e) {
                logger.log(Level.SEVERE, "Error parsing lastupdateddate to long: " + updatedDateField, e);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Unexpected error while processing lastupdateddate", e);
            }
        } else {
            logger.warning("No lastupdateddate found in fields map.");
        }

      // "updateddatesec" field
        Object lastUpdatedDateSecField = fields.get("LASTUPDATEDDATE");

        if (lastUpdatedDateSecField != null) {
            // Directly copy lastupdateddatesec to updateddatesec
            fields.put("UPDATEDDATESEC", lastUpdatedDateSecField);

            logger.info("Copied lastupdateddatesec: " + lastUpdatedDateSecField + " to updateddatesec.");
        } else {
            logger.warning("No lastupdateddate found in fields map.");
        }
    
       //  "effDateSec" field
        Object effectiveDateField = fields.get("EFFECTIVEDATE");

        if (effectiveDateField != null) {
            // Directly copy effectivedate to offDateSec
            fields.put("EFFDATESEC", effectiveDateField);

            logger.info("Copied effectivedate: " + effectiveDateField + " to offDateSec.");
        } else {
            logger.warning("No effectivedate found in fields map.");
        }

        // lastupdateddate Field
        Object updatedDateSecField = fields.get("UPDATEDDATESEC");

if (updatedDateSecField != null) {
    try {
        // Ensure the value is treated as a Long (epoch format)
        Long epochTime = Long.parseLong(updatedDateSecField.toString());

        // Set the lastupdateddate field with the same epoch time
        fields.put("lastupdateddate", epochTime);

        logger.info("Copied updateddatesec: " + epochTime + " to lastupdateddate.");
    } catch (NumberFormatException e) {
        logger.warning("Invalid epoch format for updateddatesec: " + updatedDateSecField);
    }
} else {
    logger.warning("No updateddatesec found in fields map.");
}



        //effectivedatefilter Field
        Object effDateSecField = fields.get("EFFDATESEC");

        if (effDateSecField != null) {
            try {
                // Convert effDateSec from String/Number to long
                long epochTime = Long.parseLong(effDateSecField.toString());

                // Convert epoch time to MM/dd/yyyy format
                SimpleDateFormat outputFormat = new SimpleDateFormat("MM/dd/yyyy");
                outputFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Ensure consistency in time zone
                String formattedDate = outputFormat.format(new Date(epochTime * 1000)); // Convert to milliseconds

                // Store in effectivedatefilter while retaining effDateSec
                fields.put("EFFECTIVEDATEFILTER", formattedDate);

                logger.info("Converted effDateSec: " + epochTime + " to effectivedatefilter: " + formattedDate);
            } catch (NumberFormatException e) {
                logger.warning("Invalid epoch format for effDateSec: " + effDateSecField);
            }
        } else {
            logger.warning("No effDateSec found in fields map.");
        }

     //document-url field
    // Object urlField = fields.get("url");
    // Object contentField = fields.get("content"); // Assuming <content> is stored as "content"

    // if (urlField != null && contentField != null) {
    //     String url = urlField.toString();
    //     String documentUrl = null;

    //     // Define the patterns to match
    //     String[] urlPatterns = {
    //         ".*ewdtpaovp11.*",
    //         ".*ewdtpaovp01.*",
    //         ".*ewdsacovn58.*",
    //         ".*Ewdfdcovp11.*",
    //         ".*ewdfdcovp11.*"
    //     };

    //     // Check if the URL matches any of the predefined patterns
    //     for (String pattern : urlPatterns) {
    //         if (Pattern.matches(pattern, url)) {
    //             documentUrl = url; // If matched, store the URL in document-url

    //             // Copy the entire content node structure
    //             fields.put("document-content", contentField);
    //             break;
    //         }
    //     }

    //     // Add document-url field while keeping the original url field
    //     fields.put("document-url", documentUrl);
    // }


    // effDate field
    Object effectiveDateValue = fields.get("EFFECTIVEDATE");

    if (effectiveDateValue != null) {
        try {
            logger.info("Received effectivedate: " + effectiveDateValue);

            long epochTime = Long.parseLong(effectiveDateValue.toString()); // Convert epoch to long
            String formattedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(epochTime)); // Convert to formatted date

            logger.info("Converted epoch to formatted date: " + formattedDate);

            String effDate;
            if (!formattedDate.contains(".")) {
                effDate = formattedDate.split(" ")[0]; // Get part before space
            } else {
                effDate = formattedDate.split("\\.")[0]; // Get part before period
            }

            logger.info("Extracted effDate: " + effDate);

            // Add formatted date while keeping original effectivedate
            fields.put("effDate", effDate);
            logger.info("Updated fields map with effDate: " + fields);
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE, "Error parsing effectivedate to long: " + effectiveDateValue, e);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Unexpected error while processing effectivedate", e);
        }
    } else {
        logger.warning("No effectivedate found in fields map.");
    }

   // title field
//    Object docTitleField = fields.get("docTitle");
// if (docTitleField != null) {
//     String docTitle = docTitleField.toString();

//     // Add title field while keeping original docTitle
//     fields.put("title", docTitle);
// }

    //last-modified date
    Object lastUpdatedDateField = fields.get("LASTUPDATEDDATE");

        if (lastUpdatedDateField != null) {
            logger.info("Copying lastupdateddate to last-modified: " + lastUpdatedDateField);

            // Copy value without modification
            fields.put("LAST-MODIFIED", lastUpdatedDateField);

            logger.info("Updated fields map with last-modified: " + fields);
        } else {
            logger.warning("No lastupdateddate found in fields map.");
        }

           // xxrole field
           Object roleField = fields.get("ROLE");
           if (roleField != null) {
               String role = roleField.toString();
               fields.put("xxrole", role);
               logger.info("Copied role to xxrole: " + role);
           } else {
               logger.warning("No role field found in fields map.");
           }
   
           // xxbgroup field
           Object businessGroupField = fields.get("BUSINESSGROUP");
           if (businessGroupField != null) {
               String businessGroup = businessGroupField.toString();
               fields.put("xxbgroup", businessGroup);
               logger.info("Copied businessgroup to xxbgroup: " + businessGroup);
           } else {
               logger.warning("No businessgroup field found in fields map.");
           }


// //Creating a new field "enqueueurl" with crawl URL options
// Map<String, Object> enqueueUrl = new HashMap<>();

// // Adding curl options
// Map<String, String> curlOptions = new HashMap<>();
// curlOptions.put("default-allow", "allow");
// curlOptions.put("max-hops", "0");

// // Wrapping the curl options inside a crawl-url structure
// enqueueUrl.put("crawl-url", Collections.singletonMap("curl-options", curlOptions));

// // Storing the enqueueUrl field while retaining existing fields
// fields.put("enqueueurl", enqueueUrl);
//     }
//        // Process document content
//         CrawlerPluginContent content = document.getContent();
//         if (isTextContent(fields, content)) {
//             replaceContent(crawlUrl, content);
//         } else if (isCSVContent(fields, content)) {
//             document.setContent(null);
//         } else if (content == null) {
//             addNewContent(crawlUrl, document);
//         }
    }
    


    // private boolean isTextContent(Map<String, Object> fields, CrawlerPluginContent content) {
    //     return isContent(fields, content, "text/plain", ".txt");
    // }

    // private boolean isCSVContent(Map<String, Object> fields, CrawlerPluginContent content) {
    //     return isContent(fields, content, "text/csv", ".csv");
    // }

    // private boolean isContent(Map<String, Object> fields, CrawlerPluginContent content, String contentType, String extension) {
    //     if (content == null) return false;
    //     if (content.getContentType() != null && content.getContentType().equals(contentType)) return true;

    //     Object extensionField = fields.get("__$Extension$__");
    //     if (extensionField != null && extensionField.toString().equals(extension)) return true;

    //     return false;
    // }

    // private void replaceContent(String crawlUrl, CrawlerPluginContent content) throws CrawlerPluginException {
    //     Charset charset = content.getCharset();
    //     if (charset == null) {
    //         charset = StandardCharsets.UTF_8;
    //     }

    //     try (
    //         InputStream inputStream = content.getInputStream();
    //         PrintWriter writer = new PrintWriter(new OutputStreamWriter(content.getOutputStream()))) {

    //         LineIterator lines = IOUtils.lineIterator(inputStream, charset);
    //         while (lines.hasNext()) {
    //             String line = lines.next().replaceAll("IBM", "International Business Machines");
    //             writer.println(line);
    //         }

    //         content.setCharset(StandardCharsets.UTF_8);

    //     } catch (IOException e) {
    //         throw new CrawlerPluginException(String.format("The document %s cannot be updated by the crawler plugin.", crawlUrl), e);
    //     }
    // }

    // private void addNewContent(String crawlUrl, CrawlerPluginDocument document) throws CrawlerPluginException {
    //     Map<String, Object> fields = document.getFields();
    //     if (!fields.containsKey("__$ContentURL$__")) return;

    //     String contentUrl = (String) fields.get("__$ContentURL$__");
    //     CrawlerPluginContent pluginContent = document.newContent();

    //     try (CloseableHttpClient httpclient = HttpClients.createDefault();
    //          CloseableHttpResponse response = httpclient.execute(new HttpGet(contentUrl))) {

    //         if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
    //             HttpEntity entity = response.getEntity();
    //             try (InputStream inputStream = entity.getContent();
    //                  OutputStream outputStream = pluginContent.getOutputStream()) {

    //                 IOUtils.copy(inputStream, outputStream);
    //             }

    //             pluginContent.setContentType(entity.getContentType() != null ? entity.getContentType().getValue() : null);
    //             pluginContent.setCharset(entity.getContentEncoding() != null ? Charset.forName(entity.getContentEncoding().getValue()) : null);
    //         }

    //     } catch (IOException e) {
    //         throw new CrawlerPluginException(String.format("The document %s cannot be updated by the crawler plugin.", crawlUrl), e);
    //     }

    //     document.setContent(pluginContent);
    // }
    
    @Override
    public void term() throws CrawlerPluginException {
        // Terminate the plugin (if any necessary cleanup is needed)
    }
}
