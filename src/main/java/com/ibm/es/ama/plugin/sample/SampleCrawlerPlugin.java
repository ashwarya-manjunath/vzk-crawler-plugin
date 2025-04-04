package com.ibm.es.ama.plugin.sample;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
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

// xxrole field
{
    Object roleField = fields.get("ROLE");
    logger.info("Fetched ROLE field: " + roleField);  // Added logger after fetching role field
    if (roleField != null) {
        String role = roleField.toString();
        fields.put("XXROLE", role);
        logger.info("Copied role to xxrole: " + role);
    } else {
        logger.warning("No role field found in fields map.");
    }
}

// xxbgroup field
{
    Object businessGroupField = fields.get("BUSINESSGROUP");
    logger.info("Fetched BUSINESSGROUP field: " + businessGroupField);  // Added logger after fetching businessgroup field
    if (businessGroupField != null) {
        String businessGroup = businessGroupField.toString();
        fields.put("XXBGROUP", businessGroup);
        logger.info("Copied businessgroup to xxbgroup: " + businessGroup);
    } else {
        logger.warning("No businessgroup field found in fields map.");
    }
}


// "formatDate" field
{
Object updatedDateField = fields.get("LASTUPDATEDDATE");

if (updatedDateField != null) {
    String dateStr = updatedDateField.toString().trim();
    logger.info("Received LASTUPDATEDDATE: " + dateStr);

    // Define the input date format ("Tue Apr 12 01:00:00 GMT 2005")
    DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
    DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    try {
        // Parse the input date string
        LocalDate parsedDate = LocalDate.parse(dateStr, inputFormatter);

        // Convert to desired format
        String formatDate = parsedDate.format(outputFormatter);

        logger.info("Extracted formatDate: " + formatDate);

        // Add formatted date while keeping the original LASTUPDATEDDATE
        fields.put("FORMATDATE", formatDate);
        logger.info("Updated fields map with formatDate: " + fields);

    } catch (DateTimeParseException e) {
        logger.log(Level.SEVERE, "Error parsing LASTUPDATEDDATE: " + dateStr, e);
    }
} else {
    logger.warning("No LASTUPDATEDDATE found in fields map.");
}
}
// "updateddatesec" field
{
    Object lastUpdatedDateSecField = fields.get("LASTUPDATEDDATE");

if (lastUpdatedDateSecField != null) {
    try {
        // Define the input date format
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));  // Set timezone to GMT

        // Parse the string into a Date object
        Date parsedDate = sdf.parse(lastUpdatedDateSecField.toString());

        // Convert to epoch seconds
        long epochSeconds = parsedDate.getTime() / 1000;

        // Store the value in the field map
        fields.put("UPDATEDDATESEC", epochSeconds);

        logger.info("Parsed LASTUPDATEDDATE: " + lastUpdatedDateSecField + " to epoch seconds: " + epochSeconds);
    } catch (ParseException e) {
        logger.severe("Failed to parse LASTUPDATEDDATE: " + lastUpdatedDateSecField + " due to: " + e.getMessage());
    }
} else {
    logger.warning("No LASTUPDATEDDATE found in fields map.");
}
}

// "effDateSec" field
{
    
Object rawEffectiveDateValue = fields.get("EFFECTIVEDATE");

if (rawEffectiveDateValue != null) {
     // Log the EFFECTIVEDATE value before parsing
        logger.info("Retrieved EFFECTIVEDATE from fields: " + rawEffectiveDateValue);
    try {
        // Define the input date format
        SimpleDateFormat effectiveDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
        effectiveDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));  // Ensure GMT timezone

        // Parse the date string into a Date object
        Date effectiveParsedDate = effectiveDateFormat.parse(rawEffectiveDateValue.toString());

        // Convert to epoch seconds
        long effectiveEpochTime = effectiveParsedDate.getTime() / 1000;

        // Store the epoch time in the map
        fields.put("EFFDATESEC", effectiveEpochTime);

        logger.info("Parsed EFFECTIVEDATE: " + rawEffectiveDateValue + " to epoch seconds: " + effectiveEpochTime);
    } catch (ParseException ex) {
        logger.severe("Failed to parse EFFECTIVEDATE: " + rawEffectiveDateValue + " due to: " + ex.getMessage());
    }
} else {
    logger.warning("No EFFECTIVEDATE found in fields map.");
}
}

// lastupdateddate Field
{
Object updatedDateSecField = fields.get("UPDATEDDATESEC");

if (updatedDateSecField != null) {
    // Log the raw UPDATEDDATESEC value before parsing
    logger.info("Retrieved UPDATEDDATESEC from fields: " + updatedDateSecField);

    try {
        // Ensure the value is treated as a Long (epoch format)
        Long epochTime = Long.parseLong(updatedDateSecField.toString());

        // Set the LASTUPDATEDDATE field with the same epoch time
        fields.put("LASTUPDATEDDATE", epochTime);

        logger.info("Copied UPDATEDDATESEC: " + epochTime + " to LASTUPDATEDDATE.");
    } catch (NumberFormatException e) {
        logger.warning("Invalid epoch format for UPDATEDDATESEC: " + updatedDateSecField);
    }
} else {
    logger.warning("No UPDATEDDATESEC found in fields map.");
}
}

// effectivedatefilter Field
{
    Object effDateSecField = fields.get("EFFDATESEC");

    if (effDateSecField != null) {
    String rawEpochValue = effDateSecField.toString().trim();

    // Logger BEFORE conversion
    logger.info("EFFDATESEC (raw input before epoch conversion): " + rawEpochValue);

    // Check if the value is numeric (epoch format)
    if (!rawEpochValue.matches("\\d+")) {
        logger.warning("EFFDATESEC is not in epoch format: " + rawEpochValue);
        return; // Exit early if it's not a valid epoch timestamp
    }

    try {
        long epochTime = Long.parseLong(rawEpochValue); // Convert to long

        // Log conversion before processing further
        logger.info("Parsed epoch time: " + epochTime);

        // Convert epoch to LocalDateTime (UTC)
        LocalDateTime dateTime = Instant.ofEpochSecond(epochTime)
                                        .atZone(ZoneId.of("UTC"))
                                        .toLocalDateTime();

        // Format the date to "MM/dd/yyyy"
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
        String formattedDate = dateTime.format(formatter);

        logger.info("Converted EFFDATESEC: " + epochTime + " to EFFECTIVEDATEFILTER: " + formattedDate);

        // Store formatted date while keeping original effDateSec
        fields.put("EFFECTIVEDATEFILTER", formattedDate);

    } catch (Exception e) {
        logger.log(Level.SEVERE, "Unexpected error while processing EFFDATESEC", e);
    }
} else {
    logger.warning("No EFFDATESEC found in fields map.");
}

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
    {
        Object effectiveDateValue = fields.get("EFFECTIVEDATE");

        if (effectiveDateValue != null) {
            String rawEffectiveDateStr = effectiveDateValue.toString().trim();
        
            // 🚨 Logger BEFORE conversion
            logger.info("EFFECTIVEDATE (raw input): " + rawEffectiveDateStr);
        
            // Define the input and output date formatters
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
            try {
                // Parse the input date string
                LocalDate parsedDate = LocalDate.parse(rawEffectiveDateStr, inputFormatter);
        
                // Convert to desired format
                String effDate = parsedDate.format(outputFormatter);
        
                logger.info("Converted EFFECTIVEDATE to EFFDATE (yyyy-MM-dd): " + effDate);
        
                // Store formatted date
                fields.put("EFFDATE", effDate);
                logger.info("Updated fields map with EFFDATE: " + effDate);
        
            } catch (DateTimeParseException e) {
                logger.log(Level.SEVERE, "Error parsing EFFECTIVEDATE: " + rawEffectiveDateStr, e);
            }
        } else {
            logger.warning("No EFFECTIVEDATE found in fields map.");
        }
        
    }
   // title field
//    Object docTitleField = fields.get("docTitle");
// if (docTitleField != null) {
//     String docTitle = docTitleField.toString();

//     // Add title field while keeping original docTitle
//     fields.put("title", docTitle);
// }

    //last-modified date
    {
        Object lastUpdatedDateField = fields.get("LASTUPDATEDDATE");
    
        if (lastUpdatedDateField != null) {
            // Log the raw LASTUPDATEDDATE value before copying
            logger.info("Received LASTUPDATEDDATE (epoch format): " + lastUpdatedDateField);
    
            // Log copying process
            logger.info("Copying LASTUPDATEDDATE to LAST-MODIFIED...");
    
            // Copy value without modification
            fields.put("LAST-MODIFIED", lastUpdatedDateField);
    
            // Confirm update in map
            logger.info("Updated fields map with LAST-MODIFIED: " + lastUpdatedDateField);
        } else {
            logger.warning("No LASTUPDATEDDATE found in fields map.");
        }
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
