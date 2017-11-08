package uk.gov.justice.services.test.utils.core.schema;

import static java.lang.String.format;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaComplianceChecker {

    private static final Logger LOG = LoggerFactory.getLogger(uk.gov.justice.services.test.utils.core.schema.SchemaComplianceChecker.class);

    private static final String WINDOWS_NEWLINE = "\r\n";
    private static final String UNIX_NEWLINE = "\n";

    private static String JSON_SCHEMA_FILE_EXTENSION = ".json";
    private static String JSON_SCHEMA_BASE_PATH = "json/schema/";

    private int totalSchemasChecked = 0;
    private boolean isCompliant = false;
    private int numberOfViolatingSchemas = 0;

    public boolean isCompliant() {
        return isCompliant;
    }


    /**
     * Runs checks to identify schemas with the same filename but with different content across
     * modules
     */
    public void runComplianceCheck() throws Exception {
        this.isCompliant = false;
        this.totalSchemasChecked = 0;
        this.numberOfViolatingSchemas = -1;

        //Enumerate all schema directories
        Enumeration<URL> schemaDir = getSchemaFolderURLs();

        final List<SchemaEntry> schemaFilesList = new ArrayList<SchemaEntry>();
        //Discard all non .json URLs
        //Possibly handle recursion
        while (schemaDir.hasMoreElements()) {
            final URL url = schemaDir.nextElement();
            LOG.debug("Found this schema dir {}", url.toString());

            final String protocol = url.getProtocol().toLowerCase();

            if (protocol.equals("file")) {

                //Pull all the schema files from that directory
                schemaFilesList.addAll(grabFiles(url));

            } else if (protocol.equals("jar")) {

                //Pull all the schema files from the directory in the JAR
                schemaFilesList.addAll(grabFilesFromJarInClasspath(url));

            }
        }

        //sort by filename to improve experience when going through the list and fixing files
        //hashmaps will not maintain insertion ordering when iterating through keyset
        schemaFilesList.sort(Comparator.comparing(SchemaEntry::getFilename));


        //compute checksums
        schemaFilesList
                .forEach((SchemaEntry e) ->
                {
                    e.setChecksum(calculateSHA256Checksum(e.getUrl()));

                    LOG.debug("{} - {}", e.getFilename(), e.getChecksum());
                });
        totalSchemasChecked = schemaFilesList.size();


        //Identify schemas with the same name, count how many times they occur
        Map<String, Long> schemaOccurrenceMap =
                schemaFilesList
                        .stream()
                        .map(e -> e.getFilename())
                        .collect(groupingBy(e -> e, counting()));


        //check if they are the same
        if (LOG.isDebugEnabled()) {
            schemaOccurrenceMap
                    .forEach((k, v) -> LOG.debug("{} - is replicated {} time(s) across modules", k, v));
        }

        //add to the list every schema that appears more than once
        List<String> duplicatedSchemasList =
                schemaOccurrenceMap.entrySet()
                        .stream()
                        .filter(e -> e.getValue() > 1)
                        .map(e -> e.getKey())
                        .sorted()
                        .collect(Collectors.toList());

        if (duplicatedSchemasList.size() < 1) {
            this.isCompliant = true;
            this.numberOfViolatingSchemas = 0;
            return;
        }

        final Map<String, Set<String>> offenders = new HashMap<>();

        duplicatedSchemasList.forEach((e) -> {
            Set<String> setOfDistinctVersions = schemaFilesList
                    .stream()
                    .filter(schemaEntry -> schemaEntry.getFilename().equals(e))
                    .map(schemaEntry -> schemaEntry.getChecksum())
                    .distinct()
                    .collect(Collectors.toSet());
            if (setOfDistinctVersions.size() > 1) {
                LOG.error("{} has {} distinct versions", e, setOfDistinctVersions.size());
            } else {
                LOG.debug("{} is the same across modules", e);
            }
            if (setOfDistinctVersions.size() > 1) {
                offenders.put(e, setOfDistinctVersions);
            }
        });

        final Map<String, String> schemaContentHashMap =
                offenders
                        .entrySet()
                        .stream()
                        .map(e -> e.getValue())
                        .flatMap(l -> l.stream())
                        .collect(Collectors.toSet()).stream().collect(Collectors.toMap(e -> e, Function.identity()));

        schemaContentHashMap
                .entrySet()
                .stream()
                .forEach(eSet -> {
                    final URL url = schemaFilesList
                            .stream()
                            .filter(e -> e.getChecksum().equals(eSet.getValue()))
                            .map(e -> e.getUrl())
                            .findAny()
                            .get();

                    String fileContents = null;
                    fileContents = readURL(url);
                    eSet.setValue(fileContents);
                });


        if (offenders.size() < 1) {
            this.isCompliant = true;
            LOG.info("Schema compliance passed. {} files checked in total", this.totalSchemasChecked);
        } else {

            this.numberOfViolatingSchemas = offenders.size();
            this.isCompliant = false;
            final String report = generateReport(schemaFilesList, offenders, schemaContentHashMap);
            LOG.error(report);
        }

    }

    String readURL(URL url) {
        String fileContents;
        try {
            fileContents = IOUtils.toString(url, Charset.forName("UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return fileContents;
    }

    String calculateSHA256Checksum(URL url) {
        return DigestUtils.sha256Hex(readURL(url));
    }

    Enumeration<URL> getSchemaFolderURLs() throws IOException {
        return SchemaComplianceChecker.class.getClassLoader().getResources(JSON_SCHEMA_BASE_PATH);
    }

    public String generateReport(List<SchemaEntry> schemaFiles, Map<String, Set<String>> offenders, Map<String, String> fileHashMap) {

        StringBuilder sb = new StringBuilder();
        sb.append("Summary of offending schemas files:\n\n");
        offenders.forEach((f, set) ->
        {
            sb.append("******** ").append(f).append(" ********\n");
            int i = 0;

            List<Patch> patches = new ArrayList<>();
            Iterator<String> it = set.iterator();
            String current = null;
            String next = null;
            while (it.hasNext()) {
                current = next;
                if (current == null) {
                    current = it.next();
                }
                if (it.hasNext()) {
                    next = it.next();
                } else {
                    break;
                }
                patches.add(DiffUtils.diff(convertFileContentToLines(fileHashMap.get(current)),
                        convertFileContentToLines(fileHashMap.get(next))));
            }
            int index = 0;
            for (String c : set) {
                //get all files with this checksum
                sb.append("   sha256: ").append(c).append("\n");
                List<URL> urls = schemaFiles
                        .stream()
                        .filter(se -> se.getFilename().equals(f) && se.getChecksum().equals(c))
                        .map(se -> se.getUrl())
                        .collect(Collectors.toList());
                urls.forEach(url -> {
                    sb.append("   + ").append(url.toString()).append("\n");
                });
                sb.append("\n");

                if (index + 1 < set.size()) {
                    List<Delta> deltas = patches.get(index++).getDeltas();

                    deltas.forEach(e -> sb.append(e.toString()).append("\n"));
                }

                sb.append("\n");
            }
            sb.append("\n\n");
        });
        return sb.toString();

    }

    List<SchemaEntry> grabFilesFromJarInClasspath(URL url) throws Exception {

        String pathToTheJar = extractJarPath(url);
        if (pathToTheJar==null)
        {
            throw new Exception(format("Unable to extract the jar file path from the url: %s", url.toString()));
        }

        URL urlToTheJar = new URL(pathToTheJar);

        JarFile jarFile = new JarFile(Paths.get(urlToTheJar.toURI()).toFile());

        return jarFile
                .stream()
                .filter(e -> (e.getName().startsWith(JSON_SCHEMA_BASE_PATH) && e.getName().toLowerCase().endsWith(JSON_SCHEMA_FILE_EXTENSION)))
                .map(e -> e.getName())
                .collect(toList())
                .stream()
                .map((String e) -> {
                    final String pathToSchemaInsideJar = "jar:" + pathToTheJar + "!/" + e;
                    LOG.debug(pathToSchemaInsideJar);
                    try {
                        return new SchemaEntry(new URL(pathToSchemaInsideJar), e.substring(e.lastIndexOf('/') + 1));
                    } catch (MalformedURLException mue) {
                        throw new RuntimeException(mue);
                    }
                })
                .collect(toList());
    }

    String extractJarPath(URL url) {

        String path = url.getPath();
        int index = path.indexOf("!/" + JSON_SCHEMA_BASE_PATH);
        if (index<0)
        {
            return null;
        }
        return path.substring(0, index);
    }

    List<SchemaEntry> grabFiles(URL url) throws Exception {

        return Arrays.asList(Paths.get(url.toURI()).toFile()
                .listFiles((e) -> e.getAbsolutePath().toLowerCase().endsWith(JSON_SCHEMA_FILE_EXTENSION)))
                .stream()
                .map(file -> {
                    try {
                        return new SchemaEntry(new URL(url.toString() + file.getName()), file.getName());
                    } catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(toList());
    }

    private static List<String> convertFileContentToLines(String fileContents) {
        String newlineDelimiter = UNIX_NEWLINE;
        if (fileContents.indexOf(WINDOWS_NEWLINE) > 0) {
            //windows file
            newlineDelimiter = WINDOWS_NEWLINE;
        }

        return Arrays.asList(fileContents.split(newlineDelimiter));
    }

    public int getTotalSchemasChecked() {
        return this.totalSchemasChecked;
    }


    public int getNumberOfViolatingSchemas() {
        return numberOfViolatingSchemas;
    }
}
