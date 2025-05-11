package com.example.annotation;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class AnnotationCLI {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationCLI.class);

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("AnnotationCLI").build()
                .defaultHelp(true)
                .description("Runs the annotation process on a SQLite database.");

        parser.addArgument("-db", "--database-path")
                .required(true)
                .help("Path to the SQLite database file to be annotated.");

        parser.addArgument("-t", "--threads")
                .type(Integer.class)
                .setDefault(Runtime.getRuntime().availableProcessors())
                .help("Number of parallel threads for CoreNLP processing (default: available processors).");

        parser.addArgument("-b", "--batch-size")
                .type(Integer.class)
                .setDefault(1000) // Default from Annotations.java and original Pipeline.java
                .help("Number of documents to commit per transaction during annotation (default: 1000).");

        parser.addArgument("-l", "--limit")
                .type(Integer.class)
                .required(false)
                .help("Maximum number of new documents to process in this run (optional).");

        parser.addArgument("--force")
                .action(net.sourceforge.argparse4j.impl.Arguments.storeTrue())
                .help("Force re-annotation from the beginning, overwriting existing annotations for processed documents.");

        try {
            Namespace ns = parser.parseArgs(args);

            Path dbPath = Paths.get(ns.getString("database_path"));
            int threads = ns.getInt("threads");
            int batchSize = ns.getInt("batch_size");
            Integer limit = ns.getInt("limit"); // Can be null
            boolean force = ns.getBoolean("force");

            logger.info("Starting annotation process for database: {}", dbPath.toAbsolutePath());
            logger.info("Threads: {}, Batch Size: {}, Limit: {}, Force: {}",
                    threads, batchSize, (limit == null ? "None" : limit), force);

            Annotations.AnnotationStatus status = Annotations.getAnnotationStatus(dbPath);

            if (force || status.needsProcessing) {
                int startDocumentId = force ? 1 : status.startDocumentId;
                if (force && status.startDocumentId > 1) {
                    logger.info("--force specified. Annotations will be re-processed starting from document ID 1.");
                } else if (!force && !status.needsProcessing) {
                     logger.info("No new documents to process. Annotation is up to date.");
                     System.exit(0);
                }


                logger.info("Annotation will run from document ID: {}", startDocumentId);
                Annotations.runAnnotation(dbPath, startDocumentId, threads, batchSize, limit);
                logger.info("Annotation process completed successfully.");

            } else {
                logger.info("Annotation is already complete. No new documents found to process. Use --force to re-annotate.");
            }

        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            logger.error("An error occurred during the annotation process:", e);
            System.exit(1);
        }
    }
} 