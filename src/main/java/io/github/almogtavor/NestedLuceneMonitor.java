package io.github.almogtavor;

import org.apache.lucene.queryparser.classic.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class NestedLuceneMonitor {
    public static void main(String[] args) throws IOException, ParseException {
        SpringApplication.run(NestedLuceneMonitor.class);
    }
}
