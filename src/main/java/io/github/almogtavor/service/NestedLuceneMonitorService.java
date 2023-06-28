package io.github.almogtavor.service;

import org.apache.lucene.document.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.join.*;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.BytesRef;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.monitor.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@Slf4j
@Component
@NoArgsConstructor
@ConditionalOnProperty(name = "lucene.monitor.mode", havingValue = "nested")
public class NestedLuceneMonitorService implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        MonitorConfiguration monitorConfig = new MonitorConfiguration();
        Monitor monitor = new Monitor(new StandardAnalyzer(), new TermFilteredPresearcher(), monitorConfig);

        registerQueries(monitor);

        // Creating a parent document with child documents
        Document parentDoc = new Document();
        parentDoc.add(new StringField("id", "g1", Field.Store.YES));
        parentDoc.add(new StringField("color", "green", Field.Store.YES));
        parentDoc.add(new StringField("title", "Grass", Field.Store.YES));
        parentDoc.add(new StoredField("isParent", "true"));

        // Creating child document
        Document childDoc1 = new Document();
        childDoc1.add(new TextField("childField1", "childValue1", Field.Store.YES));
        childDoc1.add(new StringField("isParent", "false", Field.Store.YES));
        
        Document childDoc2 = new Document();
        childDoc2.add(new TextField("childField2", "childValue2", Field.Store.YES));
        childDoc2.add(new StringField("isParent", "false", Field.Store.YES));

        Document[] documents = {parentDoc,childDoc1};
        MultiMatchingQueries<HighlightsMatch> hm = monitor.match(documents, HighlightsMatch.MATCHER);
        log.info("Got " + hm.getMatchCount(0) + " matches");
        hm.getMatches(0).forEach(m -> {
            log.info("Match: " + m.getQueryId() + " with " + m.getHitCount());
            m.getHits("childField1").forEach(h -> {
                log.info("  hit: " + h.toString() + " - " + childDoc1.get("childField1").substring(h.startOffset, h.endOffset));
            });
        });
    }

    private void registerQueries(Monitor monitor) throws IOException, ParseException {
        MonitorQuery monitorQuery = newMonitorQuery("ChildQuery1", "childField1:childValue1", Collections.singletonMap("customer", "123"));
        monitor.register(monitorQuery);
        monitor.register(newMonitorQuery("ChildQuery2", "childField2:childValue2", Collections.singletonMap("customer", "124")));
    }

    private MonitorQuery newMonitorQuery(String id, String queryString, Map<String, String> metadata) throws ParseException {
        QueryParser qp = new QueryParser("childField1", new StandardAnalyzer());
        Query childQuery = qp.parse(queryString);

        // Construct ToParentBlockJoinQuery from child query
        BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("isParent", "true")));
        ToParentBlockJoinQuery parentJoinQuery = new ToParentBlockJoinQuery(childQuery, parentFilter, ScoreMode.None);

        log.info("Registered monitor query " + id);
        return new MonitorQuery(id, parentJoinQuery, queryString, metadata);
    }

}
