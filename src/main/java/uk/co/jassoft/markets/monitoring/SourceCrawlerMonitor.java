package uk.co.jassoft.markets.monitoring;

import uk.co.jassoft.markets.datamodel.sources.Source;
import uk.co.jassoft.markets.datamodel.sources.SourceUrl;
import uk.co.jassoft.markets.repository.SourceRepository;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.function.Predicate;

/**
 * Created by jonshaw on 20/06/2016.
 */
@Component
public class SourceCrawlerMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(SourceCrawlerMonitor.class);

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    protected MongoTemplate mongoTemplate;

    /**
     * If source URL is pending a crawl but is not crawled in 10x the crawl interval then re-enable it.
     */
    public void monitor()
    {
        sourceRepository.findAll().forEach(source -> {

            LOG.info("Checking SourceUrls for Source [{}]", source.getName());

            source.getUrls().stream()
                    .filter(SourceUrl::isEnabled)
                    .filter(SourceUrl::isPendingCrawl)
                    .filter(hasBeenCrawled())
                    .filter(hasCrawlInterval())
                    .filter(isOverdueCrawl())
                    .forEach(sourceUrl -> {

                        sourceUrl.setPendingCrawl(false);

                        mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(source.getId()).and("urls.url").is(sourceUrl.getUrl())),
                                new Update().set("urls.$", sourceUrl)
                                , Source.class);
            });
        });
    }

    public static Predicate<SourceUrl> hasBeenCrawled() {
        return sourceUrl -> sourceUrl.getLastCrawled() != null;
    }

    public static Predicate<SourceUrl> hasCrawlInterval() {
        return sourceUrl -> sourceUrl.getCrawlInterval() != null;
    }

    public static Predicate<SourceUrl> isOverdueCrawl() {
        return sourceUrl -> sourceUrl.getLastCrawled().before(new DateTime().minusSeconds(sourceUrl.getCrawlInterval() * 10).toDate());
    }
}
