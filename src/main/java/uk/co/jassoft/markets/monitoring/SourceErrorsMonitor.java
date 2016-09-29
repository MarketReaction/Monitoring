package uk.co.jassoft.markets.monitoring;

import uk.co.jassoft.markets.datamodel.sources.Source;
import uk.co.jassoft.markets.datamodel.sources.SourceUrl;
import uk.co.jassoft.markets.datamodel.sources.errors.SourceError;
import uk.co.jassoft.markets.repository.SourceErrorRepository;
import uk.co.jassoft.markets.repository.SourceRepository;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by jonshaw on 20/06/2016.
 */
@Component
public class SourceErrorsMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(SourceErrorsMonitor.class);

    @Autowired
    private SourceErrorRepository sourceErrorRepository;

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    protected MongoTemplate mongoTemplate;

    public void monitor()
    {
        int pageCount = 0;

        while (true) {
            Page<SourceError> sourceErrorPage = sourceErrorRepository.findAll(new PageRequest(pageCount, 500));

            ArrayList<SourceError> sourceErrors = new ArrayList<>(sourceErrorPage.getContent());

            if(sourceErrors.isEmpty()) {
                break;
            }

            List<SourceError> oldErrors = sourceErrors.parallelStream().filter(sourceError ->
                sourceError.getDate().before(new DateTime().minusHours(1).toDate()))
                .collect(Collectors.toList());

            sourceErrorRepository.delete(oldErrors);

            sourceErrors.removeAll(oldErrors);

            Map<String, List<SourceError>> urlErrors = sourceErrors.stream().collect(Collectors.groupingBy(SourceError::getUrl));

            for(String url : urlErrors.keySet()) {
                List<SourceError> errorsForUrl = urlErrors.get(url);

                LOG.info("Processing [{}] errors for URL [{}]", errorsForUrl.size(), url);

                SourceError sourceError = errorsForUrl.get(0);

                Source source = sourceRepository.findOne(sourceError.getSource());

                Optional<SourceUrl> baseSourceUrl = source.getUrls().parallelStream().filter(sourceUrl -> sourceUrl.getUrl().equals(url)).findFirst();

                if(baseSourceUrl.isPresent()) {

                    if (baseSourceUrl.get().getDisabledUntil() != null && baseSourceUrl.get().getDisabledUntil().after(new Date())) {
                        continue;
                    }

                    baseSourceUrl.get().setDisabledUntil(new DateTime().plusHours(1).toDate());

                    mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(source.getId()).and("urls.url").is(url)),
                            new Update().set("urls.$", baseSourceUrl.get())
                            , Source.class);
                }

            }

            sourceErrorRepository.delete(sourceErrors);

            pageCount++;
        }

    }
}
