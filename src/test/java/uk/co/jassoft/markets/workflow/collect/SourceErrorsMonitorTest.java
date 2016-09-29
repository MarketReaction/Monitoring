package uk.co.jassoft.markets.workflow.collect;

import uk.co.jassoft.markets.monitoring.SpringConfiguration;
import uk.co.jassoft.markets.datamodel.sources.errors.SourceErrorBuilder;
import uk.co.jassoft.markets.monitoring.SourceErrorsMonitor;
import uk.co.jassoft.markets.repository.SourceErrorRepository;
import uk.co.jassoft.markets.repository.SourceRepository;
import uk.co.jassoft.utils.BaseRepositoryTest;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by jonshaw on 14/07/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class SourceErrorsMonitorTest extends BaseRepositoryTest {

    @Autowired
    private SourceErrorRepository sourceErrorRepository;

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    private SourceErrorsMonitor sourceErrorsMonitor;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.initMocks(this);

        sourceErrorRepository.deleteAll();
        sourceRepository.deleteAll();
    }

    @Test
    public void testMonitorWithOldErrorsGetDeleted() throws Exception {

        sourceErrorRepository.save(SourceErrorBuilder.aSourceError().withDate(new DateTime().minusHours(6).toDate()).build());
        sourceErrorRepository.save(SourceErrorBuilder.aSourceError().withDate(new DateTime().minusHours(6).toDate()).build());
        sourceErrorRepository.save(SourceErrorBuilder.aSourceError().withDate(new DateTime().minusHours(6).toDate()).build());
        sourceErrorRepository.save(SourceErrorBuilder.aSourceError().withDate(new DateTime().minusHours(6).toDate()).build());
        sourceErrorRepository.save(SourceErrorBuilder.aSourceError().withDate(new DateTime().minusHours(6).toDate()).build());
        sourceErrorRepository.save(SourceErrorBuilder.aSourceError().withDate(new DateTime().minusHours(6).toDate()).build());
        sourceErrorRepository.save(SourceErrorBuilder.aSourceError().withDate(new DateTime().minusHours(6).toDate()).build());

        Assert.assertEquals(7, sourceErrorRepository.count());

        sourceErrorsMonitor.monitor();

        Assert.assertEquals(0, sourceErrorRepository.count());

    }

}