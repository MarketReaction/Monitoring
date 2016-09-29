package uk.co.jassoft.markets.monitoring;

import uk.co.jassoft.markets.BaseSpringConfiguration;
import uk.co.jassoft.markets.datamodel.monitoring.MonitoringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Created by jonshaw on 13/07/15.
 */
@Configuration
@ComponentScan("uk.co.jassoft.markets.monitoring")
public class SpringConfiguration extends BaseSpringConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(SpringConfiguration.class);

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context = SpringApplication.run(SpringConfiguration.class, args);

        MonitoringType monitoringType = MonitoringType.valueOf(args[0]);

        LOG.info("Running Monitoring for Type [{}] Args [{}]", monitoringType, args);

        switch (monitoringType) {
            case SourceCrawler:
                context.getBean(SourceCrawlerMonitor.class).monitor();
                break;

            case SourceErrors:
                context.getBean(SourceErrorsMonitor.class).monitor();
                break;
        }

        context.close();
        System.exit(0);
    }


}
