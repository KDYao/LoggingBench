import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleClass {

    Logger logger = LoggerFactory.getLogger(SimpleClass.class);

    public String processList(List<String> list) {
        logger.info("client requested process the following list: {}", list);

        try {
            logger.debug("Starting process");
            // ...processing list here...
            Thread.sleep(5000);
        } catch (RuntimeException | InterruptedException e) {
            logger.error("There was an issue processing the list.", e);
        } finally {
            logger.info("Finished processing");
        }
        return "done";
    }