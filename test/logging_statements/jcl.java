package com.logicbig.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExampleClass {
    private static Log log = LogFactory.getLog(ExampleClass.class);

    public static void main(String[] args) {
        log.info("info in the main method");
        log.error("error in the main method");
        log.fatal(Object message);
        log.fatal(Object message, Throwable t);
        log.error(Object message);
        log.error(Object message, Throwable t);
        log.warn(Object message);
        log.warn(Object message, Throwable t);
        log.info(Object message);
        log.info(Object message, Throwable t);
        log.debug(Object message);
        log.debug(Object message, Throwable t);
        log.trace(Object message);
        log.trace(Object message, Throwable t);
  }
}