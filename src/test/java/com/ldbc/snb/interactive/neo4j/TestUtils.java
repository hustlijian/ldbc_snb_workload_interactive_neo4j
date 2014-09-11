package com.ldbc.snb.interactive.neo4j;

import org.apache.commons.io.FileUtils;

import java.io.File;

public class TestUtils {
    public static File getResource(String path) {
        return FileUtils.toFile(TestUtils.class.getResource(path));
    }
}
