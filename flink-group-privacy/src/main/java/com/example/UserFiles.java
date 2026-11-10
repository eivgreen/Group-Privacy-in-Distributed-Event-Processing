package com.example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

final class UserFiles {
    private UserFiles() {}

    static Map<String, String> discover(Path directory, long maxFiles) throws IOException {
        Map<String, String> filesByUser = new LinkedHashMap<>();

        try (Stream<Path> files = Files.list(directory)) {
            files.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".txt"))
                    .sorted()
                    .limit(maxFiles)
                    .forEach(path -> {
                        String filename = path.getFileName().toString();
                        String user = filename.substring(0, filename.length() - 4);
                        if (user.isBlank()) {
                            throw new IllegalArgumentException(
                                    "txt files must be named as the names of data sources" + filename);
                        }
                        filesByUser.put(user, path.toString());
                    });
        }

        if (filesByUser.isEmpty()) {
            throw new IllegalArgumentException("no txt files available!");
        }
        return filesByUser;
    }
}
