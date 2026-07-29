/*
 * Copyright 2010-2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.sqs.javamessaging.message;

import java.io.ObjectInputFilter;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Builds the serialization filter applied when reading the body of a JMS <code>ObjectMessage</code>.
 * See the ObjectMessage deserialization section of README.md for the configuration contract.
 */
final class ObjectMessageDeserializationFilter {

    static final String TRUSTED_PACKAGES_PROPERTY = "com.amazon.sqs.javamessaging.trustedPackages";

    static final String TRUST_ALL_PACKAGES_PROPERTY = "com.amazon.sqs.javamessaging.trustAllPackages";

    static final String MAX_DEPTH_PROPERTY = "com.amazon.sqs.javamessaging.maxDeserializationDepth";

    static final List<String> DEFAULT_TRUSTED_PACKAGES = List.of("java.lang", "java.util", "java.math", "java.time");

    // Nested collections cost time exponential in nesting depth, since each level rehashes the one
    // below it, so a few kilobytes can occupy a consumer thread indefinitely. A map of lists of maps
    // reaches a depth of about six.
    static final int DEFAULT_MAX_DEPTH = 24;

    // An array length is carried in the stream as an int, so a short payload can otherwise request an
    // allocation large enough to exhaust the heap. No array in a valid message can exceed this, given
    // the 256 KiB maximum SQS message size.
    static final int MAX_ARRAY_LENGTH = 262144;

    private ObjectMessageDeserializationFilter() {
    }

    /**
     * @return the filter to apply, or null if the application has disabled filtering
     */
    static ObjectInputFilter fromSystemProperties() {
        if (Boolean.parseBoolean(System.getProperty(TRUST_ALL_PACKAGES_PROPERTY))) {
            return null;
        }
        return ObjectInputFilter.Config.createFilter(patternFromSystemProperties());
    }

    static String patternFromSystemProperties() {
        StringBuilder pattern = new StringBuilder()
                .append("maxdepth=").append(maxDepth()).append(';')
                .append("maxarray=").append(MAX_ARRAY_LENGTH).append(';');
        for (String trustedPackage : DEFAULT_TRUSTED_PACKAGES) {
            // A single star matches the package itself, leaving its subpackages untrusted.
            pattern.append(trustedPackage).append(".*;");
        }
        for (String entry : configuredEntries()) {
            // Accept the entry both as an exact class name and as a package with its subpackages.
            pattern.append(entry).append(';').append(entry).append(".**;");
        }
        // Patterns are evaluated in order and the first match decides, so this rejection comes last.
        return pattern.append("!*").toString();
    }

    /**
     * Combines this filter with any already in force on the stream, such as one configured through
     * <code>jdk.serialFilter</code>, so that applying ours cannot widen what the application allows.
     */
    static ObjectInputFilter combineWith(ObjectInputFilter ours, ObjectInputFilter existing) {
        if (existing == null) {
            return ours;
        }
        return info -> {
            ObjectInputFilter.Status ourStatus = ours.checkInput(info);
            if (ourStatus == ObjectInputFilter.Status.REJECTED) {
                return ourStatus;
            }
            ObjectInputFilter.Status existingStatus = existing.checkInput(info);
            return existingStatus == ObjectInputFilter.Status.REJECTED ? existingStatus : ourStatus;
        };
    }

    private static Set<String> configuredEntries() {
        Set<String> entries = new LinkedHashSet<>();
        String configured = System.getProperty(TRUSTED_PACKAGES_PROPERTY);
        if (configured == null) {
            return entries;
        }
        Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(entry -> !entry.isEmpty())
                .forEach(entries::add);
        return entries;
    }

    private static int maxDepth() {
        String configured = System.getProperty(MAX_DEPTH_PROPERTY);
        if (configured == null) {
            return DEFAULT_MAX_DEPTH;
        }
        try {
            int maxDepth = Integer.parseInt(configured.trim());
            return maxDepth > 0 ? maxDepth : DEFAULT_MAX_DEPTH;
        } catch (NumberFormatException e) {
            return DEFAULT_MAX_DEPTH;
        }
    }
}
