/*
 * Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.sqsjms.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class SQSJMSClientUtil {
    
    private static final char HYPHEN = '-';

    private static final char UNDERSCORE = '_';

    private static final char DOT = '.';
    
    private static final Pattern ILLEGAL_ATTRIBUTE_NAME_PATTERN = Pattern.compile(UNDERSCORE + "([0-9]+)" +
                                                                                  UNDERSCORE);

    /**
     * Keeping alphabet, numeric characters, hyphens, underscores, or dots
     * Changes everything to underscores Unicode number underscores. ie. (*attr*
     * -> _42_attr_42_)
     */
    public static String serializePropertyName(String name) {

        StringBuilder stringBuilder = new StringBuilder();
        for (char ch : name.toCharArray()) {
            if (Character.isLetterOrDigit(ch) || HYPHEN == ch || DOT == ch) {
                stringBuilder.append(ch);
            } else {
                stringBuilder.append(UNDERSCORE + Integer.toString((int) ch) + UNDERSCORE);
            }
        }
        return stringBuilder.toString();
    }
    
    /**
     * Changes everything from underscores Unicode number underscores
     * back to original character. ie.
     * (_42_attr_42_ -> *attr*)
     */
    public static String deserializePropertyName(String name) {
        String result = name;
        Matcher m = ILLEGAL_ATTRIBUTE_NAME_PATTERN.matcher(result);

        while (m.find()) {
            int charValue = Integer.parseInt(m.group(1));
            result = result.replace("_" + charValue + "_", Character.toString((char) charValue));
        }
        return result;
    }
}
