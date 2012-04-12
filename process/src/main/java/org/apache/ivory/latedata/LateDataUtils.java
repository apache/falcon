/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ivory.latedata;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.expression.ExpressionHelper;

public final class LateDataUtils {

    private static final ExpressionEvaluator EVALUATOR = new
            ExpressionEvaluatorImpl();
    private static final ExpressionHelper resolver = ExpressionHelper.get();
    
    private static final String L_P = "\\s*\\(\\s*";
    private static final String R_P = "\\s*\\)";
    private static final String NUM = "[-]?[0-9]+";
    private static final String COMMA = "\\s*,\\s*";
    private static final String OR = "|";

    private static final Pattern exprPattern = Pattern.compile(
            "today" + L_P + NUM + COMMA + NUM + R_P + OR +
            "yesterday" + L_P + NUM + COMMA + NUM + R_P + OR +
            "now" + L_P + NUM + COMMA + NUM + R_P + OR +
            "currentMonth" + L_P + NUM + COMMA + NUM + COMMA + NUM + R_P + OR +
            "lastMonth" + L_P + NUM + COMMA + NUM + COMMA + NUM + R_P + OR +
            "currentYear" + L_P + NUM + COMMA + NUM + COMMA + NUM + COMMA + NUM + R_P + OR +
            "lastYear" + L_P + NUM + COMMA + NUM + COMMA + NUM + COMMA + NUM + R_P);

    public static String offsetTime(String expr, String offsetExpr) throws IvoryException {
        Long duration = getDurationFromOffset(offsetExpr);
        long minutes = duration / (60000);

        Matcher matcher = exprPattern.matcher(expr);

        StringBuffer newExpr = new StringBuffer();
        int index = 0;
        while (matcher.find(index)) {
            String subExpr = expr.substring(matcher.start(), matcher.end());
            String func = subExpr.substring(0, subExpr.indexOf('('));
            if (func.matches("\\s*now\\s*$")) {
                newExpr.append(expr.substring(0, matcher.start())).append(func).
                        append(subExpr.substring(func.length()).replaceAll("\\)\\s*$", "-" + minutes + ")"));
            } else {
                newExpr.append(expr.substring(0, matcher.start())).append(func.trim()).append("WithOffset").
                        append(subExpr.substring(func.length()).replaceAll("\\)\\s*$", ", -" + minutes + ")"));
            }
            expr = expr.substring(matcher.end());
            matcher = exprPattern.matcher(expr);
        }
        return newExpr.toString() + expr;
    }

    public static Long getDurationFromOffset(String offsetExpr) throws IvoryException {
        try {
            return (Long) EVALUATOR.evaluate("${" + offsetExpr + "}",
                        Long.class, resolver, resolver);
        } catch (ELException e) {
            throw new IvoryException("Unable evaluate " + offsetExpr, e);
        }
    }

    public static String addOffset(String dateStr, String offsetExpr) throws IvoryException {
        try {
            Long durationInMillis = getDurationFromOffset(offsetExpr);
            Date date = EntityUtil.parseDateUTC(dateStr.substring(0, 17));
            return EntityUtil.formatDateUTC(new Date(date.getTime() + durationInMillis));
        } catch (Exception e) {
            throw new IvoryException("Unable to add offset(" + offsetExpr + ") to " + dateStr, e);
        }
    }

}
