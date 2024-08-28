package io.delta.standalone.expressions;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Evaluates {@code expr1} like {@code expr2} for {@code new Like(expr1, expr2, likeType, escape)}.
 * <br/>
 * <p>
 * {@code LikeType.LIKE} standerd like expression,  {@code LikeType.ILIKE} ignore case like
 * expression,{@code LikeType.REGEX} standard regex expression match
 * <br/>
 * <p>escape character default \
 * <br/>
 * <p>
 * Note: The expr2 only supports Literal. if likeType is {@code LikeType.REGEX} ,
 * escape will be ignored
 */
public class Like extends BinaryOperator implements Predicate {

    private Pattern PATTERN;
    private final char escape;
    private final LikeType likeType;

    /**
     * default standerd like expression
     *
     * @param left  expr1
     * @param right Literal expression
     */
    public Like(Expression left, Expression right) {
        this(left, right, LikeType.LIKE, '\\');
    }

    /**
     * default standerd like expression,escape must be set
     *
     * @param left   expr1
     * @param right  Literal expression
     * @param escape escape characters
     */
    public Like(Expression left, Expression right, char escape) {
        this(left, right, LikeType.LIKE, escape);
    }

    /**
     * the {@code likeType} optional values {@code LIKE},{@code ILIKE},{@code REGEX}
     *
     * @param left     expr1
     * @param right    Literal expression
     * @param likeType {@code LikeType.LIKE} standerd like expression,{@code LikeType.ILIKE} ignore
     *                 case like expression, {@code LikeType.REGEX} standard regex expression match
     */
    public Like(Expression left, Expression right, LikeType likeType) {
        this(left, right, likeType, '\\');
    }

    /**
     * the {@code likeType} and {@code escape} must be set.
     * the likeType {@code LikeType}.
     * if likeType is {@code LikeType.REGEX} , escape will be ignored
     *
     * @param left     expr1
     * @param right    Literal expression
     * @param likeType {@code LikeType.LIKE} standerd like expression,{@code LikeType.ILIKE} ignore
     *                 case like expression, {@code LikeType.REGEX} standard regex expression match
     */
    public Like(Expression left, Expression right, LikeType likeType, char escape) {
        super(left, right, likeType.name());
        this.likeType = likeType;
        this.escape = escape;
        if (!(right instanceof Literal)) {
            throw new RuntimeException("The right expression only supports literal");
        }
    }

    @Override
    protected Object nullSafeEval(Object leftResult, Object rightResult) {
        String input = leftResult.toString();
        String likePattern = rightResult.toString();

        if (likeType.equals(LikeType.ILIKE)) {
            input = input.toLowerCase(Locale.ROOT);
            likePattern = likePattern.toLowerCase(Locale.ROOT);
        }
        if (PATTERN == null) {
            if (!likeType.equals(LikeType.REGEX)) {
                likePattern = escapeLikeRegex(likePattern, escape);
            }
            PATTERN = Pattern.compile(likePattern);
        }
        if (likeType.equals(LikeType.REGEX)) {
            return PATTERN.matcher(input).find(0);
        } else {
            return PATTERN.matcher(input).matches();
        }

    }


    private String escapeLikeRegex(String pattern, char escape) {
        int i;
        final int len = pattern.length();
        final StringBuilder javaPattern = new StringBuilder(len + len);
        for (i = 0; i < len; i++) {
            char c = pattern.charAt(i);

            if (c == escape) {
                if (i == (pattern.length() - 1)) {
                    throw invalidEscapeSequence(pattern, i);
                }
                char nextChar = pattern.charAt(i + 1);
                if ((nextChar == '_') || (nextChar == '%') || (nextChar == escape)) {
                    javaPattern.append(Pattern.quote(Character.toString(nextChar)));
                    i++;
                } else {
                    throw invalidEscapeSequence(pattern, i);
                }
            } else if (c == '_') {
                javaPattern.append('.');
            } else if (c == '%') {
                javaPattern.append(".*");
            } else {
                javaPattern.append(Pattern.quote(Character.toString(c)));
            }
        }

        return "(?s)" + javaPattern;
    }


    public RuntimeException invalidEscapeSequence(String s, int i) {
        return new RuntimeException("Invalid escape sequence '" + s + "', " + i);
    }


    public enum LikeType {
        LIKE,
        ILIKE,
        REGEX
    }

}
