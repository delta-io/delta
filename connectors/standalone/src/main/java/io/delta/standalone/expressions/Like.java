package io.delta.standalone.expressions;

/**
 * Evaluates {@code expr1} like {@code expr2} for {@code new Like(expr1, expr2)}.
 */
public class Like extends BinaryOperator implements Predicate {
    public Like(Expression left, Expression right) {
        super(left, right, "like");
    }

    @Override
    protected Object nullSafeEval(Object leftResult, Object rightResult) {
        String str = String.valueOf(rightResult);
        String subStr;
        if (str.startsWith("%")) {
            if (str.endsWith("%")) {
                subStr = str.substring(1, str.length() - 1);
                return String.valueOf(leftResult).contains(subStr);
            } else {
                subStr = str.substring(1);
                return String.valueOf(leftResult).endsWith(subStr);
            }
        } else if (str.endsWith("%")) {
            subStr = str.substring(1, str.length() - 1);
            return String.valueOf(leftResult).startsWith(subStr);
        } else {
            return String.valueOf(leftResult).equals(str);
        }

    }
}
