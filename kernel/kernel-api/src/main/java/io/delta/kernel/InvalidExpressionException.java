package io.delta.kernel;

import io.delta.kernel.expressions.Expression;

/**
 * Thrown when the given {@link Expression} is not valid.
 * TODO: we may need to divide this further into multiple exceptions.
 */
public class InvalidExpressionException
    extends Exception
{
}
