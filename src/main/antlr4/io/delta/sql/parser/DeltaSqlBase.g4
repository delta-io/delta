/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

grammar DeltaSqlBase;

@members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

// If you add keywords here that should not be reserved, add them to 'nonReserved' list.
statement
    : VACUUM (path=STRING | table=qualifiedName)
        (RETAIN number HOURS)? (DRY RUN)?                               #vacuumTable
    | (DESC | DESCRIBE) DETAIL (path=STRING | table=qualifiedName)      #describeDeltaDetail
    | GENERATE modeName=identifier FOR TABLE table=qualifiedName        #generate
    | (DESC | DESCRIBE) HISTORY (path=STRING | table=qualifiedName)
        (LIMIT limit=INTEGER_VALUE)?                                    #describeDeltaHistory
    | CONVERT TO DELTA table=qualifiedName
        (PARTITIONED BY '(' colTypeList ')')?                           #convert
    | ALTER TABLE table=qualifiedName ADD CONSTRAINT name=identifier
      constraint                                                        #addTableConstraint
    | ALTER TABLE table=qualifiedName
        DROP CONSTRAINT (IF EXISTS)? name=identifier                    #dropTableConstraint
    | .*?                                                               #passThrough
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

colTypeList
    : colType (',' colType)*
    ;

colType
    : colName=identifier dataType (NOT NULL)? (COMMENT STRING)?
    ;

dataType
    : identifier ('(' INTEGER_VALUE (',' INTEGER_VALUE)* ')')?         #primitiveDataType
    ;

number
    : MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

constraint
    : CHECK '(' checkExprToken+ ')'                                 #checkConstraint
    ;

// We don't have an expression rule in our grammar here, so we just grab the tokens and defer
// parsing them to later.
checkExprToken
    :  .+?
    ;

// Add keywords here so that people's queries don't break if they have a column name as one of
// these tokens
nonReserved
    : VACUUM | RETAIN | HOURS | DRY | RUN
    | CONVERT | TO | DELTA | PARTITIONED | BY
    | DESC | DESCRIBE | LIMIT | DETAIL
    | GENERATE | FOR | TABLE | CHECK | EXISTS
    ;

// Define how the keywords above should appear in a user's SQL statement.
ADD: 'ADD';
ALTER: 'ALTER';
BY: 'BY';
CHECK: 'CHECK';
COMMENT: 'COMMENT';
CONSTRAINT: 'CONSTRAINT';
CONVERT: 'CONVERT';
DELTA: 'DELTA';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DETAIL: 'DETAIL';
DROP: 'DROP';
EXISTS: 'EXISTS';
GENERATE: 'GENERATE';
DRY: 'DRY';
HISTORY: 'HISTORY';
HOURS: 'HOURS';
IF: 'IF';
LIMIT: 'LIMIT';
MINUS: '-';
NOT: 'NOT' | '!';
NULL: 'NULL';
FOR: 'FOR';
TABLE: 'TABLE';
PARTITIONED: 'PARTITIONED';
RETAIN: 'RETAIN';
RUN: 'RUN';
TO: 'TO';
VACUUM: 'VACUUM';

// Multi-character operator tokens need to be defined even though we don't explicitly reference
// them so that they can be recognized as single tokens when parsing. If we split them up and
// end up with expression text like 'a ! = b', Spark won't be able to parse '! =' back into the
// != operator.
EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LTE : '<=' | '!>';
GTE : '>=' | '!<';
CONCAT_PIPE: '||';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS  : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
