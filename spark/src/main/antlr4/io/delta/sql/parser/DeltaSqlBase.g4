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
 * Copyright (2021) The Delta Lake Project Authors.
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
    : statement ';'* EOF
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
        (NO STATISTICS)? (PARTITIONED BY '(' colTypeList ')')?          #convert
    | RESTORE TABLE? table=qualifiedName TO?
            clause=temporalClause                                       #restore
    | ALTER TABLE table=qualifiedName ADD CONSTRAINT name=identifier
      constraint                                                        #addTableConstraint
    | ALTER TABLE table=qualifiedName
        DROP CONSTRAINT (IF EXISTS)? name=identifier                    #dropTableConstraint
    | OPTIMIZE (path=STRING | table=qualifiedName)
        (WHERE partitionPredicate=predicateToken)?
        (zorderSpec)?                                                   #optimizeTable
    | REORG TABLE table=qualifiedName
        (WHERE partitionPredicate=predicateToken)?
        APPLY LEFT_PAREN PURGE RIGHT_PAREN                              #reorgTable
    | SHOW COLUMNS (IN | FROM) tableName=qualifiedName
        ((IN | FROM) schemaName=identifier)?                            #showColumns
    | cloneTableHeader SHALLOW CLONE source=qualifiedName clause=temporalClause?
       (TBLPROPERTIES tableProps=propertyList)?
       (LOCATION location=stringLit)?                                   #clone
    | .*?                                                               #passThrough
    ;

createTableHeader
    : CREATE TABLE (IF NOT EXISTS)? table=qualifiedName
    ;

replaceTableHeader
    : (CREATE OR)? REPLACE TABLE table=qualifiedName
    ;

cloneTableHeader
    : createTableHeader
    | replaceTableHeader
    ;

zorderSpec
    : ZORDER BY LEFT_PAREN interleave+=qualifiedName (COMMA interleave+=qualifiedName)* RIGHT_PAREN
    | ZORDER BY interleave+=qualifiedName (COMMA interleave+=qualifiedName)*
    ;

temporalClause
    : FOR? (SYSTEM_VERSION | VERSION) AS OF version=(INTEGER_VALUE | STRING)
    | FOR? (SYSTEM_TIME | TIMESTAMP) AS OF timestamp=STRING
    ;

qualifiedName
    : identifier ('.' identifier)* ('.' identifier)*
    ;

propertyList
    : LEFT_PAREN property (COMMA property)* RIGHT_PAREN
    ;

property
    : key=propertyKey (EQ? value=propertyValue)?
    ;

propertyKey
    : identifier (DOT identifier)*
    | stringLit
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | identifier LEFT_PAREN stringLit COMMA stringLit RIGHT_PAREN
    | value=stringLit
    ;

stringLit
    : STRING
    | DOUBLEQUOTED_STRING
    ;

booleanValue
    : TRUE | FALSE
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
    : CHECK '(' exprToken+ ')'                                 #checkConstraint
    ;

// We don't have an expression rule in our grammar here, so we just grab the tokens and defer
// parsing them to later. Although this is the same as `exprToken`, we have to re-define it to
// workaround an ANTLR issue (https://github.com/delta-io/delta/issues/1205)
predicateToken
    :  .+?
    ;

// We don't have an expression rule in our grammar here, so we just grab the tokens and defer
// parsing them to later.
exprToken
    :  .+?
    ;

// Add keywords here so that people's queries don't break if they have a column name as one of
// these tokens
nonReserved
    : VACUUM | RETAIN | HOURS | DRY | RUN
    | CONVERT | TO | DELTA | PARTITIONED | BY
    | DESC | DESCRIBE | LIMIT | DETAIL
    | GENERATE | FOR | TABLE | CHECK | EXISTS | OPTIMIZE
    | REORG | APPLY | PURGE
    | RESTORE | AS | OF
    | ZORDER | LEFT_PAREN | RIGHT_PAREN
    | SHOW | COLUMNS | IN | FROM | NO | STATISTICS
    | CLONE | SHALLOW
    ;

// Define how the keywords above should appear in a user's SQL statement.
ADD: 'ADD';
ALTER: 'ALTER';
APPLY: 'APPLY';
AS: 'AS';
BY: 'BY';
CHECK: 'CHECK';
CLONE: 'CLONE';
COLUMNS: 'COLUMNS';
COMMA: ',';
COMMENT: 'COMMENT';
CONSTRAINT: 'CONSTRAINT';
CONVERT: 'CONVERT';
CREATE: 'CREATE';
DELTA: 'DELTA';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DETAIL: 'DETAIL';
DOT: '.';
DROP: 'DROP';
DRY: 'DRY';
EXISTS: 'EXISTS';
FALSE: 'FALSE';
FOR: 'FOR';
FROM: 'FROM';
GENERATE: 'GENERATE';
HISTORY: 'HISTORY';
HOURS: 'HOURS';
IF: 'IF';
IN: 'IN';
LEFT_PAREN: '(';
LIMIT: 'LIMIT';
LOCATION: 'LOCATION';
MINUS: '-';
NO: 'NO';
NOT: 'NOT' | '!';
NULL: 'NULL';
OF: 'OF';
OR: 'OR';
OPTIMIZE: 'OPTIMIZE';
REORG: 'REORG';
PARTITIONED: 'PARTITIONED';
PURGE: 'PURGE';
REPLACE: 'REPLACE';
RESTORE: 'RESTORE';
RETAIN: 'RETAIN';
RIGHT_PAREN: ')';
RUN: 'RUN';
SHALLOW: 'SHALLOW';
SHOW: 'SHOW';
SYSTEM_TIME: 'SYSTEM_TIME';
SYSTEM_VERSION: 'SYSTEM_VERSION';
TABLE: 'TABLE';
TBLPROPERTIES: 'TBLPROPERTIES';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRUE: 'TRUE';
VACUUM: 'VACUUM';
VERSION: 'VERSION';
WHERE: 'WHERE';
ZORDER: 'ZORDER';
STATISTICS: 'STATISTICS';

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

DOUBLEQUOTED_STRING
    :'"' ( ~('"'|'\\') | ('\\' .) )* '"'
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
