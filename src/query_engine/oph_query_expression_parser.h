/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_EE_OPH_QUERY_EXPRESSION_PARSER_H_INCLUDED
# define YY_EE_OPH_QUERY_EXPRESSION_PARSER_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int eedebug;
#endif
/* "%code requires" blocks.  */
#line 20 "oph_query_expression_parser.y" /* yacc.c:1909  */


#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

#line 52 "oph_query_expression_parser.h" /* yacc.c:1909  */

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    UMINUS = 258,
    SYMBOL = 259,
    STRING = 260,
    DECIMAL = 261,
    INTEGER = 262,
    VNULL = 263
  };
#endif
/* Tokens.  */
#define UMINUS 258
#define SYMBOL 259
#define STRING 260
#define DECIMAL 261
#define INTEGER 262
#define VNULL 263

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 34 "oph_query_expression_parser.y" /* yacc.c:1909  */

    double double_value;
    long long long_value;
    char* sym;
    oph_query_expr_node *expression;

#line 87 "oph_query_expression_parser.h" /* yacc.c:1909  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int eeparse (int mode, oph_query_expr_node **expression, yyscan_t scanner);

#endif /* !YY_EE_OPH_QUERY_EXPRESSION_PARSER_H_INCLUDED  */
