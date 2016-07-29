%{
 
#include "oph_query_expression_evaluator.h"
#include "oph_query_expression_parser.h"
#include "oph_query_expression_lexer.h"

int yyerror(int mode, oph_query_expr_node **expression, yyscan_t scanner, const char *msg) {
    printf("%s\n", msg);
}

%}

%code requires {

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif
}

%output  "oph_query_expression_parser.c"
%defines "oph_query_expression_parser.h"
 
%define api.pure
%lex-param   { yyscan_t scanner }
%parse-param { int mode }
%parse-param { oph_query_expr_node **expression }
%parse-param { yyscan_t scanner }



%union {
    double value;
    char* sym;
    oph_query_expr_node *expression;
}
%left '&' '|' 
%left '-' '+'
%left '*' '/' '%'
%right '!'
 
%token <value> NUMBER
%token <sym> SYMBOL

%type <expression> arg_list
%type <expression> expr

%%
 
input
    : expr {if(mode) *expression = $1;}
    | error {*expression = NULL; return 1;}
    ;
 
expr
    : expr '+' expr {if(mode) $$ = oph_query_expr_create_operation( ePLUS, $1, $3 );}
    | expr '*' expr {if(mode) $$ = oph_query_expr_create_operation( eMULTIPLY, $1, $3 ); }
    | expr '-' expr {if(mode) $$ = oph_query_expr_create_operation( eMINUS, $1, $3 ); }
    | expr '/' expr {if(mode) $$ = oph_query_expr_create_operation( eDIVIDE, $1, $3 ); }
    | expr '%' expr {if(mode) $$ = oph_query_expr_create_operation( eMOD, $1, $3 ); }
    | expr '&' expr {if(mode) $$ = oph_query_expr_create_operation( eAND, $1, $3 ); }
    | expr '|' expr {if(mode) $$ = oph_query_expr_create_operation( eOR, $1, $3 ); }
    | '!' expr {if(mode) $$ = oph_query_expr_create_operation( eNOT, NULL, $2 ); }
    | '-' expr { $$ = oph_query_expr_create_operation( eNEG, NULL, $2 ); }
    | '(' expr ')' {if(mode) $$ = $2; }
    | NUMBER {if(mode) $$ = oph_query_expr_create_number($1);}
    | SYMBOL {if(mode) $$ = oph_query_expr_create_variable($1);
              else free($1);}
    | SYMBOL '(' arg_list ')' {if(mode) $$ = oph_query_expr_create_function($1,$3);
                               else free($1);}
    ;

arg_list: 
        expr {if(mode) $$ = oph_query_expr_create_operation(eARG, $1, NULL);}
        |arg_list ',' expr {if(mode) $$ = oph_query_expr_create_operation(eARG, $3, $1);}
        ;
%%
