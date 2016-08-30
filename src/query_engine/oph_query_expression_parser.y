%{
 
#include "oph_query_expression_evaluator.h"
#include "oph_query_expression_parser.h"
#include "oph_query_expression_lexer.h"
#include "oph_server_utility.h"


int eeerror(int mode, oph_query_expr_node **expression, yyscan_t scanner, const char *msg)
{
    UNUSED(mode);
    UNUSED(expression);
    UNUSED(scanner);
    printf("%s\n", msg);
    return 1;
}

%}

%code requires {

#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif
}

%define api.pure
%lex-param   { yyscan_t scanner }
%parse-param { int mode }
%parse-param { oph_query_expr_node **expression }
%parse-param { yyscan_t scanner }

%union {
    double double_value;
    long long long_value;
    char* sym;
    oph_query_expr_node *expression;
}
%left '&' '|' 
%left '-' '+'
%left '*' '/' '%'
%left UMINUS
%left '='
%right '!'

%token <sym> SYMBOL
%token <sym> STRING
%token <double_value> DECIMAL
%token <long_value> INTEGER

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
    | expr '=' expr {if(mode) $$ = oph_query_expr_create_operation( eEQUAL, $1, $3 ); }
    | expr '%' expr {if(mode) $$ = oph_query_expr_create_operation( eMOD, $1, $3 ); }
    | expr '&' expr {if(mode) $$ = oph_query_expr_create_operation( eAND, $1, $3 ); }
    | expr '|' expr {if(mode) $$ = oph_query_expr_create_operation( eOR, $1, $3 ); }
    | '!' expr {if(mode) $$ = oph_query_expr_create_operation( eNOT, NULL, $2 ); }
    | '-' expr  %prec UMINUS { $$ = oph_query_expr_create_operation( eNEG, NULL, $2 ); }
    | '(' expr ')' {if(mode) $$ = $2; }
    | DECIMAL {if(mode) $$ = oph_query_expr_create_double($1);}
    | INTEGER {if(mode) $$ = oph_query_expr_create_long($1);}
    | SYMBOL {if(mode) $$ = oph_query_expr_create_variable($1);
              else free($1);}
    | SYMBOL '(' arg_list ')' {if(mode) $$ = oph_query_expr_create_function($1,$3);
                               else free($1);}
    | STRING {if(mode) $$ = oph_query_expr_create_string($1);
              else free($1);}
    ;

arg_list: 
        expr {if(mode) $$ = oph_query_expr_create_operation(eARG, $1, NULL);}
        |arg_list ',' expr {if(mode) $$ = oph_query_expr_create_operation(eARG, $3, $1);}
        ;
%%
