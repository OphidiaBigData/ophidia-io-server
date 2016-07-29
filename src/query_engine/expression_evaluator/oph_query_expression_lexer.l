%{
 
#include "oph_query_expression_evaluator.h"
#include "oph_query_expression_parser.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

 
%}
%option outfile="oph_query_expression_lexer.c" header-file="oph_query_expression_lexer.h"
%option warn nodefault
 
%option reentrant noyywrap never-interactive nounistd
%option bison-bridge
  
EXP ([Ee][+-]?[0-9]+)
WS          [ \r\n\t]*
 
%%
 
{WS}            { /* Skip blanks. */ }

"mysql."
"*"			    { return '*'; }
"+"	            { return '+'; }
"-"			    { return '-'; }
"/"	            { return '/'; }
"("		        { return '('; }
")"		        { return ')'; }
","				{ return ','; }
"~"				{ return '~'; }
"and"			{ return '&'; }
"AND"			{ return '&'; }
"&&" 			{ return '&'; }
"|"|OR|OR   	{ return '|'; }
MOD|% 			{ return '%'; }  
!|NOT 			{ return '!'; }

[0-9]+"."[0-9]*{EXP}? |
"."?[0-9]+{EXP}? {yylval->value = atof(yytext); 
				  return NUMBER;}

[a-zA-Z_$][a-zA-Z0-9_$]*	{
				yylval->sym = strdup(yytext);
				return SYMBOL;
			}
. {}
%%
