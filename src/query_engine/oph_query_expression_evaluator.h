/*
    Ophidia IO Server
    Copyright (C) 2014-2024 CMCC Foundation

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __OPH_QUERY_EXPRESSION_EVALUATOR_H__
#define __OPH_QUERY_EXPRESSION_EVALUATOR_H__

#include <mysql.h>		// It contains UDF-related symbols and data structures
#include <mysql_com.h>
#include <ltdl.h>

#include "oph_query_parser.h"

/* Definition of the structure/functions used to contruct and use the symtable (1), 
build the syntax tree (2), execute type chacks(3) and interact with the library (4).*/


//---------- 1

//value type
typedef enum _oph_query_expr_value_type {
	OPH_QUERY_EXPR_TYPE_DOUBLE,
	OPH_QUERY_EXPR_TYPE_LONG,
	OPH_QUERY_EXPR_TYPE_STRING,
	OPH_QUERY_EXPR_TYPE_BINARY,
	OPH_QUERY_EXPR_TYPE_NULL
} oph_query_expr_value_type;

//
/**
* \brief              Struct used to store the values of every type
* \param type         Type of the value stored   
* \param double_value Value if type is double
* \param long_value   Value if type is long
* \param string_value Value if type is string
* \param binary_value Pointer to value if type is binary
* \param free_flag 	  Flag set to 1 if value is a result computed by a function and should be manually freed, 0 otherwise
* \param jump_flag    Flag set to 1 if result must be jumped by other functions, 0 otherwise
*/
typedef struct _oph_query_expr_value {
	oph_query_expr_value_type type;

	union {
		double double_value;
		long long long_value;
		char *string_value;
		oph_query_arg *binary_value;
	} data;

	char free_flag;
	char jump_flag;
} oph_query_expr_value;

/**
 * \brief			            Structure to contain UDF plugins function symbols
 * \param init_api   	    Pointer to init function in shared lib 
 * \param reset_api     	Pointer to reset function in shared lib (can be NULL)
 * \param clear_api       Pointer to clear function in shared lib (can be NULL)
 * \param add_api         Pointer to add function in shared lib (can be NULL)
 * \param exec_api        Pointer to exec function in shared lib
 * \param deinit_api      Pointer to deinit function in shared lib
 */
typedef struct {
	lt_ptr init_api;
	lt_ptr reset_api;
	lt_ptr clear_api;
	lt_ptr add_api;
	lt_ptr exec_api;
	lt_ptr deinit_api;
} oph_plugin_api;

/**
* \brief               Descriptor of udf primitives
* \param initialized   The descriptor is already initialized through an init function 
* \param aggregate     The 0 if the function is simple 1 if it is aggragate
* \param clear         For aggregate functions it indicates if the clear action needs to be executed. For simple functions it is ignored.   
* \param dlh           Pointer to the structures initialized by an init function
* \param function      plugin information
* \param initid        Pointer used by udf primitives
* \param internal_args Pointer to structures where the arguments values are stored
*/
typedef struct _oph_query_expr_udf_descriptor {
	char initialized;
	char aggregate;
	char clear;
	void *dlh;
	oph_plugin_api function;
	UDF_INIT *initid;
	UDF_ARGS *internal_args;
} oph_query_expr_udf_descriptor;

/**
* \brief			Symble table record structure
* \param name 		name of the object stored	
* \param type 		type of object (1 for constant, 2 for Function)
* \param value		Constant or variable value. (only with type 1)
* \parm  fun_type  	function type: 0 for constant parameters; 1 otherwise; (only with type 2)
* \param numArgs	Number of function arguments (only with type 2)
* \param function	Pointer to function (only with type 2)
*/
typedef struct _oph_query_expr_record {
	char *name;
	int type;

	//ONLY with type 1
	oph_query_expr_value value;

	//ONLY with type 2
	 oph_query_expr_value(*function) (oph_query_expr_value *, int, char *, oph_query_expr_udf_descriptor *, int, int *);
	int fun_type;
	int numArgs;
} oph_query_expr_record;

/**		
* \brief			Symble table structure		
* \param maxSize 	size of symtable array		
* \param array 		array of pointer to oph_query_expr_records. NULL if pointer is not there		
*/
typedef struct _oph_query_expr_symtable {
	int maxSize;
	oph_query_expr_record **array;
} oph_query_expr_symtable;

//functions to interact with symtable

/**
 * \brief               Allocates space for the global function symtable and adds to it a set of built-in functions
 * \param size          The number of the extra functions that can be added to the function symtable (>= 0)
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_create_function_symtable(int size);

/**
 * \brief               Allocates space for the symtable
 * \param table         The reference to the symtable pointer to be initiated
 * \param size          The number of the extra variables/functions that can be added to the symtable (>= 0)
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_create_symtable(oph_query_expr_symtable ** table, int size);

/**
 * \brief               De-allocates all the resorces used by a symtable  
 * \param table         The reference to the symtable to be destroyed
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_destroy_symtable(oph_query_expr_symtable *);

/**
 * \brief               Lookup by name in the symtable 
 * \param name          The name of the record to be returned
 * \param table         The reference to the symtable to inspect
 * \return              Returns a record with matching name if present; NULL otherwise
 */
oph_query_expr_record *oph_query_expr_lookup(const char *name, oph_query_expr_symtable * table);

/**
 * \brief               Add to the symtable a variable of type OPH_QUERY_EXPR_TYPE_DOUBLE 
 * \param name          The name of the new variable
 * \param value         The double_value of the new variable
 * \param table         The reference to the target symtable 
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_add_double(const char *name, double value, oph_query_expr_symtable * table);

/**
 * \brief               Add to the symtable a variable of type OPH_QUERY_EXPR_TYPE_LONG 
 * \param name          The name of the new variable
 * \param value         The long_value of the new variable
 * \param table         The reference to the target symtable 
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_add_long(const char *name, long long value, oph_query_expr_symtable * table);

/**
 * \brief               Add to the symtable a variable of type OPH_QUERY_EXPR_TYPE_STRING
 * \param name          The name of the new variable
 * \param value         The string_value of the new variable
 * \param table         The reference to the target symtable 
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_add_string(const char *name, char *value, oph_query_expr_symtable * table);

/**
 * \brief               Add to the symtable a variable of type OPH_QUERY_EXPR_TYPE_BINARY
 * \param name          The name of the new variable
 * \param value         A pointer to the binary_value of the new variable
 * \param table         The reference to the target symtable 
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_add_binary(const char *name, oph_query_arg * value, oph_query_expr_symtable * table);

/**
 * \brief               add a new function to the table (NOTE: doesn't updates old values the same way add_variable does)
 * \param name          The name of the function to be added
 * \param fun_type      0 if the funtion takes a fixed param number; 1 otherwhise;
 * \param numArgs       The number of args accepted by the fuction
 * \param function      A reference to an implementation of the function
 * \param symtable      A reference to the target symtable
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_add_function(const char *name, int fun_type, int numArgs, oph_query_expr_value(*function) (oph_query_expr_value *, int, char *,
													      oph_query_expr_udf_descriptor *, int, int *), oph_query_expr_symtable * symtable);



//---------- 2

/**
 * Node types.
 */
typedef enum _oph_query_expr_node_type {
	eVALUE,
	eSTRING,
	eVAR,
	eMULTIPLY,
	ePLUS,
	eMINUS,
	eDIVIDE,
	eEQUAL,
	eMOD,
	eAND,
	eOR,
	eNEG,
	eNOT,
	eFUN,
	eARG
} oph_query_expr_node_type;

/**
* \brief			The node structure
* \param type 		type of node	
* \param left 		left side of the tree		
* \param right		right side of the tree		
* \param value		node value; valid only when type is eVALUE	
* \param descriptor	descriptor of udf; valid only when the type is eFUN	
* \param name		node name; valid only when type is eVAR e eFUN		
*/
typedef struct _oph_query_expr_node {
	oph_query_expr_node_type type;
	struct _oph_query_expr_node *left;
	struct _oph_query_expr_node *right;

	oph_query_expr_value value;

	oph_query_expr_udf_descriptor descriptor;
	char *name;
} oph_query_expr_node;

/**
* \brief               Creates a eValue node
* \param value         The value of the node (a double)
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_double(double value);

/**
* \brief               Creates a eValue node
* \param value         The value of the node (a long)
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_long(long long value);

/**
* \brief               Creates a eValue node
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_null();

/**
* \brief               Creates a eValue node
* \param value         The value of the node (a string)
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_string(char *value);


/**
* \brief               Creates a eVAR node
* \param name          The value of the node (a string)
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_variable(char *name);

/**
* \brief               Creates a eFUN node
* \param name          The value of the node (a string)
* \param args          The reference to child nodes that contains the arguments of the function                 
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_function(char *name, oph_query_expr_node * args);

/**
* \brief               Creates an operation node storing its type, the left and the right operand nodes. 
* \param type          The node type for the new node
* \param left          A reference to the left side of the tree                 
* \param right         A reference to the right side of the tree                 
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_operation(oph_query_expr_node_type type, oph_query_expr_node * left, oph_query_expr_node * right);

/**
* \brief               Recurrently destroys the given tree and frees all its resources 
* \param b             A reference to the root node of the tree    
* \return              Returns the newly created node or NULL in case of error
*/
int oph_query_expr_delete_node(oph_query_expr_node * b, oph_query_expr_symtable * table);



//---------- 3

//type check functions ---------------------------------------------------------------before committing make sure these comments are good

/**
* \brief               Given an oph_query_expr_value returns its double value if possible. 
                       If the oph_query_expr_value is of the wrong type, it sets *er equal to -1
                       returns -1.  
* \param value         The target oph_query_expr_value
* \param er            A pointer to an error flag. Is set to -1 to comunicate an evaluation error
* \param fun_name      The name of the function that is being parsed.   
* \return              Returns the correct double_value; -1 in case of type error
*/
double get_double_value(oph_query_expr_value value, int *er, const char *fun_name);

/**
* \brief               Given an oph_query_expr_value returns its long_value if possible. 
                       If the oph_query_expr_value is of the wrong type, it sets *er equal to -1
                       returns -1.  
* \param value         The target oph_query_expr_value
* \param er            A pointer to an error flag. Is set to -1 to comunicate an evaluation error
* \param fun_name      The name of the function that is being parsed.
* \return              Returns the correct long_value; -1 in case of type error
*/
long long get_long_value(oph_query_expr_value value, int *er, const char *fun_name);

/**
* \brief               Given an oph_query_expr_value returns its string_value if possible. 
                       If the oph_query_expr_value is of the wrong type, it sets *er equal to -1
                       returns NULL.  
* \param value         The target oph_query_expr_value
* \param er            A pointer to an error flag. Is set to -1 to comunicate an evaluation error
* \param fun_name      The name of the function that is being parsed.
* \return              Returns the correct string_value; -1 in case of type error
*/
char *get_string_value(oph_query_expr_value value, int *er, const char *fun_name);

/**
* \brief               Given an oph_query_expr_value returns its binary_value if possible. 
                       If the oph_query_expr_value is of the wrong type, it sets *er equal to -1
                       returns NULL.  
* \param value         The target oph_query_expr_value
* \param er            A pointer to an error flag. Is set to -1 to comunicate an evaluation error
* \param fun_name      The name of the function that is being parsed.
* \return              Returns the correct binary_value; -1 in case of type error
*/
oph_query_arg *get_binary_value(oph_query_expr_value value, int *er, const char *fun_name);



//---------- 4

//these 2 functions are used by the user to get a AST tree and evaluate it. 

/**
* \brief               Creates an AST from an expression 
* \param expr          The string that the AST will represent
* \param e             A reference to the pointer that will point to the created tree     
* \return              Returns 0 if operation was successfull; non-0 if otherwise;
*/
int oph_query_expr_get_ast(const char *expr, oph_query_expr_node ** e);

/**
 * \brief               Evaluates the value of the AST based on the content of a symtable 
 * \param e             A reference to the AST to evaluate
 * \param res           A result that will be set equal to the result     
 * \param table         A reference to the symtable to use during evaluation
 * \return              Returns 0 if operation was successfull; non-0 if otherwise;
 */
int oph_query_expr_eval_expression(oph_query_expr_node * e, oph_query_expr_value ** res, oph_query_expr_symtable * table);

/**
 * \brief               Set the value of all the functions clear flag to 1 
 * \param e             A reference to the AST to evaluate
 */
int oph_query_expr_change_group(oph_query_expr_node * e);

/**
 *\brief                Returns a vector of the names of all the variables in the AST  
 *\param e              The root of the AST
 *\param var_list       Pointer to list of variables to be created
 *\param var_count      Number of variables retrieved
 * \return              Returns 0 if operation was successfull; non-0 if otherwise;
 */
int oph_query_expr_get_variables(oph_query_expr_node * e, char ***var_list, int *var_count);

#endif				// __OPH_QUERY_EXPRESSION_EVALUATOR_H__
