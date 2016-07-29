/*
    Ophidia IO Server
    Copyright (C) 2014-2016 CMCC Foundation

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

// * Definition of the structure/functions used to contruct and use the symtable (1), build the syntax tree (2) and interact with the library (3).

//---------- 1

/**
 * \brief			Symble table record structure
 * \param name 		name of the object stored	
 * \param type 		type of object (1 for constant, 2 for Function)
 * \param value		Constant or variable value. (only with type 1)
 * \param numArgs	Number of function arguments (only with type 2)
 * \parm  fun_type  function type: 0 for constant parameters; 1 otherwise;
 * \param function	Pointer to function (only with type 2)
 */
typedef struct _oph_query_expr_record {
    char *name;
    int   type;
    //ONLY with type 1
    double value;  
    //ONLY with type 2
    int numArgs; 
	int fun_type;
    double (*function) (double*, int);
} oph_query_expr_record;

/**
 * \brief			Symble table structure
 * \param maxSize 	size of symtable array
 * \param array 	array of pointer to oph_query_expr_records. NULL if pointer is not there
 */
typedef struct _oph_query_expr_symtable {
    int maxSize;
    oph_query_expr_record **array;
} oph_query_expr_symtable;

//functions to interact with symtable

//initialize symtable and adds to it the built-in functions/variables
/*the param size needs to be set greater or equal to the number of additional paramenters
  that the user will add to the symbol table. This number doesn't need to include for the 
  built in functions/variables that are already accounted for.
*/

/**
 * \brief               Allocates space for the symtable and adds to it a set of built-in functions/variables
 * \param table         The reference to the symtable pointer to be initiated
 * \param size          The number of the extra variables/functions that can be added to the symtable (>= 0)
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_create_symtable(oph_query_expr_symtable** table, int size);

/**
 * \brief               De-allocates all the resorces used by a symtable  
 * \param table         The reference to the symtable to be destroyed
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_destroy_symtable(oph_query_expr_symtable*);

/**
 * \brief               Lookup by name in the symtable 
 * \param name          The name of the record to be returned
 * \param table         The reference to the symtable to inspect
 * \return              Returns a record with matching name if present; NULL otherwise
 */
oph_query_expr_record* oph_query_expr_lookup(const char* name, oph_query_expr_symtable *table);

/**
 * \brief               add a new variable to the table or updates it if already present 
 * \param name          The name of the variable to be added or update
 * \param value         The new value
 * \param table         A reference to the target table
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_add_variable(const char* name, double value, oph_query_expr_symtable*);

/**
 * \brief               add a new function to the table (NOTE: doesn't updates old values the same way add_variable does)
 * \param name          The name of the function to be added
 * \param fun_type      0 if the funtion takes a fized param number; 1 otherwhise;
 * \param numArgs       The number of args accepted by the fuction
 * \param function      A reference to an implementation of the function
 * \param symtable      A reference to the target table
 * \return              0 if succesfull; non-0 otherwise
 */
int oph_query_expr_add_function(const char* name, int fun_type, int numArgs, double (*function) (double *, int), oph_query_expr_symtable* symtable);

//---------- 2

/**
 * Node types.
 */
typedef enum _oph_query_expr_node_type
{
    eVALUE,
    eVAR,
    eMULTIPLY,
    ePLUS,
    eMINUS,
    eDIVIDE,
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
 * \param name		node name; valid only when type is eVAR e eFUN
 */
typedef struct _oph_query_expr_node
{
    oph_query_expr_node_type type;
    struct _oph_query_expr_node *left;
    struct _oph_query_expr_node *right; 
    double value;
    char* name;
} oph_query_expr_node;

/**
 * Creates eValue node storing its value in double
 * The expression or NULL in case of no memory
 */

/**
* \brief               Creates a eValue node
* \param value         The value of the node (a double)
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_number(double value);


/**
 * It creates an eVAR expression node storing its name
 * The expression or NULL in case of no memory
 */

/**
* \brief               Creates a eVAR node
* \param name          The value of the node (a string)
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_variable(char* name);

/**
 * creates a eFUN expression node storing its name and the nodes that represent its arguments 
 * The expression or NULL in case of no memory
 */

/**
* \brief               Creates a eFUN node
* \param name          The value of the node (a string)
* \param args          The reference to child nodes that contains the arguments of the function                 
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_function(char* name, oph_query_expr_node *args);

/**
 * Creates an operation node storing its type, the left and the right operand nodes. 
 * The expression or NULL in case of no memory
 */

/**
* \brief               Creates an operation node storing its type, the left and the right operand nodes. 
* \param type          The node type for the new node
* \param left          A reference to the left side of the tree                 
* \param right         A reference to the right side of the tree                 
* \return              Returns the newly created node or NULL in case of error
*/
oph_query_expr_node *oph_query_expr_create_operation(oph_query_expr_node_type type, oph_query_expr_node *left, oph_query_expr_node *right);

/**
* \brief               Recurrently destroys the given tree and frees all its resources 
* \param b             A reference to the root node of the tree    
* \return              Returns the newly created node or NULL in case of error
*/
int oph_query_expr_delete_node(oph_query_expr_node *b);

//---------- 3

//these 2 functions are used by the user to get a AST tree and evaluate it. 

/**
* \brief               Creates an AST from an expression 
* \param expr          The string that the AST will represent
* \param e            A reference to the pointer that will point to the created tree     
* \return              Returns 0 if operation was successfull; non-0 if otherwise;
*/
int oph_query_expr_get_ast(const char *expr, oph_query_expr_node **e);

/**
 *Uses a the information contained in the symtable to evaluate the
 *double value of the AST. The result of the avaluation is stored using the 
 *pointer res. 
 *Returns 1 if the evaluation was succesfull; 0 if not;
 *
 */
/**
 * \brief               Evaluates the value of the AST based on the content of a symtable 
 * \param e             A reference to the AST to evaluate
 * \param res           A result that will be set equal to the result     
 * \param table         A reference to the symtable to use during evaluation
 * \return              Returns 0 if operation was successfull; non-0 if otherwise;
 */
int oph_query_expr_eval_expression(oph_query_expr_node *e, double *res, oph_query_expr_symtable *table);

#endif // __OPH_QUERY_EXPRESSION_EVALUATOR_H__

