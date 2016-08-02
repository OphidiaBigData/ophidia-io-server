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

#include "oph_query_expression_functions.h"
#include "oph_query_expression_evaluator.h"
#include "oph_query_parser.h"  
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

oph_query_expr_value oph_id(oph_query_expr_value* args, int num_args, int *er)
{

    long long id = get_long_value(args[0], er, "oph_id");
    long long size = get_long_value(args[1], er, "oph_id");
    oph_query_expr_value res;
    res.type = OPH_QUERY_EXPR_TYPE_LONG;
    res.data.long_value = 1 + floor((id - 1)/(size));
    return res;

}

oph_query_expr_value oph_id2(oph_query_expr_value* args, int num_args, int *er){

    long long id = get_long_value(args[0], er, "oph_id2");
    long long size = get_long_value(args[1], er, "oph_id2");
    long long block_size = get_long_value(args[2], er, "oph_id2");
    oph_query_expr_value res;
    res.type = OPH_QUERY_EXPR_TYPE_LONG;
    res.data.long_value = 1 + (id-1 % block_size) +(floor((id-1)/(size*block_size)))*block_size;
	return res;
}

oph_query_expr_value oph_id_to_index(oph_query_expr_value *args, int num_args, int *er)
{
    long long size;
    long long id = get_long_value(args[0], er, "oph_id_to_index")-1;
    long long index = id;
    if (id < 0) 
    {
        oph_query_expr_value res;
        res.type = OPH_QUERY_EXPR_TYPE_LONG;
        res.data.long_value = -1;
        return res;
    }
    int counter = 1;
    while (counter < num_args)
    {
        size = get_long_value(args[counter++], er, "oph_id_to_index");
        index = (id% size);
        id = (id-index)/size;
    }
    oph_query_expr_value res;
    res.type = OPH_QUERY_EXPR_TYPE_LONG;
    res.data.long_value = index + 1;
    return res;
}

oph_query_expr_value oph_id_to_index2(oph_query_expr_value* args, int num_args, int *er){
    long long id = get_long_value(args[0], er, "oph_id_to_index2");
    long long block_size = get_long_value(args[1], er, "oph_id_to_index2");
    long long size = get_long_value(args[2], er, "oph_id_to_index2");
    oph_query_expr_value res;
    res.type = OPH_QUERY_EXPR_TYPE_LONG;
    res.data.long_value = 1 + ((long long)floor((id-1)/block_size)% size);
    return res;
}

oph_query_expr_value oph_is_in_subset(oph_query_expr_value* args, int num_args, int *er){
    long long id = get_long_value(args[0], er, "oph_is_in_subset");
    long long start = get_long_value(args[1], er, "oph_is_in_subset");
    long long step = get_long_value(args[2], er, "oph_is_in_subset");
    long long size = get_long_value(args[3], er, "oph_is_in_subset");
    oph_query_expr_value res;
    res.type = OPH_QUERY_EXPR_TYPE_LONG;
    long long m1 = (id-start) % step;
    res.data.long_value = !m1 && (id>=start) && (id<=size);
    return res;
}
