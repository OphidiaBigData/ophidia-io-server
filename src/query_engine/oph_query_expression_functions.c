/*
    Ophidia IO Server
    Copyright (C) 2014-2020 CMCC Foundation

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
#include "oph_query_plugin_executor.h"
#include "oph_query_engine_log_error_codes.h"
#include "oph_server_utility.h"
#include <debug.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

oph_query_expr_value oph_id(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	UNUSED(num_args);
	UNUSED(name);
	UNUSED(destroy);
	UNUSED(descriptor);

	oph_query_expr_value res;
	res.type = OPH_QUERY_EXPR_TYPE_LONG;
	res.free_flag = 0;
	res.jump_flag = 0;
	if (destroy || !er)
		return res;

	long long id = get_long_value(args[0], er, "oph_id");
	long long size = get_long_value(args[1], er, "oph_id");
	res.data.long_value = 1 + floor((id - 1) / (size));
	return res;
}

oph_query_expr_value oph_id2(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	UNUSED(num_args);
	UNUSED(name);
	UNUSED(destroy);
	UNUSED(descriptor);

	oph_query_expr_value res;
	res.type = OPH_QUERY_EXPR_TYPE_LONG;
	res.free_flag = 0;
	res.jump_flag = 0;
	if (destroy || !er)
		return res;

	long long id = get_long_value(args[0], er, "oph_id2");
	long long size = get_long_value(args[1], er, "oph_id2");
	long long block_size = get_long_value(args[2], er, "oph_id2");
	res.data.long_value = 1 + (id - 1 % block_size) + (floor((id - 1) / (size * block_size))) * block_size;
	return res;
}

oph_query_expr_value oph_id3(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	UNUSED(num_args);
	UNUSED(name);
	UNUSED(destroy);
	UNUSED(descriptor);

	oph_query_expr_value res;
	res.type = OPH_QUERY_EXPR_TYPE_LONG;
	res.free_flag = 0;
	res.jump_flag = 0;
	if (destroy || !er)
		return res;

	long long k = get_long_value(args[0], er, "oph_id3") - 1;
	long long block_size = get_long_value(args[2], er, "oph_id3");
	oph_query_arg *binary = get_binary_value(args[1], er, "oph_id3");

	if (binary == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_ARG_TYPE_ERROR, "oph_id3", "a binary");
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_ARG_TYPE_ERROR, "oph_id3", "a binary");
		*er = -1;
		res.data.long_value = 1;
		return res;
	}

	if (binary->arg == NULL) {
		pmesg(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		logging(LOG_ERROR, __FILE__, __LINE__, OPH_QUERY_ENGINE_LOG_NULL_INPUT_PARAM);
		*er = -1;
		res.data.long_value = 1;
		return res;
	}

	long long *list = (long long *) (binary->arg);
	if (!list) {
		res.data.long_value = 1;
		return res;
	}
	long long new_size = binary->arg_length / sizeof(long long);
	long long jj, reduced_size = 0;
	for (jj = 0; jj < new_size; ++jj)
		reduced_size += list[jj];
	long long row_index = k / block_size;
	long long i, start = 0, stop = 0;
	long long relative_row_index = row_index % reduced_size;

	for (i = 0; i < new_size - 1; ++i) {
		stop += list[i];
		if ((relative_row_index >= start) && (relative_row_index < stop))
			break;
		start = stop;
	}
	res.data.long_value = k % block_size + (i + row_index / reduced_size * new_size) * block_size + 1;
	return res;		// Non 'C'-like indexing
}

oph_query_expr_value oph_id_to_index(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	UNUSED(name);
	UNUSED(destroy);
	UNUSED(descriptor);

	oph_query_expr_value res;
	res.type = OPH_QUERY_EXPR_TYPE_LONG;
	res.free_flag = 0;
	res.jump_flag = 0;
	if (destroy || !er)
		return res;

	long long size;
	long long id = get_long_value(args[0], er, "oph_id_to_index") - 1;
	long long index = id;
	if (id < 0) {
		res.data.long_value = -1;
		return res;
	}
	int counter = 1;
	while (counter < num_args) {
		size = get_long_value(args[counter++], er, "oph_id_to_index");
		index = (id % size);
		id = (id - index) / size;
	}
	res.data.long_value = index + 1;
	return res;
}

oph_query_expr_value oph_id_to_index2(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	UNUSED(num_args);
	UNUSED(name);
	UNUSED(destroy);
	UNUSED(descriptor);

	oph_query_expr_value res;
	res.type = OPH_QUERY_EXPR_TYPE_LONG;
	res.free_flag = 0;
	res.jump_flag = 0;
	if (destroy || !er)
		return res;

	long long id = get_long_value(args[0], er, "oph_id_to_index2");
	long long block_size = get_long_value(args[1], er, "oph_id_to_index2");
	long long size = get_long_value(args[2], er, "oph_id_to_index2");
	res.data.long_value = 1 + ((long long) floor((id - 1) / block_size) % size);
	return res;
}

oph_query_expr_value oph_is_in_subset(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	UNUSED(num_args);
	UNUSED(name);
	UNUSED(destroy);
	UNUSED(descriptor);

	oph_query_expr_value res;
	res.type = OPH_QUERY_EXPR_TYPE_LONG;
	res.free_flag = 0;
	res.jump_flag = 0;
	if (destroy || !er)
		return res;

	long long id = get_long_value(args[0], er, "oph_is_in_subset");
	long long start = get_long_value(args[1], er, "oph_is_in_subset");
	long long step = get_long_value(args[2], er, "oph_is_in_subset");
	long long size = get_long_value(args[3], er, "oph_is_in_subset");
	long long m1 = (id - start) % step;
	res.data.long_value = !m1 && (id >= start) && (id <= size);
	return res;
}

oph_query_expr_value oph_query_generic_long(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	oph_query_expr_value res;
	res.free_flag = 0;
	res.jump_flag = 0;
	res.data.long_value = 1;
	res.type = OPH_QUERY_EXPR_TYPE_LONG;
	if (!er)
		return res;

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Running generic long: %s\n", name);

	if (destroy) {
		oph_query_plugin_deinit(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args);
		return res;
	} else {
		if (!descriptor->initialized) {
			*er = oph_query_plugin_init(&(descriptor->function), &(descriptor->dlh), &(descriptor->initid), &(descriptor->internal_args), name, num_args, args, &(descriptor->aggregate));
			if (!er) {
				return res;
			}
			descriptor->initialized = 1;

			//if aggregate and first group execute clear
			if (descriptor->aggregate) {
				*er = oph_query_plugin_clear(&(descriptor->function), descriptor->dlh, descriptor->initid);
				if (!er) {
					return res;
				}
			}
		}

		if (!descriptor->aggregate) {
			*er = oph_query_plugin_exec(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, name, num_args, args, &res);
			if (*er) {
				return res;
			}
		} else {
			//execute add code
			*er = oph_query_plugin_add(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, num_args, args);
			if (*er) {
				return res;
			}
			//If aggregation function on intermediate row        
			res.jump_flag = 1;

			//if aggregate and last row in group  execute clear
			if (descriptor->clear) {
				*er = oph_query_plugin_exec(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, name, num_args, args, &res);
				if (*er) {
					return res;
				}
				res.jump_flag = 0;

				*er = oph_query_plugin_clear(&(descriptor->function), descriptor->dlh, descriptor->initid);
				if (!er) {
					return res;
				}
				descriptor->clear = 0;
			}
		}

		return res;
	}
}

oph_query_expr_value oph_query_generic_double(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	oph_query_expr_value res;
	res.free_flag = 0;
	res.jump_flag = 0;
	res.data.double_value = 1.0;
	res.type = OPH_QUERY_EXPR_TYPE_DOUBLE;
	if (!er)
		return res;

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Running generic double: %s\n", name);

	if (destroy) {
		oph_query_plugin_deinit(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args);
		return res;
	} else {
		if (!descriptor->initialized) {
			*er = oph_query_plugin_init(&(descriptor->function), &(descriptor->dlh), &(descriptor->initid), &(descriptor->internal_args), name, num_args, args, &(descriptor->aggregate));
			if (!er) {
				return res;
			}
			descriptor->initialized = 1;

			//if aggregate and first group execute clear
			if (descriptor->aggregate) {
				*er = oph_query_plugin_clear(&(descriptor->function), descriptor->dlh, descriptor->initid);
				if (!er) {
					return res;
				}
			}
		}

		if (!descriptor->aggregate) {
			*er = oph_query_plugin_exec(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, name, num_args, args, &res);
			if (*er) {
				return res;
			}
		} else {
			//execute add code
			*er = oph_query_plugin_add(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, num_args, args);
			if (*er) {
				return res;
			}
			//If aggregation function on intermediate row        
			res.jump_flag = 1;

			//if aggregate and last row in group  execute clear
			if (descriptor->clear) {
				*er = oph_query_plugin_exec(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, name, num_args, args, &res);
				if (*er) {
					return res;
				}
				res.jump_flag = 0;

				*er = oph_query_plugin_clear(&(descriptor->function), descriptor->dlh, descriptor->initid);
				if (!er) {
					return res;
				}
				descriptor->clear = 0;
			}
		}
		return res;
	}
}

oph_query_expr_value oph_query_generic_binary(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	oph_query_expr_value res;
	res.free_flag = 0;
	res.jump_flag = 0;
	res.data.binary_value = NULL;
	res.type = OPH_QUERY_EXPR_TYPE_BINARY;
	if (!er)
		return res;

	pmesg(LOG_DEBUG, __FILE__, __LINE__, "Running generic binary: %s\n", name);

	if (destroy) {
		oph_query_plugin_deinit(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args);
		return res;
	} else {
		if (!descriptor->initialized) {
			*er = oph_query_plugin_init(&(descriptor->function), &(descriptor->dlh), &(descriptor->initid), &(descriptor->internal_args), name, num_args, args, &(descriptor->aggregate));
			if (!er) {
				return res;
			}
			descriptor->initialized = 1;

			//if aggregate and first group execute clear
			if (descriptor->aggregate) {
				*er = oph_query_plugin_clear(&(descriptor->function), descriptor->dlh, descriptor->initid);
				if (!er) {
					return res;
				}
			}
		}

		if (!descriptor->aggregate) {
			res.free_flag = 1;
			*er = oph_query_plugin_exec(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, name, num_args, args, &res);
			if (*er) {
				return res;
			}
		} else {
			//execute add code
			*er = oph_query_plugin_add(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, num_args, args);
			if (*er) {
				return res;
			}
			//If aggregation function on intermediate row        
			res.jump_flag = 1;

			//if aggregate and last row in group  execute clear
			if (descriptor->clear) {
				res.free_flag = 1;
				*er = oph_query_plugin_exec(&(descriptor->function), descriptor->dlh, descriptor->initid, descriptor->internal_args, name, num_args, args, &res);
				if (*er) {
					return res;
				}
				res.jump_flag = 0;

				*er = oph_query_plugin_clear(&(descriptor->function), descriptor->dlh, descriptor->initid);
				if (!er) {
					return res;
				}
				descriptor->clear = 0;
			}
		}
		return res;
	}
}

oph_query_expr_value oph_query_generic_string(oph_query_expr_value * args, int num_args, char *name, oph_query_expr_udf_descriptor * descriptor, int destroy, int *er)
{
	UNUSED(num_args);
	UNUSED(name);
	UNUSED(args);

	oph_query_expr_value res;
	res.free_flag = 0;
	res.jump_flag = 0;
	res.data.string_value = "";
	res.type = OPH_QUERY_EXPR_TYPE_STRING;
	if (!er)
		return res;

	//TODO Complete string function
	if (destroy) {
		printf("deinit\n");
		//deinit code
		return res;
	} else {
		if (!descriptor->initialized) {
			printf("init\n");
			//init code (need to initiate all the values in the descriptor)
			descriptor->clear = 1;
			descriptor->initialized = 1;
		}
		//if aggregate and group changed (or first group execute clear)
		if (descriptor->aggregate && descriptor->clear) {
			//execute clear code
			printf("clear\n");
			descriptor->clear = 0;
		}
		//if aggregate execute add after exec
		if (descriptor->aggregate) {
			//execute add code
			printf("add\n");
			return res;
		}
		//paramenters translation code
		//exec code
		printf("exec\n");

		//return value calculated by exec function
		res.data.string_value = "a string";
		return res;
	}
}
