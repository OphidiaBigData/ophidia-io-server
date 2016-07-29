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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

double oph_id(double* args, int num_args){
	return 1 + floor((args[0] - 1)/(args[1])) ;
}

double oph_id2(double* args, int num_args){
	return 1 + (args[0]-1 %  (int) args[2]) +(floor((args[0]-1)/(args[1]*args[2])))*args[2];
}

double oph_is_in_subset(double* args, int num_args){
	int m1 = (int) (args[0]-args[1]) %  (int) args[2];
	return !m1 && (args[0]>=args[1]) && (args[0]<=args[3]);
}

double oph_id_to_index2(double* args, int num_args){
	return 1 + ((int) floor((args[0]-1)/args[1])% (int) args[2]) ;
}

double sum(double* args, int num_args){
    int sum = 0;
    int i = 0;
    for(;i<num_args;i++){
        sum += args[i];
    }
    return sum;
}

double oph_id_to_index(double *args, int num_args)
{
    double size, id = args[0]-1, index = id;
    if (id < 0) 
    {
        // pmesg(LOG_ERROR, __FILE__, __LINE__, "Invalid value %d\n",id);
        return -1;
    }
    int counter = 1;
    while (counter < num_args)
    {
        size = args[counter++];
        index = (double)((int)id% (int)size);
        id = (id-index)/size;
    }
    return index+1;
}
