/*
    Ophidia IO Server
    Copyright (C) 2014-2018 CMCC Foundation

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

#include "debug.h"
#include <string.h>
#include <time.h>

#define LOGGING_MAX_STRING 100

int msglevel;			/* the higher, the more messages... */
char *prefix = 0;

#if defined(NDEBUG) && defined(__GNUC__)
/* Nothing. pmesg has been "defined away" in debug.h already. */
#else
void pmesg(int level, const char *source, long int line_number, const char *format, ...)
{
#ifdef NDEBUG
	/* Empty body, so a good compiler will optimise calls
	   to pmesg away */
#else
	va_list args;
	char log_type[10];
	//fprintf(stderr, "MSGLEVEL = %d\n", msglevel);

	int new_msglevel = msglevel % 10;
	if (level > new_msglevel)
		return;

	switch (level) {
		case LOG_ERROR:
			sprintf(log_type, "ERROR");
			break;
		case LOG_INFO:
			sprintf(log_type, "INFO");
			break;
		case LOG_WARNING:
			sprintf(log_type, "WARNING");
			break;
		case LOG_DEBUG:
			sprintf(log_type, "DEBUG");
			break;
		default:
			sprintf(log_type, "UNKNOWN");
			break;
	}
	if (msglevel > 10) {
		time_t t1 = time(NULL);
		char *s = ctime(&t1);
		s[strlen(s) - 1] = 0;	// remove \n
		fprintf(stderr, "[%s][%s][%s][%ld]\t", s, log_type, source, line_number);
	} else {
		fprintf(stderr, "[%s][%s][%ld]\t", log_type, source, line_number);
	}

	va_start(args, format);
	vfprintf(stderr, format, args);
	va_end(args);
#endif				/* NDEBUG */
}
#endif				/* NDEBUG && __GNUC__ */

void logging(int level, const char *source, long int line_number, const char *format, ...)
{
	int new_msglevel = msglevel % 10;
	if (level > new_msglevel)
		return;

	char namefile[LOGGING_MAX_STRING];

	if (prefix)
		snprintf(namefile, LOGGING_MAX_STRING, LOGGING_PATH_PREFIX, prefix);
	else
		snprintf(namefile, LOGGING_MAX_STRING, LOGGING_PATH);
	FILE *log_file;
	if ((log_file = fopen(namefile, "a"))) {
		va_list args;
		char log_type[10];
		//fprintf(stderr, "MSGLEVEL = %d\n", msglevel);

		switch (level) {
			case LOG_ERROR:
				sprintf(log_type, "ERROR");
				break;
			case LOG_INFO:
				sprintf(log_type, "INFO");
				break;
			case LOG_WARNING:
				sprintf(log_type, "WARNING");
				break;
			case LOG_DEBUG:
				sprintf(log_type, "DEBUG");
				break;
			default:
				sprintf(log_type, "UNKNOWN");
				break;
		}
		if (msglevel > 10) {
			time_t t1 = time(NULL);
			char *s = ctime(&t1);
			s[strlen(s) - 1] = 0;	// remove \n
			fprintf(log_file, "[%s][%s][%s][%ld]\t", s, log_type, source, line_number);
		} else {
			fprintf(log_file, "[%s][%s][%ld]\t", log_type, source, line_number);
		}

		va_start(args, format);
		vfprintf(log_file, format, args);
		va_end(args);

		fclose(log_file);
	} else
		pmesg(LOG_ERROR, source, line_number, "Error in opening log file '%s'\n", namefile);
}

void set_log_prefix(char *p)
{
	prefix = p;
}

void set_debug_level(int level)
{
	msglevel = level;
}
