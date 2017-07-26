#include "ruby/ruby.h"
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>

#define SUCCESS 1
#define FAILURE 0

static sqlite3 *conn;

static int
_rb_sqlite3_exec(
	VALUE self,
	char *query,
	int (*callback)(void*,int,char**,char**),
	void *callback_arg
);


/*
	callback関数
*/

// デバック用
static int 
print_resp(
      void *get_prm,
      int col_cnt,
      char **row_txt,
      char **col_name
);

// レコードをhash化してarrayにpushしていく
static int
record2hash(
  	void *value_arr,
	int col_cnt,
	char **row_txt,
    char **col_name 
);


static VALUE
rb_sqlite3_open(VALUE self, VALUE filename_str)
{
	char *filename = RSTRING_PTR(filename_str);
	int status = 0;
	conn = NULL;

	// sqlite3への接続
	status = sqlite3_open(
		filename,
		&conn
		);

	if (SQLITE_OK != status) {
		return Qfalse;
	}

	return Qtrue;
}

static VALUE
rb_sqlite3_exec(VALUE self, VALUE query_str)
{
	if (_rb_sqlite3_exec(self, RSTRING_PTR(query_str), print_resp, NULL)) {
		return Qtrue;
	} else {
		return Qfalse;
	}
}

/*
create table テーブル名(id INTEGER) が発行される。
*/

static VALUE
rb_sqlite3_create_table(VALUE self, VALUE table_name_str)
{
	char *table_name = RSTRING_PTR(table_name_str);
	char query[255];
	sprintf(query, "create table %s(id integer)", table_name);
	if (_rb_sqlite3_exec(self, query, NULL, NULL)) {
		return Qtrue;
	} else {
		return Qfalse;
	}
}

static VALUE
rb_sqlite3_add_column(VALUE self, VALUE table_str, VALUE key_str, VALUE type_str)
{
	char *table = RSTRING_PTR(table_str);
	char *key = RSTRING_PTR(key_str);
	char *type = RSTRING_PTR(type_str);

	char query[255];
	sprintf(query, "alter table %s add column %s %s", table, key, type);

	if (_rb_sqlite3_exec(self, query, NULL, NULL)) {
		return Qtrue;
	} else {
		return Qfalse;
	}
}

static VALUE
rb_sqlite3_insert(VALUE self, VALUE table_str, VALUE ary)
{
	char *table = RSTRING_PTR(table_str);
	char query[500];

	sprintf(query, "insert into %s values(", table);

	while (1) {
		VALUE obj = rb_ary_shift(ary);
		char obj_str1[255];
		char obj_str2[255];
		char *obj_str3;
		switch (TYPE(obj)) {
			case T_FIXNUM:
				sprintf(obj_str1, "%d", FIX2INT(obj));
				strcat(query, obj_str1);
				break;

			case T_FLOAT:
				sprintf(obj_str2, "%lf", RFLOAT_VALUE(obj));
				strcat(query, obj_str2);
				break;

			case T_STRING:
				obj_str3 = RSTRING_PTR(obj);
				strcat(query, "'");
				strcat(query, obj_str3);
				strcat(query, "'");
				break;

			default:
				rb_raise(rb_eTypeError, "not valid value");
    			break;
		}

		if (RARRAY_LEN(ary) == 0) {
			break;
		}

		strcat(query, ",");
	}

	strcat(query, ")");

	if (_rb_sqlite3_exec(self, query, NULL, NULL)) {
		return Qtrue;
	} else {
		return Qfalse;
	}
}

static VALUE
rb_sqlite3_select(VALUE self, VALUE table_str, VALUE columns_str, VALUE where_str)
{
	char *table = RSTRING_PTR(table_str);
	char *columns = RSTRING_PTR(columns_str);

	char query[500];
	sprintf(query, "select %s from %s", columns, table);

	if (!NIL_P(where_str)) {
		char *where = RSTRING_PTR(where_str);
		sprintf(query, "%s where %s", query, where);
	}
	
	VALUE arr = rb_ary_new();
	if (_rb_sqlite3_exec(self, query, record2hash, arr)) {
		return arr;
	} else {
		return Qfalse;
	}
}


static VALUE
rb_sqlite3_close(VALUE self)
{
	int status = 0;

	status = sqlite3_close(conn);
	if (SQLITE_OK != status) {
		exit(-1);
	}
	return Qnil;
}

static int
_rb_sqlite3_exec(
	VALUE self,
	char *query,
	int (*callback)(void*,int,char**,char**),
	void *callback_arg
	)
{
	int status = 0;
	char *err_msg = NULL;

	status = sqlite3_exec(
		conn,
		query,
		callback,
		callback_arg,
		&err_msg
		);

	if (SQLITE_OK != status) {
		sqlite3_close(conn);
		sqlite3_free(err_msg);
		return FAILURE;
	}

	return SUCCESS;
}


static int
record2hash(
      void *value_arr   , // sqlite3_exec()の第4引数
      int col_cnt     , // 列数
      char **row_txt  , // 行内容
      char **col_name   // 列名
    )
{
	VALUE hash = rb_hash_new();
	int i;
	for (i = 0; i < col_cnt; i++) {
		VALUE key = rb_str_new_cstr(col_name[i]);
		VALUE val;
		if (row_txt[i] == NULL) {
			val = Qnil;
		} else {
			val = rb_str_new_cstr(row_txt[i]);
		}
		rb_hash_aset(hash, key, val);
	}
	rb_ary_push(value_arr, hash);

  	return 0;
}

static int
print_resp(
      void *get_prm   , // sqlite3_exec()の第4引数
      int col_cnt     , // 列数
      char **row_txt  , // 行内容
      char **col_name   // 列名
    )
{
	int i;
	for (i = 0; i < col_cnt; i++) {
		printf("%s : %s, ", col_name[i], row_txt[i]);
	}
	printf("\n");
  	return 0;
}

void
Init_sqlite3_core(void)
{
	VALUE cSqlite3Core = rb_define_class("Sqlite3Core", rb_cObject);
	rb_define_method(cSqlite3Core, "open", rb_sqlite3_open, 1);
	rb_define_method(cSqlite3Core, "exec", rb_sqlite3_exec, 1);
	rb_define_method(cSqlite3Core, "create_table", rb_sqlite3_create_table, 1);
	rb_define_method(cSqlite3Core, "add_column", rb_sqlite3_add_column, 3);
	rb_define_method(cSqlite3Core, "insert", rb_sqlite3_insert,2);
	rb_define_method(cSqlite3Core, "select", rb_sqlite3_select, 3);
	rb_define_method(cSqlite3Core, "close", rb_sqlite3_close, 0);
}
