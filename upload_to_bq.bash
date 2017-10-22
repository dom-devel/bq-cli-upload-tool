#!/bin/bash

#python script writes out file location to temporary file
"BQ Single File Upload - dont run by itself.exe"

file_path=$(cat "temp_file_path")
filename=$(cat "temp_filename")
dataset=$(cat "temp_dataset")
table=$(cat "temp_table")
schema=$(cat "temp_schema")

rm temp_file_path
rm temp_filename
rm temp_dataset
rm temp_table
rm temp_schema

gsutil cp $file_path "gs://distilled_data/$filename"
call bq mk $dataset
call bq load --max_bad_records=5 --skip_leading_rows=1 --allow_quoted_newlines --schema $schema $dataset$table "gs://distilled_data/%FILENAME%"