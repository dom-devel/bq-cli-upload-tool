::spawns as a subprocess to avoid console window closing and losing error messages
if not defined in_subprocess (cmd /k set in_subprocess=y ^& %0 %*) & exit )

::python script writes out file location to temporary file
"BQ Single File Upload - dont run by itself.exe"
SET /p FILEUPLOAD=<temp_file_path
SET /p FILENAME=<temp_filename
SET /p DATASET=<temp_dataset
SET /p TABLE=<temp_table
SET /p SCHEMA=<temp_schema
DEL temp_file_path
DEL temp_filename
DEL temp_dataset
DEL temp_table
DEL temp_schema

call gsutil cp %FILEUPLOAD% "gs://distilled_data/%FILENAME%"
call bq mk %DATASET%
call bq load --max_bad_records=5 --skip_leading_rows=1 --allow_quoted_newlines --schema %SCHEMA% %DATASET%%TABLE% "gs://distilled_data/%FILENAME%"
PAUSE