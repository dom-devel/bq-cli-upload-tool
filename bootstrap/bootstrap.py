"""bootstrap.bootstrap: provides entry point main()."""

__version__ = "0.21"
import pandas as pd
import argparse
import tarfile
import zipfile
from pathlib import Path
from shutil import copyfile
from subprocess import PIPE, run
import os
import re
import sys
import shutil
import time
from os import listdir
import json
import logging
import logging.config
from inspect import getsourcefile
from os.path import abspath

if sys.version_info[0]+(sys.version_info[1]/10) < 3.5:
    raise Exception("Python 3.3 or a more recent version is required.")


def setup_logging(
    default_path='logging.json',
    default_level=logging.INFO
):
    """Setup logging configuration

    """
    path = os.path.join(os.path.dirname(abspath(getsourcefile(lambda:0))),
        default_path)
    print(path)
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        print("DOM2")
        logging.basicConfig(level=default_level)


def make_sanitised_filename_copy(folderpath, filename):
    '''
    This function takes a file and creates a copy of it with a sanitised name,
    in the same directory.

    Returns: String of sanitised filename.
    '''
    sanitized_filename = re.sub("[&\*\?\-]", "", filename)
    full_sanitized_path = os.path.join(folderpath, sanitized_filename)
    copyfile(os.path.join(folderpath, filename), full_sanitized_path)
    return [sanitized_filename,full_sanitized_path]


def upload_to_gsc(absolute_path, filename, folderpath, bucket, movecopy):
    '''
    Takes a filename and a bucket and then uses the CLI tool gsutil 
    to upload them into Google Cloud Storage.

    Returns: the name of the file loaded to Google Search Console.
    '''

    final_upload_name = filename
    # Both * and ? are wildcards so need to be removed, - is used in shell for params, 
    # gsutil doesn't like &. (unsure why on the last one.)
    if re.search("[&\*\?\-]", filename):
        logger.info("The filename contains &,*,? or - . A copy of the file will be made without those for upload.")
        try:
            sanitized_path_info = make_sanitised_filename_copy(folderpath, filename)
            logger.info("A copy of the file called {} has been sucessfully created.".format(sanitized_path_info[0]))
        except Exception as e:
            logger.error("The copyfile has failed. Script will now exit.")
            logger.error(e)
            exit()
        upload_to_gsc = [
            'gsutil',
            'mv',
            sanitized_path_info[1],
            'gs://{0}/{1}'.format(bucket, sanitized_path_info[0])
        ]
        final_upload_name = sanitized_path_info[0]
    else:
        upload_to_gsc = [
            'gsutil',
            movecopy,
            absolute_path,
            'gs://{0}/{1}'.format(bucket, filename)
        ]

    output_upload = run_shell_command(upload_to_gsc)

    if output_upload['stderr']:
        if 'Operation completed over' in output_upload['stderr']:
            logger.info(final_upload_name+" was successfully uploaded to "+bucket)
            return final_upload_name
        else:
            logger.info(output_upload['stderr'])
            logger.info("Normally this error fires because the bucket doesn't exist or you don't have access. Buckets you have access to are:")

            get_all_buckets = ['gsutil', 'list']
            output_allprojects = run_shell_command(get_all_buckets)

            logging.info(output_allprojects['stdout'])
            exit(1)

    return final_upload_name


def run_shell_command(command):
    '''
    This function runs a shell command and returns the returncode, stdout and stderr
    in a dictionary.
    '''
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return {
        "args":result.args,
        "code":result.returncode, 
        "stdout":result.stdout, 
        "stderr":result.stderr
    }


def read_csv_to_df(inputfile, args, line_skip, nrows, columns_to_date_guess, has_date=True):
    '''
    This function takes an input file and the config args and then uses it to open
    a CSV format it depending on the arguments provided.

    Returns: A dataframe.
    '''

    # custom_date_needed = False

    sep_var = args.delimiter
    encoding_var=args.encoding
    date_columns = False
    dateparse = None

    if has_date is True:
        if args.timestamp_columns is not "No":
            if columns_to_date_guess is not False:
                logging.info("You have set date guessing to true and set a custom timestamp format. \
                              Only the custom timestamp format will be used.")
            # custom_date_needed = True
            date_columns = args.timestamp_columns
            dateparse = lambda x: pd.to_datetime(x, format=args.timestamp_strptime)
        elif columns_to_date_guess is not False:
            date_columns = columns_to_date_guess
            dateparse = None
    try:
        df = pd.read_csv(inputfile, 
                         parse_dates=date_columns, 
                         date_parser=dateparse, 
                         encoding=encoding_var, 
                         sep=sep_var, 
                         low_memory=False, 
                         skiprows=line_skip, 
                         nrows=nrows)
    except FileNotFoundError as e:
        logging.error("The file {} can't be found, please select an existing file.".format(inputfile))
        exit()
    except ValueError as e:
        if has_date is True:
            logging.error("The strptime string provided for parsing the dates has failed. "
                          "Please check your string. A guide can be found here. http://strftime.org/ "
                          "The script will now exit.")
        logging.error(e)
        exit()

    return df


def format_json_for_upload(filename, folderpath):
    '''
    This function takes a JSON file and outputs it so there is a single object per
    line.

    Returns: The name of the new json file.
    '''

    df = pd.read_json(os.path.join(folderpath,filename))
    new_json_loc = os.path.join(folderpath, "formatted_json_{}".format(filename))
    df.to_json(new_json_loc, orient="records", lines=True)

    return "formatted_json_{}".format(filename)


def upload_to_bq(upload_file_name, original_file_path, args, strict_schema, file_format):
    '''
    This function takes a filename and some config arguments and then calls the
    bq command line utility to upload a file to BigQuery.
    '''
    load_data_bq = [
        'bq',
        'load'
    ]

    standard_params = [
        '--max_bad_records={0}'.format(args.max_bad_records),
        '--source_format={0}'.format(file_format),
        '{0}.{1}'.format(args.dataset, args.table),
        'gs://{0}/{1}'.format(args.bucket, upload_file_name)
    ]

    if file_format == "CSV":
        file_type_params = [
            '--field_delimiter={0}'.format(args.delimiter),
            '--skip_leading_rows={0}'.format(args.line_skip),
        ]
    else:
        file_type_params = []

    if strict_schema:
        schema = [
            '--schema',
            strict_schema
        ]
    else:
        schema = [
            '--autodetect'
        ]

    final_command = load_data_bq + schema + file_type_params + standard_params

    output_load_data = run_shell_command(final_command)

    if 'error in load operation' in output_load_data['stdout'] or "FATAL" in output_load_data['stdout']:
        # Stdout contains the errors, stderr contains the loading messages
        logging.info("FAILURE: {0} was not uploaded into {1}.{2}" 
                     .format(upload_file_name, args.dataset, args.table))
        logging.info(output_load_data['stdout'])
        file_fail_position = re.search("starting at\s(location|position)\s(\d*)",output_load_data['stdout'])
        if file_fail_position:
            fail_byte = file_fail_position.group(1)
            with open(original_file_path, 'r') as f:    
                f.seek(int(fail_byte))
                logging.info("The line which contains the byte {} and is causing the error \
                              above is:".format(fail_byte))
                logging.info(f.readline())
    else:
        logging.info("SUCCESS: {0} was successfully uploaded into {1}.{2}" 
                     .format(upload_file_name, args.dataset, args.table))        


def create_schema(df, args):
    '''
    This function takes a dataframe and returns a BQ compatible schema string. Using
    the data types of pandas columns.

    Returns: A string.
    '''

    schema_series = df.dtypes
    schema_list = []
    date_fields = []
    for k,v in schema_series.iteritems(): 
        key_str = str(k).translate(str.maketrans('','','!@/()%,')).replace(" ","_").replace("-","_")
        key_val = str(v).replace('object','string').replace('int64','integer').replace('float64','float').replace('bool','boolean').replace('datetime64[ns]', 'timestamp')
        array = [key_str,key_val]
        schema_list.append(array)

        if str(v) == "datetime64[ns]":
            date_fields.append(str(k))

    if args.guess_date is True:
        logger.info("The following fields have been set to dates: {}".format(",".join(date_fields)))

    schema = ','.join(':'.join(inner) for inner in schema_list)
    return schema


def get_sane_path(p):
    """
    Function to uniformly return a real, absolute filesystem path.

    https://stackoverflow.com/questions/45169947/how-should-i-get-a-user-to-indicate-a-file-path-to-a-directory-in-a-command-line
    """

    # ~/directory -> /home/user/directory
    p = os.path.expanduser(p)
    # A/.//B -> A/B
    p = os.path.normpath(p)
    # Resolve symbolic links
    p = os.path.realpath(p)
    # Ensure path is absolute
    p = os.path.abspath(p)
    return p


def last_uploaded_file(config_dict):
    '''
    This function saves a config dictionary to a file.
    '''

    output_loc = os.path.join(os.path.dirname(abspath(getsourcefile(lambda:0))),
        "last_uploaded_file.txt")

    file = open(output_loc,"w") 
    json.dump(config_dict, file)
    file.close() 


def setup_bq(args):
    '''
    This function checks if the specified dataset opens.
    '''
    bq_dataset_exists = ['bq', 'ls', args.dataset]
    output_dataset = run_shell_command(bq_dataset_exists)

    if "Not found:" in output_dataset['stdout']:
        create_bq_dataset = ['bq', 'mk', args.dataset]
        output_create_dataset = run_shell_command(create_bq_dataset)
        logging.info(output_create_dataset['stdout'])
    else:
        logging.info("The dataset {} already exists in BQ".format(args.dataset))


def get_non_numeric_columns(file, args, line_skip, nrows):
    '''
    This file takes a file, the program arguments, a line skip number and the number of rows to skip.
    It then opens the file into a dataframe, and returns a list of every column which isn't numeric.

    Returns: A list
    '''
    df_guess = read_csv_to_df(file, args, line_skip, nrows, False)
    columns_to_guess = list(df_guess.select_dtypes(exclude=['float64','int64']).columns.values)

    return columns_to_guess


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def main():   
    # Initialize logging.
    setup_logging()
    global logger

    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="This program will load the selected file into "
                                                 "BigQuery.")
    parser.add_argument("path",
                        help="Specify the source CSV file.")    
    parser.add_argument("bucket",
                        help="Enter the bucket to upload to Google Cloud Storage. If the bucket "
                             "doesn""t exist, it will be created in an American region.")
    parser.add_argument("dataset", 
                        help="Enter the name of the dataset in BigQuery")
    parser.add_argument("table",
                        help="Enter the name of the BigQuery table.")
    parser.add_argument("-p", "--project", default="default", 
                        help="The default gcloud project will be used. Enter the name of the "
                             "Google Cloud Console project, if you want to change. It should look "
                             "like magic-hat-100231")
    parser.add_argument("-ls", "--line_skip",default=1, type=int,
                        help="Enter the number of lines to skip at the top of the file. You need "
                             "to skip column headers. BigQuery will take the last line skipped as "
                             "the column headers.")
    parser.add_argument("-pp", "--pandas_processing", default=False, type=bool, 
                        help="This will open file into a dataframe and perform any custom "
                             "processing selected before saving to a CSV. Takes a boolean.")
    parser.add_argument("-d", "--delimiter",default=",", 
                        help="Bigquery accepts CSV, JSON or Agro. If you have a non-comma "
                             "delimiter, specify it here. If you""re setting a specific file type "
                             "you don""t need this.")
    parser.add_argument("-e", "--encoding",default="utf-8", 
                        help="Requires file processing option. Specify the encoding of the file to "
                             "be uploaded, it will be converted to utf-8 as this is the only "
                             "encoding supported by BigQuery.")
    parser.add_argument("-br", "--max_bad_records",default="0", 
                        help="This will set the maximum number of errors BigQuery will allow "
                             "per file to be uploaded.")
    parser.add_argument("-ss", "--strict_schema", default=False, type=bool, 
                        help="This will take the first 2 lines of the file and use it to specify a "
                             "schema. Useful if BQ is inferring incorrect schema. Takes a boolean.")
    parser.add_argument("-gd", "--guess_date", default=False, type=bool, 
                        help="This will take the first 200 lines of the file and use it to specify a "
                             "schema, it will also attempt to guess any non number fields as dates. Takes a boolean.")
    parser.add_argument("-tc", "--timestamp_columns",default="No", nargs="+", 
                        help="Requires file processing. BQ only recognises certain timestamp "
                             "formats. Enter any columns which have timestamp""s in them.")
    parser.add_argument("-ts", "--timestamp_strptime",default="No", 
                        help="Requires file processing. Provide a strptime string to process "
                             "the dates with. Currently this script doesn""t support files with "
                             "multiple different timestamps.")
    parser.add_argument("-rl", "--reload_uploaded_file",default=False, type=bool,
                        help="Reload uploaded file. When you have uploaded a very large file "
                             "and it has failed to open in BigQuery, this option allows you to just "
                             "retry the load. (Typically used for increasing error threshold). Takes a boolean.")

    args = parser.parse_args()

    # Bucket data validation, if ends or starts in slash remove.
    args.bucket = args.bucket.strip("/")

    # Check for existance of Google Cloud Console.
    command = 'gsutil'
    command2 = 'bq'

    if shutil.which(command) is None:
        logger.info("This script requires gsutil to be installed. The easiest way to get this "
                    "is to install and setup the Google Cloud SDK. Instructions here: "
                    "https://cloud.google.com/sdk/docs/quickstarts")
        exit(1)
    if shutil.which(command2) is None:
        logger.info("This script requires bq to be installed. The easiest way to get this "
                    "is to install and setup the Google Cloud SDK. Instructions here: "
                    "https://cloud.google.com/sdk/docs/quickstarts")
        exit(1)

    logger.info("Both gsutil and bq are installed on this machine. Script will continue.")

    # If project isn't set, get default project and log it
    if args.project == "default":
        get_default_project = ['gcloud','config','list']
        output_default_project = run_shell_command(get_default_project)
        try:
            regex_project = re.search("project = (\S*)", output_default_project['stdout']).group(1)
            args.project = regex_project
        except Exception as e:
            logger.error("There is no default project set in gcloud, run gcloud init and set it up.")
            exit()

        logger.info("The default configured project is {}.".format(regex_project))
    else:
        # Check the project provided exists
        check_project_exists = ['gcloud','projects','describe',args.project]
        output_project_exist = run_shell_command(check_project_exists)

        if output_project_exist['stderr']:
            logger.info(output_project_exist['stderr'])
            logger.info("Available projects are:")

            get_all_projects = ['gcloud','projects','list']
            output_get_projects = run_shell_command(get_all_projects)

            logger.info(output_get_projects['stdout'])
            exit(1)
        else:
            logger.info("The Google Cloud project {} exists".format(args.project))

    if args.reload_uploaded_file is False:
        # Check the bucket provided exists
        check_bucket_exists = ['gsutil','ls','-p',args.project]
        output_bucket_exist = run_shell_command(check_bucket_exists)

        if output_bucket_exist['stdout']:
            if "gs://{}/".format(args.bucket) in output_bucket_exist['stdout']:
                logger.info("The GCS bucket {} exists.".format(args.bucket))
            else:
                create_bucket = ['gsutil','mb','-p', args.project, "gs://{}/".format(args.bucket)]
                output_create_bucket = run_shell_command(create_bucket)
                logger.info("The GCS bucket {} didn't exist and has been created: Standard storage, "
                             "American region.".format(args.bucket))
        else:
            logger.info("Something has gone wrong, stdout and stderr from gsutil logged below.")
            logger.info(output_bucket_exist)
            exit(1)

        # Warn user about uploading a folder
        is_file = True
        if os.path.isdir(args.path):
            logger.info("You have selected a folder to upload. This script will now attempt to upload "
                         "everything in that folder. In case this was a mistake the script will now "
                         "pause for 2 seconds.")
            time.sleep(2)
            is_file = False

        if args.timestamp_columns != "No":
            if args.timestamp_strptime == "No":
                logger.info("A timestamp format must be provided with the timestamp column, please \
                             specify one.")
                exit(1)
            else:
                logger.info("You are setting custom timestamps, but didn\'t enabled pandas "
                             "processing. In order to process a specific timestamp format the file "
                             "must be opened in pandas and so this has been enabled.")
                args.pandas_processing = True

        if args.timestamp_strptime != "No":
            if args.timestamp_columns == "No":
                logger.info("Timestamp column(s) must be provided with the format, please specify them.")
                exit(1)

        # Turn folder or file into list of absolute file paths.
        if is_file is False:
            # Get all files in directory
            file_list = [get_sane_path(os.path.join(args.path,f)) for f in listdir(args.path) if os.path.isfile(os.path.join(args.path, f))]
        else:
            file_list = [get_sane_path(args.path)]

        logger.info("Files to be uploaded: {0}".format(len(file_list)))

        # Iterate through list of files to upload
        for file in file_list:
            filename = os.path.basename(file)
            folderpath = os.path.dirname(file)

            logger.info("The script will attempt to upload: {}".format(filename))

            file_format = "CSV"
            suffixes = Path(filename).suffixes
            non_supported_archive = set([".zip", ".tar"])

            if "xz" in suffixes or "bz2" in suffixes:
                logger.info("BigQuery doesn't support xz or bz2 compression you'll need to "
                            "uncompress before loading with this script.")
                exit()

            # Things that need to be handled with tar
            # Non gzip needs to be decompressed. This will be done automatically by pandas.
            # If there is an intersection then some compression work is needed
            zipped_non_delimit = False
            zip_file_extracted = False
            if set(suffixes) & non_supported_archive:

                if ".tar" in suffixes:
                    # tar = tarfile.open("sample.tar.gz", "w:gz")
                    archive = tarfile.open(file)
                    if len(archive.getmembers()) != 1:
                        logger.info("This script only supports tar archives with a single file inside."
                                    "Please open the archive manually and run on the folder.")
                        exit()
                    else:
                        # Filename is only used for generating new files not loading
                        # so to avoid pandas writing out uncompressed dataframes as .gz files
                        # we set filename here.
                        archived_filename = archive.getmembers()[0].name
                        filename = Path(archived_filename).resolve().stem
                        # Overwrite suffixes with file inside archive as pandas can read
                        # both zip and tar
                        suffixes = Path(archived_filename).suffixes
                else:
                    archive = zipfile.ZipFile(file, "r")
                    if len(archive.infolist()) != 1:
                        logger.info("This script only supports zip archives with a single file inside."
                                    "Please open the archive manually and run on the folder.")
                        exit()
                    else:
                        archived_filename = archive.namelist()[0]
                        filename = Path(archived_filename).resolve().stem
                        suffixes = Path(archived_filename).suffixes

                # If tar won't be opened in pandas then it needs to be opened.
                if args.guess_date is False and args.pandas_processing is False: 
                    logger.info("An archive is being uploaded directly. BigQuery doesn't support archives "
                                "so {} will be extracted and then uploaded.".format(archived_filename))
                    archive.extractall(folderpath)
                    zip_file_extracted = True

                    file = os.path.join(folderpath,archived_filename)
                    filename = archived_filename

            # Create strict schema for BQ if needed
            strict_schema = None
            gcs_mv_or_cp = "cp"

            if ".json" in suffixes or ".avro" in suffixes or zipped_non_delimit is True:
                if ".json" in suffixes:
                    file_format = "NEWLINE_DELIMITED_JSON"
                    gcs_mv_or_cp = "mv"

                    # We have to format and duplicate the JSON file so we rewrite names
                    old_file = file
                    filename = format_json_for_upload(filename,folderpath)
                    file = os.path.join(folderpath, filename)

                    # If file was a zip, then extracted file will need to be removed
                    # once JSON copy is processed.
                    if zip_file_extracted:
                        os.remove(old_file)

                elif ".avro" in suffixes:
                    file_format = "AVRO"

                logger.info("You are uploading a non-CSV file, this means non of the optional "
                            "functionality can be used, the file will uploaded as is or in the case "
                            "of JSON, formatted so there is one object per line and then uploaded.")
                args.pandas_processing = False
            else:
                # To check line skip we try to open the first line of the file in pandas
                open_current_skip = read_csv_to_df(file, args, args.line_skip-1, 2, False)
                open_test_skip = read_csv_to_df(file, args, 10, 2, False, has_date=False)

                if open_current_skip.shape != open_test_skip.shape:
                    logger.error("The number of columns in your file changes if 10 rows are skipped. Your line skip is set incorrectly. Remember the skip should also skip the header row.")
                    exit()

                columns_to_guess = False
                if args.strict_schema is True: 
                    nrows = 2
                    if args.guess_date is True:
                        # There's no reason strict schema can't also be with guessed dates
                        args.pandas_processing = True
                        columns_to_guess = get_non_numeric_columns(file, args, args.line_skip-1, 200)
                    df_sliced_data = read_csv_to_df(file, args, args.line_skip-1, nrows, columns_to_guess)
                    strict_schema = create_schema(df_sliced_data, args) 
                elif args.guess_date is True:
                    # Guessing the dates, means we'll need then open the file in pandas
                    # to output the dates in the correct format.
                    args.pandas_processing = True
                    columns_to_guess = get_non_numeric_columns(file, args, args.line_skip-1, 200)
                    df_sliced_data = read_csv_to_df(file, args, args.line_skip-1, 200, columns_to_guess)
                    strict_schema = create_schema(df_sliced_data, args)

                    logger.info("You are guessing dates, this means pandas_processing will be enabled "
                                 "and the file opened in pandas to allow date formatting into a BigQuery "
                                 "friendly date format. It will attempt to guess dates based on the top "
                                 "200 rows of the file.")

            if args.pandas_processing is False:
                file_upload_name = upload_to_gsc(file, filename, folderpath, args.bucket, gcs_mv_or_cp)
            else:
                df = read_csv_to_df(file,args,args.line_skip-1,None, columns_to_guess) 
                pandas_processed_out = os.path.join(folderpath, "pandas_processed"+filename)

                df.to_csv(pandas_processed_out, 
                          index=False, 
                          date_format='%Y-%m-%d %H:%M:%S', 
                          quoting=1, 
                          encoding='utf-8')

                # If we've opened the file into pandas then we've already performed the line
                # skip and it should be reset to 1.
                args.line_skip = 1
                file_upload_name = upload_to_gsc(pandas_processed_out, filename, folderpath, args.bucket, "mv")

            last_config = {
                "file_upload_name": file_upload_name,
                "file": file,
                "args": vars(args),
                "strict_schema": strict_schema,
                "file_format": file_format
            }

            last_uploaded_file(last_config)
    elif args.reload_uploaded_file is True:
        output_loc = os.path.join(os.path.dirname(abspath(getsourcefile(lambda:0))),
                                  "last_uploaded_file.txt")
        with open(output_loc, 'r') as file:
            saved_args = json.load(file)

        file_upload_name = saved_args['file_upload_name']
        file = saved_args['file']
        strict_schema = saved_args['strict_schema']
        file_format = saved_args['file_format']

        # Overwrite previous settings with new error number
        args_temp = Struct(**saved_args['args'])
        args_temp.max_bad_records = args.max_bad_records
        args = args_temp

        logger.info("Attempting to reload the last loaded file into BigQuery: {}".format(file_upload_name))

    setup_bq(args)
    upload_to_bq(file_upload_name,file, args, strict_schema, file_format)


if __name__ == '__main__':
    main()
