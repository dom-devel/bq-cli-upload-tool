import pandas as pd
import datetime as datetime
import argparse
import string
import os
from gooey import Gooey, GooeyParser


def remove_sf_bad_lines(inputfile,remove_top_lines):
    o=open("temp_file","w+")
    f=open(inputfile)
    for i in range(remove_top_lines):
    	f.next()

    for line in f:
    	p=line
    if p:
	    o.write(p)
    f.close()
    o.close()

    return "temp_file"


# def slice_file(inputfile):
#     '''
#     ' Takes a file and creates a cut down version which contains the top 
#     ' 100K lines in a file called sliced_file
#     '
#     ' Returns: The name of the temp file -- sliced_file
#     '''
#     sliced_file = open("sliced_file","w")

#     with open(inputfile) as original_file:
#         head = [next(myfile) for x in xrange(100000)]
#         sliced_file.write(head)
#         sliced_file.close()

#     return "sliced_file"     

def read_csv_to_df(inputfile, file_type):

    custom_date_needed = False
    custom_date_and_column_needed = False

    sep_var =','
    encoding_var=None

    if file_type == 'screaming_frog':
        custom_date_needed = True
        dateparse = lambda x: pd.to_datetime(x, format="%a, %d %b %Y %H:%M:%S %Z")
        date_columns = ['Last Modified']
    elif file_type == 'stat_export' or file_type == 'stat_processed_top_20':
        sep_var='\t'
        encoding_var='utf-16'
    elif file_type == 'majestic_export':
        custom_date_needed = True
        dateparse = lambda x: pd.to_datetime(x, format="%Y-%m-%d")
        date_columns = ['Source Crawl Date', 'Source First Found Date']

    if custom_date_needed is True:
        df = pd.read_csv(inputfile, parse_dates=date_columns, date_parser=dateparse, encoding=encoding_var, sep=sep_var)
    else:
        df = pd.read_csv(inputfile, encoding=encoding_var, sep=sep_var)

    if file_type == 'stat_processed_top_20':
        df = df.drop(df.columns[[8,9]], axis=1).rename(columns={df.columns[6]:'result_type'}).rename(columns={df.columns[7]:'ranking_url'})

    return df

def convert_sf_timestamp(date_string):
    date_object = datetime.datetime.strptime(date_string, "%a, %d %b %Y %H:%M:%S")
    date_string_processed = datetime.datetime.strftime(date_object, "%Y-%m-%d %H:%M:%S")
    return date_string_processed

# def convert_timestamp(date_string):
# 	date_object = datetime.datetime.strptime(date_string, "%d-%b-%Y %H:%M:%S")
# 	date_string_processed = datetime.datetime.strftime(date_object, "%Y-%m-%d %H:%M:%S")
# 	return date_string_processed

def create_schema(df):
    '''
    ' This function takes a dataframe and returns a BQ compatible schema string. Using
    ' the data types of pandas columns.
    '
    ' Returns: A string.
    '''

    schema_series = df.dtypes
    schema_list = []
    for k,v in schema_series.iteritems(): 
            key_str = str(k).translate(None, '!@/()%,').replace(" ","_").replace("-","_")
            key_val = str(v).replace('object','string').replace('int64','integer').replace('float64','float').replace('bool','boolean').replace('datetime64[ns]', 'timestamp')
            array = [key_str,key_val]
            schema_list.append(array)

    schema = ','.join(':'.join(inner) for inner in schema_list)
    return schema

@Gooey(required_cols=1,optional_cols=1, default_size=[800,600])
def main():
    parser = GooeyParser(description='This file will load the selected file into BigQuery.')
    parser.add_argument('input',help='Specify the source CSV file.',widget='FileChooser')
    parser.add_argument('line_skip',type=int,help='Enter the number of lines to skip at the top of the file.')
    parser.add_argument('dataset',help='Enter the name of the dataset in BigQuery')
    parser.add_argument('table',help='Enter the name of the BigQuery table.')
    parser.add_argument('--high_mem', default="no", help='Enter high memory mode. Use if you\'re recieving errors about being unable to set dtype.', choices=['yes', 'no'])
    parser.add_argument('--file_type', choices=['screaming_frog','majestic_export','stat_export','stat_processed_top_20'], help='What is the file type? Don\'t touch if not sure.')
    #parser.add_argument('string_of_timestamp',help='Space separated list of date columns.')
    #parser.add_argument('remove_top_lines',help='Number of rows from top to remove. SF is usually 4.')
    #parser.add_argument('--secrets_file', type=str, default='credentials.json', help='Filepath of your Google Client ID and Client Secret')
    args = parser.parse_args()
    input_filename = args.input.split("\\")[-1]
    file_path_without_file = "\\".join(args.input.split("\\")[:-1])

    #Cutting the top 2 lines changes the file name we want to load into DF
    file_for_df_load = input_filename
    if args.line_skip > 0:
        file_for_df_load = remove_sf_bad_lines(args.input, args.line_skip)

    #If high memory then we need to cut down the file and generate the colum types
    if args.high_mem == 1:
        sliced_file(file_for_df_load)
        df_sliced_data = read_csv_to_df(file_for_df_load,args.file_type)
        sliced_dtypes = df_sliced_data.dtypes

        #open dataframe with pre-set dtypes
        df = read_csv_to_df("sliced_file",args.file_type, dtype=sliced_dtypes)
    else:
        df = read_csv_to_df(file_for_df_load,args.file_type)

    schema = create_schema(df).lower()
    
    #there's a chance if the filename is also in the path this will break
    f=open("temp_file_path", "w+")
    f.write('"'+file_path_without_file+"\\processed"+input_filename+'"')
    f.close()

    f=open("temp_dataset", "w+")
    f.write(args.dataset)
    f.close()

    f=open("temp_table", "w+")
    f.write("."+args.table)
    f.close()

    f=open("temp_filename", "w+")
    f.write(input_filename)
    f.close()
    
    f=open("temp_schema", "w+")
    f.write(schema)
    f.close()

    df.to_csv(file_path_without_file+"\\processed"+input_filename, index=False, date_format='%Y-%m-%d %H:%M:%S', quoting=1, encoding='utf-8')

if __name__ == '__main__':
    main()