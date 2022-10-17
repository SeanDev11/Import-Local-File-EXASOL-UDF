import File_Load_Functions_form_1 as fl

## EXAMPLE - None of these servers, filepaths or tables exist.

file_path = "Public/temp/"

"""
function arguments

1. path_to_files
2. file_type: String that defines input file type. E.g ULF
3. target_table: String that contains the name of the target table in exasol.
4. target_schema: String that contains the name of the target schema in exasol.
5. ZUser: String that contains user id for drive.
6. ZPassword: String that contains password for drive.
7. ExasolUser: String that contains exasol username.
8. ExasolPassword: String that contains exasol password.
9. return: Returns nothing.
"""

fl.import_local_files_exasol(file_path, "csv", "LOAD_CSV_TEST", "SANDBOX",\
                             "USER", "PWD", "EXASOLUSER", "EXASOLPASSWORD")

import os
import pyexasol
import time
import ftputil
import pandas as pd

def import_local_files_exasol(path_to_files, file_type, target_table, target_schema, ZUser, ZPassword, ExasolUser, ExasolPassword):
    """
    :param path_to_files
    :param file_type: String that defines input file type. E.g ULF
    :param target_table: String that contains the name of the target table in exasol.
    :param target_schema: String that contains the name of the target schema in exasol.
    :param ZUser: String that contains user id for drive.
    :param ZPassword: String that contains password for drive.
    :param ExasolUser: String that contains exasol username.
    _param ExasolPassword: String that contains exasol password.
    :return: Returns nothing.
    """
    file_type = file_type.upper()
    csv_target_flag = False
    # Create connection to Exasol
    C = pyexasol.connect(dsn='160.10.10.xxx', user=ExasolUser, password=ExasolPassword, compression=True, schema=target_schema)
    # Check if target table exists
    if not C.meta.table_exists(target_table):
      if file_type == 'ULF':
        # Change this to suit your schema
        C.execute(f"CREATE TABLE {target_schema}.{target_table} ( \
                    FILE_NM varchar(500), \
                    ROWNR int, \
                    BATCHNR int, \
                    CYCLE int, \
                    FC int, \
                    DC_SEG int, \
                    PA_SEG int, \
                    DISPENSED_DATE varchar(60), \
                    DISPENSED_DATE_DAY varchar(60), \
                    UNITS_RPM int, \
                    UNITS_GV int, \
                    UNITS_PV int, \
                    UNITS_SD int, \
                    UNITS_RB int, \
                    UNITS_RT int \
                  )")
        print(f"Target table did not exist so it was created: {target_schema}.{target_table}")
      elif file_type == 'CSV':
        csv_target_flag = True  
       
    # Connect to FTP Server
    host = ftputil.FTPHost('abc.com', ZUser, ZPassword)
    host.use_list_a_option = False
    # Go to target directory
    host.chdir('Root/' + path_to_files)
    # Create list with all file names from directory
    names = host.listdir(host.curdir)
    file_names = []
    for name in names:
      if host.path.isfile(name):
        file_names.append(name)

    # Check filetype
    if file_type == 'ULF':
        # Create temporary table to load file
        try:
          C.execute("CREATE TABLE import_local_files_temp (LINE varchar(1000))")
        except pyexasol.ExaQueryError:
          print("Temp table already exists, dropping then recreating..")
          C.execute("DROP TABLE import_local_files_temp")
          C.execute("CREATE TABLE import_local_files_temp (LINE varchar(1000))")
        
        for x in file_names:
            # Check row seperator
            row_seperator = check_row_seperator(host, x)
            # Remove old data from temp table
            C.execute("TRUNCATE TABLE import_local_files_temp;")
            # Import file
            C.execute("IMPORT INTO {0}.import_local_files_temp \
            FROM CSV AT 'ftp://abc.com/Root/{1}' \
            USER '{2}' IDENTIFIED BY '{3}' FILE '{4}' \
            ENCODING = 'UTF-8' ROW SEPARATOR = '{5}' COLUMN SEPARATOR = ';' \
            COLUMN DELIMITER = '\"' SKIP = 0;".format(target_schema, path_to_files, ZUser, ZPassword, x, row_seperator))

            stmt = C.last_statement()
            print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
            # Remove header/footer
            C.execute("DELETE FROM import_local_files_temp WHERE LINE LIKE '%PS_%'")
            # Transfer data from temp table into target table with desired formatting
            C.execute(f"INSERT INTO {target_table} SELECT '{x}' AS FILE_NM, \
                                                TO_NUMBER(SUBSTR(LINE, 1, 9)) AS ROWNR, \
                                                TO_NUMBER(SUBSTR(LINE,10,5)) AS BATCHNR, \
                                                TO_NUMBER(SUBSTR(LINE, 15, 5)) AS CYCLE, \
                                                TO_NUMBER(SUBSTR(LINE, 20, 7)) AS FC, \
                                                TO_NUMBER(SUBSTR(LINE, 27, 7)) AS DC_SEG, \
                                                TO_NUMBER(SUBSTR(LINE, 35, 7)) AS PA_SEG, \
                                                TRIM(SUBSTR(LINE, 42, 6)) AS DISPENSED_DATE, \
                                                TRIM(SUBSTR(LINE, 48, 2)) AS DISPENSED_DATE_DAY, \
                                                TO_NUMBER(SUBSTR(LINE, 50, 14))/100000 AS UNITS_RPM, \
                                                TO_NUMBER(SUBSTR(LINE, 64, 14))/100000 AS UNITS_GV, \
                                                TO_NUMBER(SUBSTR(LINE, 78, 14))/100000 AS UNITS_PV, \
                                                TO_NUMBER(SUBSTR(LINE, 92, 14))/100000 AS UNITS_SD, \
                                                TO_NUMBER(SUBSTR(LINE, 106, 14))/100000 AS UNITS_RB, \
                                                TO_NUMBER(SUBSTR(LINE, 120, 14))/100000 AS UNITS_RT \
                                                FROM import_local_files_temp;")

        # Delete temp table
        C.execute("DROP TABLE import_local_files_temp;")
      
    elif file_type == 'CSV':
      for x in file_names:
        # Read csv file into pandas dataframe HANDLE UnicodeDecodeError: Try diff encodings
        encoding_ls = ['latin1','utf_16']
        try:
          csv_df = pd.read_csv(f'ftp://{ZUser}:{ZPassword}@abc.com/Root/{path_to_files}{x}', encoding = encoding_ls[0],delimiter=';')
        except UnicodeDecodeError:
          csv_df = pd.read_csv(f'ftp://{ZUser}:{ZPassword}@abc.com/Root/{path_to_files}{x}', encoding = encoding_ls[1],delimiter=';')
        # Check if target table exists -> NOT -> create using schema func
        if csv_target_flag:
          target_schema = create_SQL_schema(csv_df)
          C.execute(f'CREATE TABLE {target_table} ({target_schema})')
          csv_target_flag = False
        # Import into target table
        C.import_from_pandas(csv_df, target_table)
        stmt = C.last_statement()
        print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
        
              
def check_row_seperator(host, file):
  row_seperator = 'CRLF'
  input_file = host.open(file, mode='rb')
  first_ln = input_file.readline()
  if b'\r\n' in first_ln:
    row_seperator = 'CRLF'
  elif b'\n' in first_ln:
    row_seperator = 'LF'
  input_file.close()
  return row_seperator


def create_SQL_schema(pandas_DF):
  """
  Creates an SQL schema as a string.
  
  :param pandas_DF: Pandas dataframe that has desired schema.
  
  Returns an SQL schema as a string.
  """
  
  # Conversions from Python to SQL Types (Add as needed)
  dtype_dict = {'object' : 'VARCHAR(1000)', 'bool' : 'BOOL', 'float64' : 'DOUBLE PRECISION',\
                  'int32' : 'DECIMAL(30,5)', 'int64' : 'decimal(36,10)'}
  
  df_cols = pandas_DF.columns
  df_types = pandas_DF.dtypes
  
  schemaString = "\n"
  
  # Create schema
  if df_cols.size == df_types.size:
  
    comma = ",\n"
  
    for i in range(0,df_types.size):
      if str(df_types[i]) in dtype_dict:
        if i == df_types.size - 1:
          comma = "\n"
        schemaString = schemaString + "      " + df_cols[i] + " " + dtype_dict[str(df_types[i])] + comma
      else:
        print(f"Unknown datatype: {df_types[i]}. Please add to dtype_dict in create_SQL_schema.")

  return schemaString

###################################################
################### Alternative ###################
###################################################
import os
import pyexasol
import time
import ftputil
import pandas as pd

def import_local_files_exasol(path_to_files, file_type, target_table, target_schema, ZUser, ZPassword, ExasolUser, ExasolPassword):
    """
    :param path_to_files
    :param file_type: String that defines input file type. E.g ULF
    :param target_table: String that contains the name of the target table in exasol.
    :param target_schema: String that contains the name of the target schema in exasol.
    :param ZUser: String that contains user id for drive.
    :param ZPassword: String that contains password for drive.
    :param ExasolUser: String that contains exasol username.
    _param ExasolPassword: String that contains exasol password.
    :return: Returns nothing.
    """
    file_type = file_type.upper()
    csv_target_flag = False
    # Create connection to Exasol
    C = pyexasol.connect(dsn='160.00.00.xxxx', user=ExasolUser, password=ExasolPassword, compression=True, schema=target_schema)
    # Check if target table exists
    if not C.meta.table_exists(target_table):
      if file_type == 'ULF':
        C.execute(f"CREATE TABLE {target_schema}.{target_table} ( \
                    SRC varchar(500), \
                    DT_MTH varchar(500), \
                    VERSION varchar(500), \
                    UNIT_TYPE varchar(500), \
                    ROWNR int, \
                    BATCHNR int, \
                    CYCLE int, \
                    FC int, \
                    DC_SEG int, \
                    PA_SEG int, \
                    DISPENSED_DATE varchar(60), \
                    DISPENSED_DATE_DAY varchar(60), \
                    UNITS_RPM int, \
                    UNITS_GV int, \
                    UNITS_PV int, \
                    UNITS_SD int, \
                    UNITS_RB int, \
                    UNITS_RT int \
                  )")
        print(f"Target table did not exist so it was created: {target_schema}.{target_table}")
      elif file_type == 'CSV':
        csv_target_flag = True  
       
    # Connect to FTP Server
    host = ftputil.FTPHost('abc.com', ZUser, ZPassword)
    host.use_list_a_option = False
    # Go to target directory
    host.chdir('Root/' + path_to_files)
    # Create list with all file names from directory
    names = host.listdir(host.curdir)
    file_names = []
    for name in names:
      if host.path.isfile(name):
        file_names.append(name)
    
    if file_type == 'ULF':
        # Create temporary table to load file
        try:
          C.execute("CREATE TABLE import_local_files_temp (LINE varchar(1000))")
        except pyexasol.ExaQueryError:
          print("Temp table already exists, dropping then recreating..")
          C.execute("DROP TABLE import_local_files_temp")
          C.execute("CREATE TABLE import_local_files_temp (LINE varchar(1000))")

        for x in file_names:
            # Check row seperator
            row_seperator = check_row_seperator(host, x)
            # Remove old data from temp table
            C.execute("TRUNCATE TABLE import_local_files_temp;")
            # Import file
            C.execute("IMPORT INTO {0}.import_local_files_temp \
            FROM CSV AT 'ftp://abc.com/Root/{1}' \
            USER '{2}' IDENTIFIED BY '{3}' FILE '{4}' \
            ENCODING = 'UTF-8' ROW SEPARATOR = '{5}' COLUMN SEPARATOR = ';' \
            COLUMN DELIMITER = '\"' SKIP = 0;".format(target_schema, path_to_files, ZUser, ZPassword, x, row_seperator))

            stmt = C.last_statement()
            print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
            # Remove header/footer
            C.execute("DELETE FROM import_local_files_temp WHERE LINE LIKE '%PS_%'")
            # Transfer data from temp table into target table with desired formatting
            C.execute(f"INSERT INTO {target_table} SELECT \
                          '{x}' AS SRC, \
                          SUBSTR('{x}', 18, 6) AS DT_MTH, \
                          'NEW' AS VERSION, \
                                'FULL' AS UNIT_TYPE, \
                          TO_NUMBER(SUBSTR(LINE, 1, 9)) AS ROWNR, \
                                                TO_NUMBER(SUBSTR(LINE,10,5)) AS BATCHNR, \
                                                TO_NUMBER(SUBSTR(LINE, 15, 5)) AS CYCLE, \
                                      TO_NUMBER(SUBSTR(LINE, 20, 7)) AS FC, \
                                      TO_NUMBER(SUBSTR(LINE, 27, 7)) AS DC_SEG, \
                                      TO_NUMBER(SUBSTR(LINE, 35, 7)) AS PA_SEG, \
                                      TRIM(SUBSTR(LINE, 42, 6)) AS DISPENSED_DATE, \
                                      TRIM(SUBSTR(LINE, 48, 2)) AS DISPENSED_DATE_DAY, \
                                        TO_NUMBER(SUBSTR(LINE, 50, 14))/100000 AS UNITS_RPM, \
                                      TO_NUMBER(SUBSTR(LINE, 64, 14))/100000 AS UNITS_GV, \
                          TO_NUMBER(SUBSTR(LINE, 78, 14))/100000 AS UNITS_PV, \
                                      TO_NUMBER(SUBSTR(LINE, 92, 14))/100000 AS UNITS_SD, \
                                      TO_NUMBER(SUBSTR(LINE, 106, 14))/100000 AS UNITS_RB, \
                                    TO_NUMBER(SUBSTR(LINE, 120, 14))/100000 AS UNITS_RT \
                                                FROM import_local_files_temp;")

        # Delete temp table
        C.execute("DROP TABLE import_local_files_temp")
      
    elif file_type == 'CSV':
      for x in file_names:
        # Read csv file into pandas dataframe
        encoding_ls = ['latin1','utf_16']
        try:
          csv_df = pd.read_csv(f'ftp://{ZUser}:{ZPassword}@abc.com/Root/{path_to_files}{x}', encoding = encoding_ls[0],delimiter=';')
        except UnicodeDecodeError:
          csv_df = pd.read_csv(f'ftp://{ZUser}:{ZPassword}@abc.com/Root/{path_to_files}{x}', encoding = encoding_ls[1],delimiter=';')
        # Check if target table exists -> NOT -> create using schema func
        if csv_target_flag:
          target_schema = create_SQL_schema(csv_df)
          C.execute(f'CREATE TABLE {target_table} ({target_schema})')
          csv_target_flag = False
        # Import into target table
        C.import_from_pandas(csv_df, target_table)
        stmt = C.last_statement()
        print(f'IMPORTED {stmt.rowcount()} rows in {stmt.execution_time}s')
        
              
def check_row_seperator(host, file):
  row_seperator = 'CRLF'
  input_file = host.open(file, mode='rb')
  first_ln = input_file.readline()
  if b'\r\n' in first_ln:
    row_seperator = 'CRLF'
  elif b'\n' in first_ln:
    row_seperator = 'LF'
  input_file.close()
  return row_seperator


def create_SQL_schema(pandas_DF):
  """
  Creates an SQL schema as a string.
  
  :param pandas_DF: Pandas dataframe that has desired schema.
  
  Returns an SQL schema as a string.
  """
  
  # Conversions from Python to SQL Types (Add as needed)
  dtype_dict = {'object' : 'VARCHAR(1000)', 'bool' : 'BOOL', 'float64' : 'DOUBLE PRECISION',\
                  'int32' : 'DECIMAL(30,5)', 'int64' : 'decimal(36,10)'}
  
  df_cols = pandas_DF.columns
  df_types = pandas_DF.dtypes
  
  schemaString = "\n"
  
  # Create schema
  if df_cols.size == df_types.size:
  
    comma = ",\n"
  
    for i in range(0,df_types.size):
      if str(df_types[i]) in dtype_dict:
        if i == df_types.size - 1:
          comma = "\n"
        schemaString = schemaString + "      " + df_cols[i] + " " + dtype_dict[str(df_types[i])] + comma
      else:
        print(f"Unknown datatype: {df_types[i]}. Please add to dtype_dict in create_SQL_schema.")

  return schemaString
