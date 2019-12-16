import pandas as pd
import json
import ast
import numpy as np

def get_dtype_series(series):
    """Get the type of a Pnadas Series
    
    Arguments:
        series {pandas.Series} -- pandas.Series to get the dtype
    
    Raises:
        ValueError: Raise value error if there is more than one dtype inside the series
    
    Returns:
        class -- Return the class of the data type 
    """
    dtypes = series.apply(type).unique()

    #Check if there is more than one Data Type
    if len(dtypes) > 1:
        print(f"DataTypes {dtypes}")
        raise ValueError("The series must have only one datatype")
    return dtypes[0]

def get_dtypes_series(series):
    """Same as get_dtype_series but don't raise any error, if there is more 
    than one dtype in the pandas.Series
    
    Arguments:
        series {pandas.Series} -- Series to get all the data types
    
    Returns:
        list -- Returns a list with all classes in the Series
    """
    return series.apply(type).unique()


def str_series_to_dict(series):
    """Serialize a str like a dict series to a series of dict
    
    Arguments:
        series {pandas.Series} -- A pandas.Series with dict like strings inside.
    
    Raises:
        TypeError 1: Raise an TypError if the dtype of the series is already a dict
        TypeError: Raise a TypeError if the dtype of the series isn't a string.
    
    Returns:
        pandas.Series -- A series with all values inside the previous series serialized to dict
    """
    dict_list = list()
    dtype = get_dtype_series(series)
    if dtype == dict:
        raise TypeError("The dtype of the series is already a dict.")
    elif dtype != str:
        raise TypeError("The dtype of the series must be a str")

    for x in series.values:
        x = ast.literal_eval(x)
        dict_list.append(x)
    return pd.Series(dict_list,name=series.name)

def add_prefix_sufix_in_df(df,prefix=None,sufix=None):
    """Add prefix and sufix to the columns of a pandas.DataFrame

    Arguments:
        df {pandas.DataFrame} -- The DataFram which will have the prefix or sufixes added.
    
    Keyword Arguments:
        prefix {str} -- String that will bem on the start of all columns (default: {None})
        sufix {str} -- String that will bem on the end of all columns (default: {None})
    
    Raises:
        TypeError: If any of the two arguments are'n String
        TypeError: If the df isn't a pandas.DataFrame
        AttributeError: If there i no specified sufix and prefix
    
    Returns:
        list -- Returns a column list with all renamed coilumns 
    """
    if type(prefix) != str and prefix != None:
        raise TypeError("prefix argument must be an str")
    elif type(sufix) != str and sufix != None:
        raise TypeError("sufix argument must be an str")
    if type(df) != pd.DataFrame:
        raise TypeError("Df must be an pandas.DataFrame object")

    if prefix != None and sufix == None:
        column_list = (prefix+'_'+x for x in df.columns)
    elif prefix != None and sufix != None:
        column_list = (prefix+'_'+x+'_'+sufix for x in df.columns)
    elif sufix != None and prefix == None:
        column_list = (x+'_'+sufix for x in df.columns)
    else:
        raise AttributeError("This function needs to receive a prefix or a sufix.")

    return column_list

def clean_series_nan(series):
    series =  series.fillna("{}")
    series = series.apply(str)
    return series
   


def df_list_expansion(dataframe,column_to_expand,indexer_column=None,column_name='Values',indexer_name='Indexer'):
    """Expand an series that has lists in his values.

    Arguments:
        dataframe {pandas.DataFrame} -- Datafram that will  expande. 
        column_to_expand {str} -- Column that has the Values to expand
    
    Keyword Arguments:
        indexer_column {str} -- Column to use duplicate as reference from initial df (default: {None})
        column_name {str} -- Column name to the returned df (default: {'Values'})
        indexer_name {str} -- Indexer column name to return in the df (default: {'Indexer'})
    
    Returns:
        pandas.DataFrame -- Return a pandas.DataFrame 
    """
    if indexer_column != None:
        indexers = dataframe[indexer_column].values
    else:
        indexers = np.array(dataframe.index)

    index_array=list()
    values = dataframe[column_to_expand].values
    array = np.c_[indexers,values]
    values_array=list()
    for indexer,values in array:
        values = json.loads(values)
        values = np.array(list(values))
        for value in values:
            index_array.append(indexer)
            values_array.append(value)
    index_array = np.array(index_array)
    values_array = np.array(values_array)
    frame = {indexer_name:index_array,column_name:values_array}
    return pd.DataFrame(frame)

def df_dict_expansion(df,column_name,drop=False,prefix=None,sufix=None):
    """Expand a column with dict in his values to columns a, where every new key in 
    the dict series is a nerw column .
    
    Arguments:
        df {pandas.DataFrame} -- Dataframe to be expanded
        column_name {str} -- Name of the column with the dicts
    
    Keyword Arguments:
        drop {bool} -- If True after expanding will drop the column(default: {False})
        prefix {str} --  String that will be before every new column(default: {None})
        sufix {[type]} -- String that will be after every new column (default: {None})
    
    Returns:
        [pandas.DataFrame] -- Returns a concatenated pandas.Dataframe with the expanded columns and 
        the origin DataFrame.
    """
    values_list = list()
    series = df[column_name]
    series_dtype = get_dtype_series(series)

    if series_dtype != dict:
        series = str_series_to_dict(df[column_name])

    for value in series.values:
        values_list.append(value)

    expanded_df = pd.DataFrame(values_list)

    if prefix != None or sufix != None:
        expanded_df.columns = add_prefix_sufix_in_df(expanded_df,prefix=prefix,sufix=sufix)
    
    if drop == True:
        return pd.concat([df,expanded_df],axis=1).drop(columns=[column_name])

    return pd.concat([df,expanded_df],axis=1)