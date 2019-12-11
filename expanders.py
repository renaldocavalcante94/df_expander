import pandas as pd


def df_list_expansion(df,column_to_expand,column_indexer=None):
    """
    Expand a column with list object in values to other dataframe.
    
    For each object in the list, creates a new line
    
    Parameters
    ----------
    df : dataframe to be expanded
    column: column with the list to be expanded.
    indexer: if we want some indexer to bem maintened to each line expanded. 
    
    Returns
    -------
    New pandas.DataFrame
    With this columns: ['Indexer','Value']
    
    Raises
    ------
    TypeError
        if the df isn`t a Dataframe
    KeyError
        If one of the columns aren't in the dataframe. 
    """
    #Variable to be conveterd in a DataFrame
    value_list = list()
    #Check if the object is a Dataframe
    if type(df) == pd.core.frame.DataFrame:
        #Loop throught the rows of the data frame
        for row in df.itertuples():
            #allocate  avaraible to expand 
            list_to_expand = df.loc[row.Index][column_to_expand]
  
            
            #Try to get the indexer if there is one specified.
            try:
                #Alocate the indexer variable to the fixable indexer,
                indexer = df.loc[row.Index][column_indexer]
            except KeyError:
                #If there is none specified, allocate as none. 
                column_indexer = None
            #Loop throught the objcts in series
            for item in list_to_expand:
                #Create an object to be passed to the list of objects with the indexer or not
                obj = dict()
                
                #Check if the column indexer is none, if is none, the result will be only the expanded values. 
                if column_indexer != None:
                    #Allocate the object indexer 
                    obj['indexer'] = indexer
                #Allocate the value of the lopp throught list in a dict
                obj['value'] = item
                #Apend the object to the dict
                value_list.append(obj)
    else:
        #Raise an error if the df object isn`t a DataFrame
        raise TypeError("df isn't a pandas dataframe")    
    #Return the new Dataframe
    return pd.DataFrame(value_list)

def df_dict_expansion(df,column_to_expand,drop=False,debug=False):
    """
    Expand a column with dict object into new columns to the same dataframe
    
    Every different key in the dicts inside the column choosed to expand will be a new column
    
    Parameters
    ----------
    df : dataframe to be expanded
    column: column with the dict to be expanded
    
    Returns
    -------
    Returns df with the other columns concatenated
        
    Raises
    ------
    TypeError
        if the df isn`t a Dataframe
    KeyError
        If one of the columns aren't in the dataframe. 
    """
    
    #Reset the index of the df to avoid errors
    #df.reset_index()
    #The list of keys that will be turned into new columns
    key_list = list()
    #First iteration over the lines to get the keys


    for row in df.itertuples():
        dict_to_expand = df.loc[row.Index][column_to_expand]
        #Don't save if the value is None
        if dict_to_expand == None:
            continue
        keys = dict_to_expand.keys()
        for key in keys:
            key_list.append(key)
        last_row = row.Index
    keys = pd.Series(key_list).unique()


    
    
    #List of dicts that will be turned into a new DF
    values_list = list()
    #Iterate over the choosed column to be expanded to get the values of the keys that were gathered before
    for row in df[column_to_expand]:
        #dict that will be the line into the new df that will be concatenated
        x = dict()
        #Loop throught the keys to find the values of each key into the dict
        for key in keys:
            try:
                x[key] = row[key]
            #If we dont find an value to the key or the row itself isnt a dict, the value for this column will be None.
            except (KeyError,TypeError):
                x[key] = None
        #Append the new dict
        values_list.append(x)
    #Creates the new expaded DF
    expanded_df = pd.DataFrame(values_list)
    #Concat the initial df and the new df
    expanded_df = pd.concat([df.reset_index(),expanded_df],axis=1).drop(columns='index')
    #If the drop parameter is True drop the column that was expand
    if drop == True:
        return expanded_df.drop(columns=column_to_expand)
    else: 
        #else return the the df with the column
        return expanded_df
        