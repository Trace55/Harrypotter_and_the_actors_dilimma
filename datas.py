import os, pathlib, google, numpy as np, pandas as pd
from google.cloud import bigquery
import plotly.graph_objects as go
import plotly.express as px


# bigquery_storage is not built into Vertex AI, so we install if necessary.
# try-except an extrememly useful python feature.  Here, we first try to import from bigquery_storage
# If that works, program moves on.  If that fails because it's not installed, we pip install it from https://pypi.org/.
try:
    from google.cloud.bigquery_storage import BigQueryReadClient
except:
    os.system('pip install --upgrade google-cloud-bigquery-storage')
    from google.cloud.bigquery_storage import BigQueryReadClient

# Create connection to BigQuery
cred, proj = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
bqclient   = bigquery.Client(credentials=cred, project=proj)


# Define useful functions to interact with BigQuery
def get_cols(tbl):
    """Get list of columns on tbl"""
    t = bqclient.get_table(tbl)
    return [s.name for s in t.schema]

def run_query(query):
    """Run sql query and return pandas dataframe of results, if any"""
    res = bqclient.query(query).result()
    try:
        return res.to_dataframe()
    except:
        return True

def head(tbl, rows=10):
    """Display the top rows of tbl"""
    query = f'select * from {tbl} limit {rows}'
    df = run_query(query)
    print(df)
    return df

def delete_table(tbl):
    """Delete tbl if it exists"""
    query = f'drop table {tbl}'
    try:
        run_query(query)
    except google.api_core.exceptions.NotFound:
        pass

def load_table(tbl, df=None, query=None, file=None, overwrite=True, preview_rows=0):
    """Load data into tbl either from a pandas dataframe, sql query, or local csv file"""

    if overwrite:
        delete_table(tbl)

    if df is not None:
        job = bqclient.load_table_from_dataframe(df, tbl).result()
    elif query is not None:
        job = bqclient.query(query, job_config=bigquery.QueryJobConfig(destination=tbl)).result()
    elif file is not None:
        with open(file, mode='rb') as f:
            job = bqclient.load_table_from_file(f, tbl, job_config=bigquery.LoadJobConfig(autodetect=True)).result()
    else:
        raise Exception('at least one of df, query, or file must be specified')

    if preview_rows > 0:
        head(tbl, preview_rows)
    return tbl


def subquery(query, indents=1):
    s = '\n' + indents * '    '
    return query.strip().replace('\n', s)

def print_query(query, final_only=False):
    if final_only:
        print(query[-1])
    else:
        for k, q in enumerate(query):
            print(f'stage {k}')
            print(q)
            print('===============================================================================')
rng = np.random.default_rng(42)

db  = 'ambient-odyssey-331623.harry'

def raw_df():
    q = """ select * from(
    select * from(
        select *,
            case 
        when book = 'philosophers_stone' then 1
        when book = 'chamber_of_secrets' then 2
        when book = 'prisoner_of_azkaban' then 3
        end as book_number
            from (
                select *
                from `ambient-odyssey-331623.harry.characters` as A
                join (
                    select name as name_1, book, avg(mentions) as avg_mentions 
                    from `ambient-odyssey-331623.harry.mentions` 
                    group by name, book) as B
                on '%' || B.name_1||'%' like '%' || A.name ||'%'
    ))as A 

    full join (SELECT character, movie_number, count(*) as script_counts
    FROM `ambient-odyssey-331623.harry.script_v1` group by character, movie_number) as B 
    on A.name  like '%' || B.character ||'%' and A.book_number = B.movie_number
    ) as A
    full join
    `ambient-odyssey-331623.harry.screen_times_v1` as B
    on '%' ||B.names||'%' like '%' || A.character ||'%' and A.movie_number = B.movie
    Where Name is not Null order by Name

    """
    df = run_query(q)
    return df
    
def clean_df():
    q = """ select * from(
    select * from(
        select *,
            case 
        when book = 'philosophers_stone' then 1
        when book = 'chamber_of_secrets' then 2
        when book = 'prisoner_of_azkaban' then 3
        end as book_number
            from (
                select *
                from `ambient-odyssey-331623.harry.characters` as A
                join (
                    select name as name_1, book, avg(mentions) as avg_mentions 
                    from `ambient-odyssey-331623.harry.mentions` 
                    group by name, book) as B
                on '%' || B.name_1||'%' like '%' || A.name ||'%'
    ))as A 

    full join (SELECT character, movie_number, count(*) as script_counts
    FROM `ambient-odyssey-331623.harry.script_v1` group by character, movie_number) as B 
    on A.name  like '%' || B.character ||'%' and A.book_number = B.movie_number
    ) as A
    full join
    `ambient-odyssey-331623.harry.screen_times_v1` as B
    on '%' ||B.names||'%' like '%' || A.character ||'%' and A.movie_number = B.movie
    Where Name is not Null order by Name

    """
    df = run_query(q)

    X = (df.drop(columns = ["Id",'Wand',"Loyalty", "Skills", "Patronus","book", 'book_number', 'character', 'names', 'movie',
                            'name_1','Death']))
    for i in X.columns:
        for j in range(X.shape[0]):
            try: X[i].iloc[j] = X[i].iloc[j].replace('\xa0', ' ')
            except: continue

    ######################
    X = X[~X['screen_time_sec'].isna()].reset_index(drop=True)
    X.columns = X.columns.str.lower()
    X['hair_colour'] = X['hair_colour'].replace('Silver| formerly auburn', 'Grey').replace('Blond', 'Blonde').replace('Colourless and balding', 'Bald')
    

    ###############
    #here we need to clean up all the catagorigal columns 
    # Start with job, in the books/movies the def against the D.A shows up more so
    # splits jobs into student, D.A.D.A and other

    X = X.fillna('unknown')
    X['job_grouped'] = X['job']
    X['blood_grouped'] = X['blood_status']
    X['birth_yr'] = 0
    X['eye_colour'] = X['eye_colour'].replace('Bright green', 'Green').replace('Bright brown', 'Brown').replace('Scarlet ', 'Scarlet') 

    odd_births = {'Vincent Crabbe'     : 1980,
                  'Minerva McGonagall' : 1889,
                  'Pomona Sprout'      : 1941,
                  'Quirinus Quirrell'  : 1967,
                  'Sir Nicholas'       : 1450,
                 }

    for i, df in X.iterrows():
        if df['job'].find('Dark Arts') > -1:
            X['job_grouped'].iloc[i] = 'defense against the dark arts professor'
        elif df['job'].find('Student') > -1:
            X['job_grouped'].iloc[i] = 'student'
        else:
            X['job_grouped'].iloc[i] = 'other'
        if df['blood_status'].find('or') > -1:
            X['blood_grouped'].iloc[i] = 'magic (unknown)'

        for word in df['birth'].split():
            if (word.isdigit()) and (int(word)>1880):
                X['birth_yr'].iloc[i] = int(word)

    for key,item in odd_births.items():
        idx = X[X['name'] == key].index
        X.loc[idx, 'birth_yr'] = item
    X = X.drop(columns = ['job', 'blood_status', 'birth'])
    return X

def mentions_animation():
    q = """ select * from(
    select * from(
        select *,
            case 
        when book = 'philosophers_stone' then 1
        when book = 'chamber_of_secrets' then 2
        when book = 'prisoner_of_azkaban' then 3
        end as book_number
            from (
                select *
                from `ambient-odyssey-331623.harry.characters` as A
                join (
                    select name as name_1, book, chapter, mentions
                    from `ambient-odyssey-331623.harry.mentions_chapters` 
                    ) as B
                on '%' || B.name_1||'%' like '%' || A.name ||'%'
    ))as A 

    full join (SELECT character, movie_number, count(*) as script_counts
    FROM `ambient-odyssey-331623.harry.script_v1` group by character, movie_number) as B 
    on A.name  like '%' || B.character ||'%' and A.book_number = B.movie_number
    ) as A
    full join
    `ambient-odyssey-331623.harry.screen_times_v1` as B
    on '%' ||B.names||'%' like '%' || A.character ||'%' and A.movie_number = B.movie
    Where Name is not Null order by Name

    """
    df = run_query(q)

    X = (df.drop(columns = ["Id",'Wand',"Loyalty", "Skills", "Patronus","book", 'book_number', 'character', 'names', 'movie',
                            'name_1','Death']))
    for i in X.columns:
        for j in range(X.shape[0]):
            try: X[i].iloc[j] = X[i].iloc[j].replace('\xa0', ' ')
            except: continue

    ######################
    X = X[~X['screen_time_sec'].isna()].reset_index(drop=True)
    X.columns = X.columns.str.lower()
    X['hair_colour'] = X['hair_colour'].replace('Silver| formerly auburn', 'Grey').replace('Blond', 'Blonde').replace('Colourless and balding', 'Bald')


    ###############
    #here we need to clean up all the catagorigal columns 
    # Start with job, in the books/movies the def against the D.A shows up more so
    # splits jobs into student, D.A.D.A and other

    X = X.fillna('unknown')
    X['job_grouped'] = X['job']
    X['blood_grouped'] = X['blood_status']
    X['birth_yr'] = 0
    X['eye_colour'] = X['eye_colour'].replace('Bright green', 'Green').replace('Bright brown', 'Brown').replace('Scarlet ', 'Scarlet') 

    odd_births = {'Vincent Crabbe'     : 1980,
                  'Minerva McGonagall' : 1889,
                  'Pomona Sprout'      : 1941,
                  'Quirinus Quirrell'  : 1967,
                  'Sir Nicholas'       : 1450,
                 }

    for i, df in X.iterrows():
        if df['job'].find('Dark Arts') > -1:
            X['job_grouped'].iloc[i] = 'defense against the dark arts professor'
        elif df['job'].find('Student') > -1:
            X['job_grouped'].iloc[i] = 'student'
        else:
            X['job_grouped'].iloc[i] = 'other'
        if df['blood_status'].find('or') > -1:
            X['blood_grouped'].iloc[i] = 'magic (unknown)'

        for word in df['birth'].split():
            if (word.isdigit()) and (int(word)>1880):
                X['birth_yr'].iloc[i] = int(word)

    for key,item in odd_births.items():
        idx = X[X['name'] == key].index
        X.loc[idx, 'birth_yr'] = item
    X = X.drop(columns = ['job', 'blood_status', 'birth'])
    X['movie_number'] = X['movie_number'].astype('int')
    X['comb_chapters'] = X['chapter']
    for i,data in X.iterrows():
        if data['movie_number'] == 1:
            X['comb_chapters'].iloc[i] = data['chapter']
        if data['movie_number'] == 2:
            X['comb_chapters'].iloc[i] = data['chapter'] + 17
        if data['movie_number'] == 3:
            X['comb_chapters'].iloc[i] = data['chapter'] + 17 + 19

    X['comb_chapters'] = X['comb_chapters'].astype('int')
    X = X.sort_values(['name','house','comb_chapters'])
    import plotly.express as px

    fig = px.scatter(X, 
                     x="script_counts",
                     y="mentions",
                     size="screen_time_sec",
                     color="house",
                     color_discrete_sequence=['red', 'gold' ,'blue','green', 'black'],
                     category_orders={"house": ['Gryffindor', 'Hufflepuff', 'Ravenclaw','Slytherin','unknown']},
                     animation_frame="comb_chapters",
                       hover_name="name",
                     # log_y=True,
                     range_x=[-10,400],
                     range_y=[-10,100],

                     # size_max=60,
                    )
    fig.update_layout(title=f'Book Changes at 17 & 36',   width=900, height=700 )
    fig.show()