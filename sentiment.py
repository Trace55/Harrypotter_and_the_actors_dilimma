import os, pathlib, google, numpy as np, pandas as pd
from google.cloud import bigquery
import plotly.graph_objects as go
import plotly.express as px
# import dash
# from dash import dcc
# from dash import html
# from dash.dependencies import Input, Output

def make_sentiment_plt():
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

    mv = (['philosophers_stone','chamber_of_secrets', 'prisoner_of_azkaban', 'goblet_of_fire', 
      'order_of_the_phoenix','half_blood_prince', 'deathly_hallows' ])

    cols = get_cols(f"{db}.book_{mv[1]}")

    q = ""
    for i,j in enumerate(mv):
        q += f"""select 
        {(', ').join(cols).lower()},
        {i+1} as book_number,

        from {db}.book_{j} union all\n"""

    q = q[:-10]
    book_df = run_query(q)

    mv = ['philosophers_stone','chamber_of_secrets', 'prisoner_of_azkaban']
    cols = get_cols(f"{db}.movie_{mv[1]}")[1:]

    q = ""
    for i,j in enumerate(mv):
        q += f"""select 
        concat(upper(left(character,1)),'',lower(substring(character,2,char_length(trim(character))))) as character,
        {(', ').join(cols).lower()},
        {i+1} as movie_number,

        from {db}.movie_{j} union all\n"""

    q = q[:-10]
    df = run_query(q)

    for i in range(df.shape[0]):
        df['character'].iloc[i] = df['character'].iloc[i].replace('\n', '')
        df['character'].iloc[i] = df['character'].iloc[i].replace('  ', ' ')
        if df['character'].iloc[i][-1] == ' ':
            df['character'].iloc[i] = df['character'].iloc[i][:-1]
        df['character'].iloc[i] = df['character'].iloc[i].replace('\xa0', ' ')

        d = {'Oiiver'             : 'Oliver wood',
         'Oliver'             : 'Oliver wood',
         'Wood'               : 'Oliver wood',
         'stan shunpike'      : "Stan shunpike",
         'Lockhart'           : 'Gilderoy lockhart',
         'Harry-ron-hermione' : 'All 3',
         'All'                : 'All 3',
         'Ron and harry'      : 'Harry and ron',
         'Tom'                : 'Tom riddle',
         'Vernon'             : 'Uncle vernon'
    }

    for i in range(df.shape[0]):
        try:
            char = d[df['character'].iloc[i]]
            df['character'].iloc[i] = char
        except:
            continue
    df = df.sort_values('character')

    import re
    alphabets= "([A-Za-z])"
    prefixes = "(Mr|St|Mrs|Ms|Dr)[.]"
    suffixes = "(Inc|Ltd|Jr|Sr|Co)"
    starters = "(Mr|Mrs|Ms|Dr|He\s|She\s|It\s|They\s|Their\s|Our\s|We\s|But\s|However\s|That\s|This\s|Wherever)"
    acronyms = "([A-Z][.][A-Z][.](?:[A-Z][.])?)"
    websites = "[.](com|net|org|io|gov)"

    def split_into_sentences(text):
        text = " " + text + "  "
        text = text.replace("\n"," ")
        text = re.sub(prefixes,"\\1<prd>",text)
        text = re.sub(websites,"<prd>\\1",text)
        if "Ph.D" in text: text = text.replace("Ph.D.","Ph<prd>D<prd>")
        text = re.sub("\s" + alphabets + "[.] "," \\1<prd> ",text)
        text = re.sub(acronyms+" "+starters,"\\1<stop> \\2",text)
        text = re.sub(alphabets + "[.]" + alphabets + "[.]" + alphabets + "[.]","\\1<prd>\\2<prd>\\3<prd>",text)
        text = re.sub(alphabets + "[.]" + alphabets + "[.]","\\1<prd>\\2<prd>",text)
        text = re.sub(" "+suffixes+"[.] "+starters," \\1<stop> \\2",text)
        text = re.sub(" "+suffixes+"[.]"," \\1<prd>",text)
        text = re.sub(" " + alphabets + "[.]"," \\1<prd>",text)
        if "”" in text: text = text.replace(".”","”.")
        if "\"" in text: text = text.replace(".\"","\".")
        if "!" in text: text = text.replace("!\"","\"!")
        if "?" in text: text = text.replace("?\"","\"?")
        text = text.replace(".",".<stop>")
        text = text.replace("?","?<stop>")
        text = text.replace("!","!<stop>")
        text = text.replace("<prd>",".")
        sentences = text.split("<stop>")
        sentences = sentences[:-1]
        sentences = [s.strip() for s in sentences]
        return sentences

    from textblob import TextBlob

    ls = []
    for i, data in df.groupby('movie_number'):
        data = data.reset_index(drop=True)
        for j in range(data.shape[0]):
            s = data['sentence'].iloc[j]
            ss = split_into_sentences(s)
            for j in ss:
                ls.append([TextBlob(j).polarity, TextBlob(j).subjectivity, 'movie',i])

    hdf_movies = pd.DataFrame(ls, columns= ['polarity', 'subjectivity', 'media', 'series_number'])

    ls = []
    for i, data in book_df.groupby('book_number'):
        data = data.reset_index(drop=True)
        for j in range(data.shape[0]):
            s = data['script'].iloc[j]
            ss = split_into_sentences(s)
            for j in ss:
                ls.append([TextBlob(j).polarity, TextBlob(j).subjectivity, 'book',i])
    hdf_books = pd.DataFrame(ls, columns= ['polarity', 'subjectivity', 'media', 'series_number'])
    hdf = pd.concat([hdf_books,hdf_movies]).reset_index(drop= True)

    dfs = hdf
 
    dfs['series_nm'] = dfs['series_number']
    bks = (['philosophers_stone','chamber_of_secrets', 'prisoner_of_azkaban', 'goblet_of_fire', 
          'order_of_the_phoenix','half_blood_prince', 'deathly_hallows' ])
    for i in range(dfs.shape[0]):
        dfs['series_nm'].iloc[i] = bks[dfs['series_nm'].iloc[i]-1]
    
    dfs = list(hdf.query("polarity != 0 & subjectivity != 0").groupby(['series_nm',"media"]))

    first_title = dfs[0][0]
    traces = []
    buttons = []
    for i,d in enumerate(dfs):
        visible = [False] * len(dfs)
        visible[i] = True
        name = d[0]
        # print(d[1])
        traces.append(
            px.density_heatmap(d[1], x="polarity", y="subjectivity", range_x= [-1,1], range_y = [0,1]

                     ).update_traces(visible=True if i==0 else False).data[0]
        )
        buttons.append(dict(label=f'{name}',
                            method="update",
                            args=[{"visible":visible},
                                  {"title":f"{name}"}]))

    updatemenus = [{'active':0, "buttons":buttons}]

    fig = go.Figure(data=traces, layout=dict(updatemenus=updatemenus))
    fig.update_layout(title=f'{first_title}', title_x=0.5,   width=900, height=700,
                      xaxis_title="polarity",
                        yaxis_title="subjectivity", )
    fig.show()