from flask import Flask, render_template, url_for, request, redirect
from datetime import datetime
import pandas as pd

app = Flask(__name__)
mtg_formats = ['Standard', 'Pioneer', 'Modern', 'Legacy', 'Commander']

def read_df(format_name : str) -> pd.DataFrame():
    mtg_format = format_name.lower()
    df = pd.read_csv('./static/tables/{mtg_format}_pricing_table.csv'.format(mtg_format=mtg_format))
    df.drop(columns=['run_id', 'run_id.1', 'id'], inplace=True)
    df.drop(columns=df.columns[0], axis=1, inplace=True)
    return df

def create_media_paths(format_name : str):
    media_paths = []
    n_rows = len(read_df(format_name).index)
    for i in range(n_rows):
        tmp_path = "/static/media/{format_name}_place_{number}.png".format(format_name=format_name.lower(), 
                                                                            number=i)
        media_paths.append(tmp_path)
    return media_paths

@app.route('/', methods=['POST', 'GET'])
def index():
    return render_template('index.html', mtg_formats=mtg_formats)

@app.route('/pricings/<format_name>')
def pricings(format_name):
    if format_name not in mtg_formats:
        return redirect('/')
    df = read_df(format_name)
    # TODO scryfall links as hyperlinks on names. Buttom with link/expandable to plots.
    # Data cleaning of decimal values
    scryfall_urls = df['scryfall_uri']
    df.drop(columns=['scryfall_uri'], inplace=True)
    media_paths = create_media_paths(format_name)
    column_names = ['Card Name', 'Set Name', 'Relative Difference (%)', 'Total Difference (USD)', 'New Price (USD)'
                    , 'Old Price (USD)', 'Date Checked-New', 'Date Checked-Old']
    # Data formatting
    df['rel_difference_usd'] = df['rel_difference_usd'].astype(float).round(2)
    df['difference_usd'] = df['difference_usd'].astype(float).round(2)
    return render_template('pricings.html', format_name=format_name, f_name_low=format_name.lower(),
            column_names=column_names, row_data=list(df.values.tolist()), zip=zip,
            media_paths=media_paths)

if __name__ == "__main__":
    app.run(debug=True)