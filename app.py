import os
import threading
from flask import Flask, render_template, request, redirect, url_for, flash
from db import insert_data, get_full_data, get_votes_full_data, get_pred_df,get_perfume_url
from get_prediction import get_prediction
from main import query_catalog_parser, update_data
from log import setup_logging
from models_training import train_model

setup_logging()

app = Flask(__name__)

secret_key = os.getenv('secret_key')
app.secret_key = secret_key


@app.route('/', methods=['GET', 'POST'])
def home():
    catalog_df = get_full_data()
    if request.method == 'POST':
        selected_perfume = request.form.get('selected_perfume')
        if selected_perfume:
            return redirect(url_for('check_perfume', perfume_name=selected_perfume))
    return render_template('index.html', catalog_df=catalog_df)


@app.route('/check/<perfume_name>', methods=['GET', 'POST'])
def check_perfume(perfume_name):
    votes_full_data = get_votes_full_data()
    if perfume_name in votes_full_data.full_name.values:
        previous_vote = votes_full_data[votes_full_data.full_name == perfume_name].vote.values[0]
        message = "You used to like it. If you've changed your mind, vote again" if previous_vote else "You used to dislike it. If you've changed your mind, vote again"
    else:
        perfumes_data = get_pred_df(perfume_name)
        if perfumes_data.empty:
            update_data([get_perfume_url(perfume_name)])
            perfumes_data = get_pred_df(perfume_name)
        prediction = get_prediction(perfumes_data)
        if prediction == 0:
            message = 'You will barely like it...'
        elif prediction == 1:
            message = 'Likely, you would find it quite nice'
        else:
            message = "Can't tell. I need more data"

    return render_template('check.html', perfume_name=perfume_name, message=message)


@app.route('/vote', methods=['GET', 'POST'])
def vote():
    catalog_df = get_full_data()
    if request.method == 'POST':
        perfume_vote_selection = request.form.getlist('perfume_vote_selection')
        vote_type = request.form.get('vote_type')
        vote = True if vote_type == 'like' else False

        for perfume in perfume_vote_selection:
            perfume_id = int(catalog_df[catalog_df['full_name'] == perfume]['perfume_id'].values[0])
            insert_data('my_votes', [[perfume_id, vote]])

        threading.Thread(target=train_model).start()
        flash(f'Your vote for {", ".join(perfume_vote_selection)} is added')
        return redirect(url_for('vote'))

    return render_template('vote.html', catalog_df=catalog_df)


@app.route('/add', methods=['POST'])
def add_perfume():
    new_perfume_names = request.form.get('new_perfume_names')
    if new_perfume_names:
        for perfume_name in [name.strip() for name in new_perfume_names.split(',')]:
            result = query_catalog_parser(perfume_name)
            if result != 'empty':
                if result:
                    flash(f'{perfume_name} has been added')
                else:
                    flash(f'{perfume_name} cannot be added. Please try again in a few minutes')
                    break
            else:
                flash(f"Can't find {perfume_name}")

    return redirect(url_for('home'))


if __name__ == '__main__':
    app.run(debug=True)
