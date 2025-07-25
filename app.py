from flask import Flask, render_template, request, redirect, url_for
from similar_movie import get_similar_movies
from search_movie import Database

app = Flask(__name__)   # Flask 앱 초기화

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')
 
@app.route('/search-movie', methods=['POST'])
def search_movie():
    word = request.form['title']
    page = 1
    try:        
        page = int(request.form['page'])
    except KeyError:
        page = 1

    database = Database() 
    
    movies = database.get_movie_list(10, 10 * (page-1), word)
    count = database.get_movie_count(word)
    print(count)
    if (count['COUNT(*)']):
        count = count['COUNT(*)']    
    print(len(movies))
    return render_template('main.html',
                           movies = movies,
                           count=int(count),
                           original = word,
                           page = page)
 
@app.route('/similar-movie', methods=['POST'])
def similar_movie():
    try:
        title = str(request.form['title'])
        print(title)
        
        if (title):
            result = get_similar_movies(title)
            # print(result[0])
            return render_template('result.html', 
                            result = result,
                            original = title)
        
    except ValueError:
        return render_template('index.html', error= "error!")
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
