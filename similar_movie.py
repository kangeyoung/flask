from collections import deque, defaultdict
import pymysql
from tabulate import tabulate  # 결과 예쁘게 보기 위함 (옵션)
from typing import Dict, List
import heapq
import math

# db 커넥터
def dbconnect(__name__):
    if __name__ == '__main__':
        DB_HOST = "localhost"
        # DB_HOST = "svc.sel5.cloudtype.app"        
        DB_PORT = 3306
        # DB_PORT = 32089
        DB_USER = "root"
        DB_PASSWORD = "1111"
        DB_DATABASE = "movie"
    elif __name__ == '__test__':
        DB_HOST = "localhost"
        DB_PORT = 3306
        DB_USER = "root"
        DB_PASSWORD = "1111"
        DB_DATABASE = "test"

    try:
        con = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_DATABASE,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return con
    except pymysql.MySQLError as e:
        print(f"DB 연결 오류: {e}")
        return None

# db에서 해당 영화 찾기
def get_movie_by_title(con, movie_name):
    try:
        with con.cursor() as cur:
            sql = "SELECT * FROM movies WHERE korean_title = %s"
            cur.execute(sql, (movie_name,))
            return cur.fetchone()
    except pymysql.MySQLError as e:
        print(f"데이터베이스 작업 오류: {e}")
        return None

# db에서 해당 영화 id로 키워드 리스트 가져오기
def get_keywords_by_movie(con, movie_id):
    try:
        with con.cursor() as cur:
            sql = "SELECT keywords_id FROM movie_keywords WHERE movies_id = %s"
            cur.execute(sql, (movie_id,))
            return [row["keywords_id"] for row in cur.fetchall()]
    except pymysql.MySQLError as e:
        print(f"데이터베이스 작업 오류: {e}")
        return []

# db에서 키워드 2개 이상 같은 영화 리스트 가져오기
def get_similar_movies_with_info(con, keyword_ids, movie_id, min_match_count=1, limit=500):
    try:
        with con.cursor() as cur:
            # 키워드 ID 리스트를 String으로 변환
            format_strings = ','.join(['%s'] * len(keyword_ids))
            sql = f"""
                SELECT DISTINCT m.id AS movieId,
                       m.korean_title,
                       m.poster_path AS poster,
                       r.averageRating AS rating,
                       m.overview AS description,
                       m.keywords AS keywordsRaw,
                       mk.match_count AS keywordsRanking
                FROM movies m
                JOIN ratings r ON r.tconst = m.imdb_id
                JOIN titles_adult ta ON m.imdb_id = ta.tconst
                JOIN (
                    SELECT mk2.movies_id AS movie_id, COUNT(*) AS match_count
                    FROM movie_keywords mk2
                    WHERE mk2.keywords_id IN ({format_strings})
                    GROUP BY mk2.movies_id
                    HAVING match_count >= %s
                ) mk ON m.id = mk.movie_id
                WHERE ta.is_adult = 0
                  AND m.poster_path != ''
                  AND m.overview != ''
                  AND m.id != %s
                ORDER BY mk.match_count DESC
                LIMIT %s
            """
            # 쿼리 실행: 키워드 ID 리스트 + min_match_count + movie_id + limit
            cur.execute(sql, keyword_ids + [min_match_count, movie_id, limit])
            return cur.fetchall()
    except pymysql.MySQLError as e:
        print(f"데이터베이스 작업 오류: {e}")
        return []

# bfs 돌리기     
def bfslogic(data, topNStarts=3, maxDepth=2, recommendSize=30):
    class Movie:
        def __init__(self, movie_data):
            self.movie_id = movie_data.get('movieId', 0)
            self.title = movie_data.get('korean_title', '')
            self.poster = movie_data.get('poster', '')
            self.rating = movie_data.get('rating', 0.0)
            self.description = movie_data.get('description', '')
            # keywordsRaw를 쉼표와 공백으로 분리하여 리스트로 변환
            self.keywords = [keyword.strip() for keyword in movie_data.get('keywordsRaw', '').split(',') if keyword.strip()]
            self.keywords_ranking = movie_data.get('keywordsRanking', 0)

        def to_dict(self):
            """JSON 직렬화를 위해 딕셔너리로 변환"""
            return {
                'movieId': self.movie_id,
                'title': self.title,
                'poster': self.poster,
                'rating': self.rating,
                'description': self.description,
                'keywords': self.keywords,
                'keywordsRanking': self.keywords_ranking
            }

        def __str__(self):
            """객체의 문자열 표현"""
            return f"Movie(id={self.movie_id}, title={self.title}, keywords={self.keywords})"
    
    # 딕셔너리 객체를 movie 객체로 변환    
    movies = []
    for row in data:
        movie = Movie(row)    
        movies.append(movie)
    
    # movieId만 따로 빼내기
    idToMovies = {}
    for movie in movies:
        if(movie.movie_id != None):
            idToMovies[movie.movie_id] = movie
    
    # print(idToMovies[movies[0].movie_id])
    
    # 역인덱스
    keywordToMovies = defaultdict(list)
    for movie in movies:
        keywords = movie.keywords
        if keywords is None:
            continue
        for keyword in keywords:
            keywordToMovies[keyword].append(movie.movie_id)    
    # print(keywordToMovies['fish'])
    
    # 그래프
    graph = defaultdict(lambda: defaultdict(int))
    for movieList in keywordToMovies.values():
        size = len(movieList)
        for i in range(size):
            movieA = movieList[i]
            for j in range(i + 1, size):
                movieB = movieList[j]
                
                # movieA -> movieB
                graph[movieA][movieB] += 1
                # movieB -> movieA (양방향)
                graph[movieB][movieA] += 1
    
    # movies keywordsRanking으로 내림차순
    movies.sort(key=lambda m: m.keywords_ranking, reverse=True) 

    class Node:
        def __init__(self, movie_id, depth, score):
            self.movie_id = movie_id
            self.depth = depth
            self.score = score
        
        def __lt__(self, other):
            return self.score > other.score  # max-heap처럼
            
    # 키워드 랭킹 기준으로 시작 노드 선정
    movies.sort(key=lambda m: m.keywords_ranking, reverse=True)
    start_movies = movies[:topNStarts]

    final_recommendations = []
    visited_global = set()

    for start_movie in start_movies:
        start_id = start_movie.movie_id
        current_movie_total_keywords = len(start_movie.keywords)
        recommendPerStart = recommendSize/topNStarts

        queue = []
        heapq.heappush(queue, (-float('inf'), start_id, Node(start_id, 0, float('inf'))))

        visited_local = set()
        local_recommendations = []

        while queue and len(local_recommendations) < recommendPerStart and len(final_recommendations) < recommendSize:
            _, _, current = heapq.heappop(queue)
            movie_id = current.movie_id
            depth = current.depth

            if movie_id in visited_local or movie_id in visited_global:
                continue
            visited_local.add(movie_id)

            if depth > 0:
                recommended = idToMovies.get(movie_id)
                if recommended:
                    local_recommendations.append(recommended)
                    visited_global.add(movie_id)

            if depth >= maxDepth:
                continue

            neighbors = graph.get(movie_id, {})
            for neighbor_id, keyword_weight in neighbors.items():
                if neighbor_id in visited_local or neighbor_id in visited_global:
                    continue

                neighbor_movie = idToMovies.get(neighbor_id)
                if not neighbor_movie:
                    continue

                avg_keyword_count = (current_movie_total_keywords + len(neighbor_movie.keywords)) / 2.0
                normalized_weight = keyword_weight / avg_keyword_count
                adjusted_ranking = math.log(1 + neighbor_movie.keywords_ranking)
                score = normalized_weight * adjusted_ranking

                heapq.heappush(queue, (-score, neighbor_id, Node(neighbor_id, depth + 1, score)))

        final_recommendations.extend(local_recommendations)
        if len(final_recommendations) >= recommendSize:
            break

    # 6. 최종 정렬: 키워드 랭킹 기준
    final_recommendations.sort(key=lambda m: (m.keywords_ranking), reverse=True)

    return [m.to_dict() for m in final_recommendations]    
        
def get_similar_movies(title):
    con = dbconnect('__main__')
    if not con:
        return

    movie_name = title
    movie = get_movie_by_title(con, movie_name)
    if not movie:
        print("❌ 영화를 찾을 수 없습니다.")
        con.close()
        return

    print(f"🎬 검색한 영화: {movie['korean_title']}")

    keywords = get_keywords_by_movie(con, movie['id'])
    print(f"🔑 관련 키워드 수: {len(keywords)}")

    if not keywords:
        print("❌ 키워드가 없어 비슷한 영화를 찾을 수 없습니다.")
        con.close()
        return

    similar_movies = get_similar_movies_with_info(con, keywords, movie['id'], min_match_count=1, limit=500)
    print(f"🎞️ 키워드 1개 이상 공유하는 영화 수: {len(similar_movies)}")

    if similar_movies:
        print("📋 추천 영화 리스트:")
        # print(tabulate(similar_movies, headers="keys", tablefmt="grid"))
        # print(type(similar_movies[0]))
        # print(similar_movies[0])        
    else:
        print("❌ 추천할 영화가 없습니다.")
        return

    bfs_movies = bfslogic(similar_movies)
    
    if bfs_movies:
        print("bfs 기반 연관 영화")
        # print(bfs_movies[0].title)
        print(len(bfs_movies))
        # for bfs_movie in bfs_movies:
        #     print(bfs_movie)
    return bfs_movies    
    
    con.close()
