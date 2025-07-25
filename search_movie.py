import pymysql
from pymysql import Error

class Database:
    def __init__(self):
        self.connection = None
        try:
            self.connection = pymysql.connect(
                host='localhost', 
                port=3306,
                database='movie',  # test 데이터베이스 사용
                user='root',
                password='1111',  # mariadb 설치 당시의 패스워드, 실제 환경에서는 보안을 위해 환경변수 등을 사용
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor   # 쿼리 결과를 딕셔너리로 변환
            )
            print("MariaDB에 성공적으로 연결되었습니다.")
        except Error as e:
            print(f"MariaDB 연결 중 오류 발생: {e}")

    def get_movie_list(self, limit=10, offset=0, word=""):
        """영화 리스트 가져오기"""
        try:
            if self.connection is None:
                print("데이터베이스 연결이 없습니다.")
                return []
                
            with self.connection.cursor() as cursor:
                query = f"""
                SELECT 
                    m.id,
                    m.imdb_id,
                    m.korean_title,
                    m.overview,
                    m.poster_path,
                    r.averagerating,
                    r.numVotes,
                    t.is_adult
                FROM movies m
                JOIN ratings r ON m.imdb_id = r.tconst
                JOIN titles_adult t ON m.imdb_id = t.tconst
                WHERE m.korean_title LIKE %s AND t.is_adult = 0
                ORDER BY m.id DESC
                LIMIT {limit} OFFSET {offset}
                """
                
                like_pattern = f"%{word}%"
                cursor.execute(query, (like_pattern,))
                records = cursor.fetchall()
            
            return records
        except Error as e:
            print(f"데이터 조회 중 오류 발생: {e}")
            return []
    
    def get_movie_count(self, word=""):
        try:
            if self.connection is None:
                print("데이터베이스 연결이 없습니다.")
                return 0
                
            with self.connection.cursor() as cursor:
                query = f"""
                SELECT COUNT(*)
                FROM movies m
                JOIN titles_adult t ON m.imdb_id = t.tconst                
                WHERE korean_title LIKE %s AND t.is_adult = 0
                """
                
                like_pattern = f"%{word}%"
                cursor.execute(query, (like_pattern,))
                result = cursor.fetchone()
                print(result)                
            
            return result
        except Error as e:
            print(f"데이터 조회 중 오류 발생: {e}")
            return []

    def close(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            self.connection.close()
            print("MariaDB 연결이 종료되었습니다.")
    
    
        