from flask import Flask, request, jsonify
from db_config import get_db_connection  

app = Flask(__name__)

# 1. GET /authors - List all authors
@app.route('/authors', methods=['GET'])
def get_all_authors():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "Select * from authors"
            cursor.execute(sql)
            authors = cursor.fetchall()  
            if not authors:
                return jsonify({'message': 'No data found'}), 404
            return jsonify(authors), 200
    finally:
        conn.close()

# 2. POST /authors - Add a new author
@app.route('/authors', methods=['POST'])
def add_author():
    data = request.get_json()
    name = data.get('name')
    email = data.get('email')
    bio = data.get('bio')

    if not all([name, email, bio]):
        return jsonify({'error': 'Name, email, and bio are required'}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT id FROM authors WHERE email = %s"
            cursor.execute(sql, (email,))
            if cursor.fetchone():
                return jsonify({'error': 'Email is already in use'}), 400
            
            sql2 = "INSERT INTO authors (name, email, bio) VALUES (%s, %s, %s)"
            cursor.execute(sql2, (name, email, bio))  
            conn.commit() 
            return jsonify({'message': 'Author added successfully'}), 201
    finally:
        conn.close()

# 3. GET /authors/<int:author_id> - Get author's profile and their posts
@app.route('/authors/<int:author_id>', methods=['GET'])
def get_author_with_posts(author_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT * FROM authors WHERE id = %s"
            cursor.execute(sql, (author_id,))
            author = cursor.fetchone()   
            if not author:
                return jsonify({'error': 'Author not found'}), 404
            sql2 = "SELECT id, title, content FROM posts WHERE author_id = %s"
            cursor.execute(sql2, (author_id,))
            posts = cursor.fetchall() 
            if posts:
                author['posts'] = posts
            else:
                author['posts'] = 'Post not found'
            return jsonify(author), 200
    finally:
        conn.close()

# 4. GET /posts - List all posts ordered by created_at DESC
@app.route('/posts', methods=['GET'])
def get_all_posts():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT * FROM posts ORDER BY created_at DESC"
            cursor.execute(sql)
            posts = cursor.fetchall()  
            if not posts:
                return jsonify({'message': 'No data found'}), 404
            return jsonify(posts), 200
    finally:
        conn.close()

# 5. GET /posts/<int:post_id> - Get a single post by ID
@app.route('/posts/<int:post_id>', methods=['GET'])
def get_post(post_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT 
                    p.id AS post_id, p.title, p.content, p.created_at,
                    a.id AS author_id, a.name AS author_name, a.email AS author_email
                FROM posts p
                JOIN authors a ON p.author_id = a.id
                WHERE p.id = %s
            """
            cursor.execute(sql, (post_id,))
            post_data = cursor.fetchone() 
            if not post_data:
                return jsonify({'error': 'Post not found'}), 404
            
            post = {
                'id': post_data['post_id'],
                'title': post_data['title'],
                'content': post_data['content'],
                'created_at': post_data['created_at'],
                'author': {
                    'author_id': post_data['author_id'],
                    'name': post_data['author_name'],
                    'email': post_data['author_email']
                }
            }
            return jsonify(post), 200
    finally:
        conn.close()




# 6. POST /posts - Create a new post
@app.route('/posts', methods=['POST'])
def create_post():
    data = request.get_json()
    title = data.get('title')
    content = data.get('content')
    author_id = data.get('author_id')

    if not all([title, content, author_id]):
        return jsonify({'error': 'Title, content, and author_id are required'}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT id FROM authors WHERE id = %s"
            cursor.execute(sql, (author_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Author not found'}), 404

            sql = "INSERT INTO posts (title, content, author_id) VALUES (%s, %s, %s)"
            cursor.execute(sql, (title, content, author_id)) 
            conn.commit()   
            return jsonify({'message': 'Post created successfully'}), 201
    finally:
        conn.close()

# 7. PUT /posts/<int:post_id> - Update a post by ID
@app.route('/posts/<int:post_id>', methods=['PUT'])
def update_post(post_id):
    data = request.get_json()
    title = data.get('title')
    content = data.get('content')

    if not any([title, content]):
        return jsonify({'error': 'At least one of title or content must be provided'}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = "SELECT id FROM posts WHERE id = %s"
            cursor.execute(sql, (post_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Post not found'}), 404

            fields = []
            values = []
            if title:
                fields.append("title = %s")
                values.append(title)
            if content:
                fields.append("content = %s")
                values.append(content)

            values.append(post_id)
            sql = f"UPDATE posts SET {', '.join(fields)} WHERE id = %s"
            cursor.execute(sql, tuple(values))
            conn.commit()   
            return jsonify({'message': 'Post updated successfully'}), 200
    finally:
        conn.close()

# 8. DELETE /posts/<int:post_id> - Delete a post by ID
@app.route('/posts/<int:post_id>', methods=['DELETE'])
def delete_post(post_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM posts WHERE id = %s", (post_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Post not found'}), 404

            cursor.execute("DELETE FROM posts WHERE id = %s", (post_id,))
            conn.commit() 
            return jsonify({'message': 'Post deleted successfully'}), 200
    finally:
        conn.close()

# 9. GET /posts/by_author/<int:author_id> - Get all posts by a specific author
@app.route('/posts/by_author/<int:author_id>', methods=['GET'])
def get_posts_by_author(author_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM authors WHERE id = %s", (author_id,))
            if cursor.fetchone() is None:
                return jsonify({'error': 'Author not found'}), 404

            cursor.execute("SELECT * FROM posts WHERE author_id = %s", (author_id,))
            posts = cursor.fetchall()  
            return jsonify(posts), 200
    finally:
        conn.close()

# 10. GET /stats/posts/count - Get the total number of posts and the number of authors
@app.route('/stats/posts/count', methods=['GET'])
def get_post_count():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS total_posts FROM posts")
            total_posts = cursor.fetchone()['total_posts']  

            cursor.execute("SELECT COUNT(*) AS total_authors FROM authors")
            total_authors = cursor.fetchone()['total_authors']  

            return jsonify({
                'total_posts': total_posts,
                'total_authors': total_authors
            }), 200
    finally:
        conn.close()


if __name__ == '__main__':
    app.run(debug=True)
