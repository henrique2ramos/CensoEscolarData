import sqlite3
import pandas as pd
from flask import Flask, jsonify, request

app = Flask(__name__)
DB_NAME = 'censo_escola.db'

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row 
    return conn

@app.route('/escolas', methods=['GET'])
def get_escolas():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset = (page - 1) * per_page
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM escolas_brasil LIMIT ? OFFSET ?", (per_page, offset))
    escolas = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(escolas)

@app.route('/instituicoesensino/ranking/<int:ano>', methods=['GET'])
def get_ranking(ano):
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset = (page - 1) * per_page

    conn = get_db_connection()
    query = """
        SELECT * FROM escolas_brasil 
        WHERE CAST(nu_ano_censo AS TEXT) LIKE ? 
        ORDER BY CAST(qt_mat_total AS REAL) DESC 
        LIMIT ? OFFSET ?
    """
    df = pd.read_sql_query(query, conn, params=(str(ano), per_page, offset))
    conn.close()

    if df.empty:
        return jsonify({"mensagem": "Fim do ranking ou sem dados"}), 404

    df['nu_ranking'] = range(offset + 1, offset + 1 + len(df))
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)