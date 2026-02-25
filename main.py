import sqlite3
from flask import Flask, jsonify, request

app = Flask(__name__)
DB_NAME = 'censo_escola.db'

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row 
    return conn

@app.route('/escolas', methods=['GET'])
def get_escolas():
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))
        
        offset = (page - 1) * per_page
        
        conn = get_db_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM escolas_nordeste LIMIT ? OFFSET ?"
        cursor.execute(query, (per_page, offset))
        rows = cursor.fetchall()
        
        escolas = [dict(row) for row in rows]

        cursor.execute("SELECT COUNT(*) FROM escolas_nordeste")
        total_registros = cursor.fetchone()[0]

        conn.close()

        return jsonify({
            "status": "sucesso",
            "pagina_atual": page,
            "por_pagina": per_page,
            "total_registros": total_registros,
            "dados": escolas
        }), 200

    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.route('/escolas/estado/<string:uf>', methods=['GET'])
def get_escolas_por_uf(uf):
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "SELECT * FROM escolas_nordeste WHERE SG_UF = ? LIMIT ? OFFSET ?"
    cursor.execute(query, (uf.upper(), per_page, offset))
    
    escolas = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return jsonify(escolas), 200

if __name__ == '__main__':
    app.run(debug=True)