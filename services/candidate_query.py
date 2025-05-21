from database import get_connection

def search_candidates_by_skills(skills: list[str]) -> list[dict]:
    conn = get_connection()
    cursor = conn.cursor()

    # Crear una consulta para obtener candidatos que tengan todas las skills dadas
    query = '''
        SELECT c.*
        FROM candidatos c
        JOIN candidatos_keywords k ON c.id = k.candidato_id
        WHERE LOWER(k.keyword) = ANY(%s)
        GROUP BY c.id
        HAVING COUNT(DISTINCT LOWER(k.keyword)) = %s;
    '''

    cursor.execute(query, (skills, len(skills)))
    rows = cursor.fetchall()

    # Extraer nombres de columnas
    colnames = [desc[0] for desc in cursor.description]
    conn.close()

    return [dict(zip(colnames, row)) for row in rows]
