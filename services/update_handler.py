from database import get_connection

def update_candidate(data: dict) -> str:
    conn = get_connection()
    cursor = conn.cursor()

    email_lookup = data.get("current_email") or data.get("email")
    if not email_lookup:
        raise ValueError("Se necesita 'email' o 'current_email' para identificar al candidato.")

    allowed_fields = ['first_name', 'last_name', 'phone', 'city', 'country', 'linkedin', 'position', 'email']
    updates = []
    values = []

    for field in allowed_fields:
        if field in data and field != "current_email":
            updates.append(f"{field} = %s")
            values.append(data[field])

    if not updates:
        raise ValueError("No hay campos válidos para actualizar.")

    values.append(email_lookup)

    query = f"""
        UPDATE candidatos
        SET {', '.join(updates)}
        WHERE LOWER(email) = LOWER(%s)
        RETURNING id
    """

    cursor.execute(query, values)
    result = cursor.fetchone()

    conn.commit()
    conn.close()

    if result:
        return "1 registro(s) actualizado(s)"
    else:
        return "0 registros actualizados: email no encontrado"
