import pandas as pd
from io import StringIO
from database import get_connection

def get_best_email(row):
    for col in ['personal_email', 'work_email', 'additional_email_1', 'additional_email_2', 'additional_email_3']:
        email = row.get(col)
        if pd.notna(email) and email.strip():
            return email.strip().lower()
    return None

def get_best_phone(row):
    for col in ['phone', 'phone_2', 'phone_3', 'phone_4']:
        phone = row.get(col)
        if pd.notna(phone) and phone.strip():
            return phone.strip()
    return None

def import_contact_csv(contents):
    conn = get_connection()
    cursor = conn.cursor()

    df = pd.read_csv(StringIO(contents.decode("utf-8")))
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Normalizar campos
    df['first_name'] = df['first_name'].fillna('').str.strip()
    df['last_name'] = df['last_name'].fillna('').str.strip()
    df['email'] = df.apply(get_best_email, axis=1)
    df['phone'] = df.apply(get_best_phone, axis=1)
    df['company_name'] = df['company'].fillna('Sin Empresa').str.strip()
    df['linkedin'] = df['linkedin_url'].fillna('').str.strip()
    df['job_title'] = df['title'].fillna('').str.strip()
    df['city'] = df['location'].fillna('').str.strip()
    df['country'] = 'Mexico'  # Suponiendo que la mayoría son locales

    rows_inserted = 0
    empresas_insertadas = {}

    try:
        for _, row in df.iterrows():
            if not row['email']:
                continue  # Saltar si no hay email válido

            company_name = row['company_name']
            if company_name not in empresas_insertadas:
                cursor.execute("""
                    SELECT company_id FROM empresas WHERE LOWER(company_name) = LOWER(%s)
                """, (company_name,))
                result = cursor.fetchone()

                if result:
                    company_id = result[0]
                else:
                    cursor.execute("""
                        INSERT INTO empresas (company_name, city, country)
                        VALUES (%s, %s, %s)
                        RETURNING company_id
                    """, (
                        company_name,
                        row['city'],
                        row['country']
                    ))
                    company_id = cursor.fetchone()[0]

                empresas_insertadas[company_name] = company_id
            else:
                company_id = empresas_insertadas[company_name]

            # Verifica si el candidato ya existe
            cursor.execute("""
                SELECT id FROM candidatos WHERE LOWER(email) = LOWER(%s)
            """, (row['email'],))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO candidatos (
                        first_name, last_name, job_title, phone, email, city, country, linkedin, company_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['first_name'],
                    row['last_name'],
                    row['job_title'],
                    row['phone'],
                    row['email'],
                    row['city'],
                    row['country'],
                    row['linkedin'],
                    company_id
                ))
                rows_inserted += 1

                # Aquí podrías agregar keywords desde "headline", "industry" o "summary" si se requiere
                # candidato_id = cursor.lastrowid
                # for kw in extract_keywords(row): ...

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

    return rows_inserted
