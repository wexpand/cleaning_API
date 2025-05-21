import pandas as pd
from io import StringIO
from database import get_connection

def import_recruitcrm_csv(contents):
    conn = get_connection()
    cursor = conn.cursor()

    df = pd.read_csv(StringIO(contents.decode("utf-8")))
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    df = df.rename(columns={
        'title_/_position': 'job_title',
        'current_organization': 'company_name',
        'linkedin_profile': 'linkedin',
        'total_experience': 'experience'
    })

    df['first_name'] = df['candidate_first_name'].str.strip()
    df['last_name'] = df['candidate_last_name'].str.strip()
    df['email'] = df['email'].str.strip().str.lower()
    df['phone'] = df['phone'].astype(str).str.strip()
    df['city'] = df['city'].fillna('').str.strip()
    df['state'] = df['state'].fillna('').str.strip()
    df['country'] = df['country'].fillna('Mexico').str.strip()
    df['company_name'] = df['company_name'].fillna('Sin Empresa').str.strip()

    rows_inserted = 0
    empresas_insertadas = {}

    try:
        for _, row in df.iterrows():
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
                        INSERT INTO empresas (company_name, city, state, country)
                        VALUES (%s, %s, %s, %s)
                        RETURNING company_id
                    """, (
                        company_name,
                        row['city'],
                        row['state'],
                        row['country']
                    ))
                    company_id = cursor.fetchone()[0]

                empresas_insertadas[company_name] = company_id
            else:
                company_id = empresas_insertadas[company_name]

            cursor.execute("""
                SELECT id FROM candidatos WHERE LOWER(email) = LOWER(%s)
            """, (row['email'],))
            existing = cursor.fetchone()

            if not existing:
                cursor.execute("""
                    INSERT INTO candidatos (
                        first_name, last_name, job_title, phone, email,
                        city, country, linkedin, company_id, experience
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    row['first_name'],
                    row['last_name'],
                    row['job_title'] if pd.notna(row['job_title']) else '',
                    row['phone'],
                    row['email'],
                    row['city'],
                    row['country'],
                    row['linkedin'] if pd.notna(row['linkedin']) else '',
                    company_id,
                    str(row['experience']) if pd.notna(row['experience']) else ''
                ))

                candidato_id = cursor.fetchone()[0]
                rows_inserted += 1

                # Insertar skills
                if pd.notna(row['skills']):
                    for kw in [k.strip().lower() for k in row['skills'].split(',') if k.strip()]:
                        cursor.execute("""
                            INSERT INTO candidatos_keywords (candidato_id, keyword)
                            VALUES (%s, %s)
                        """, (candidato_id, kw))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

    return rows_inserted
