import pandas as pd
from io import StringIO
from database import get_connection

def parse_employees(val):
    if pd.isnull(val):
        return None
    val = str(val).strip().lower().replace(',', '')
    if 'k' in val:
        try:
            return int(float(val.replace('k', '')) * 1000)
        except:
            return None
    try:
        return int(val)
    except:
        return None

def import_apollo_csv(contents):
    conn = get_connection()
    cursor = conn.cursor()

    # Leer el CSV
    df = pd.read_csv(StringIO(contents.decode("utf-8")))

    # Procesar columnas necesarias
    df['first_name'] = df['Name'].apply(lambda x: x.split()[0] if pd.notnull(x) else '')
    df['last_name'] = df['Name'].apply(lambda x: ' '.join(x.split()[1:]) if pd.notnull(x) and len(x.split()) > 1 else '')
    df['email'] = df['Emails'].apply(lambda x: x.split()[0] if pd.notnull(x) else None)
    df['city'] = df['Location'].apply(lambda x: x.split(',')[0].strip() if pd.notnull(x) and ',' in x else x)
    df['country'] = df['Location'].apply(lambda x: x.split(',')[-1].strip() if pd.notnull(x) and ',' in x else '')
    df['company_name'] = df['Company']
    df['Company · Number of employees'] = df['Company · Number of employees'].apply(parse_employees)

    rows_inserted = 0

    try:
        for index, row in df.iterrows():
            company_name = row['company_name']
            number_of_employees = row.get('Company · Number of employees')
            industry = row.get('Company · Industries')

            # Buscar empresa existente
            cursor.execute("""
                SELECT company_id FROM empresas WHERE LOWER(company_name) = LOWER(%s)
            """, (company_name,))
            result = cursor.fetchone()

            if result:
                company_id = result[0]
            else:
                cursor.execute("""
                    INSERT INTO empresas (company_name, number_of_employees, industry, city, country)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING company_id
                """, (
                    company_name,
                    number_of_employees if pd.notnull(number_of_employees) else None,
                    industry if pd.notnull(industry) else None,
                    row['city'],
                    row['country']
                ))
                company_id = cursor.fetchone()[0]

            # Insertar candidato si no existe por email
            cursor.execute("""
                SELECT id FROM candidatos WHERE LOWER(email) = LOWER(%s)""", (row['email'],))
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO candidatos (
                        first_name, last_name, job_title, phone, email, city, country, linkedin, company_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['first_name'],
                    row['last_name'],
                    row.get('Job title'),
                    row['Phone numbers'] if pd.notnull(row['Phone numbers']) else None,
                    row['email'],
                    row['city'],
                    row['country'],
                    row['Links'] if pd.notnull(row['Links']) else None,
                    company_id
                ))
                rows_inserted += 1

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

    return rows_inserted
