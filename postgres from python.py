import psycopg2


def create_db():
    connection = psycopg2.connect(
        database='postgres',
        user='postgres',
        password='6802425'
    )
    connection.autocommit = True

    with connection.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'netology';")
        exists = cur.fetchone()
        if not exists:
            cur.execute('CREATE DATABASE netology;')
            print("Database has been created successfully")
        else:
            print("Database exists")
    connection.close()


def drop_tables(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS phones;")
        cur.execute("DROP TABLE IF EXISTS clients;")
        conn.commit()


def create_table(conn):
    with conn.cursor() as cur_conn:
        cur_conn.execute("""
        CREATE TABLE IF NOT EXISTS clients(
        PRIMARY KEY (client_id),
        client_id      SERIAL,
        client_name    VARCHAR(30) NOT NULL,
        client_surname VARCHAR(50) NOT NULL,
        client_email   VARCHAR(50) UNIQUE NOT NULL               
        );
        """)

        cur_conn.execute("""
        CREATE TABLE IF NOT EXISTS phones(
        PRIMARY KEY (client_phone),
        client_id      INTEGER REFERENCES clients(client_id),
        client_phone   VARCHAR(16)
        );
        """)
        conn.commit()
        print('Tables has been created successfully')


def add_phone(conn, client_id: int, *phones: str):
    with conn.cursor() as cur:
        for phone in phones:
            cur.execute("""
            INSERT INTO phones(client_id, client_phone)
            VALUES (%s, %s);
            """, (client_id, phone))
            conn.commit()


def add_client(conn, *phones: str, name: str, surname: str, email: str):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients(client_name, client_surname, client_email) 
        VALUES (%s, %s, %s) 
        RETURNING client_id;""", (name, surname, email))
        client_id = cur.fetchone()[0]
        print('Client has been added successfully')
        add_phone(conn, client_id, *phones)


def change_client(conn, client_id: int, name: str = None,
                  surname: str = None, email: str = None,
                  old_phone: str = None, new_phone: str = None):
    with conn.cursor() as cur:
        if name:
            cur.execute("""
            UPDATE clients
               SET client_name = %s
             WHERE client_id = %s;
            """, (name, client_id))
            conn.commit()
        if surname:
            cur.execute("""
            UPDATE clients
               SET client_surname = %s
             WHERE client_id = %s;
            """, (surname, client_id))
            conn.commit()
        if email:
            cur.execute("""
            UPDATE clients
               SET client_email = %s
             WHERE client_id = %s;
            """, (email, client_id))
            conn.commit()
        if old_phone and new_phone:
            cur.execute("""
            UPDATE phones
               SET client_phone = %s 
             WHERE client_id = %s
               AND client_phone = %s;
            """, (new_phone, client_id, old_phone))
            conn.commit()
        print('Client has been changed successfully')


def remove_phone(conn, client_id: int, removed_phone: str):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
        WHERE client_id = %s
          AND client_phone = %s;
        """, (client_id, removed_phone))
        conn.commit()
        print('Phone has been removed successfully')


def remove_client(conn, client_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phones
        WHERE client_id = %s;
        """, (client_id,))
        cur.execute("""
        DELETE FROM clients
        WHERE client_id = %s;
        """, (client_id,))
        conn.commit()
        print('Client has been removed successfully')


def find_client(conn, name: str = None, surname: str = None, email: str = None, phone: str = None):
    """
    - email и phone являются уникальными полями и однозначно идентифицируют клиента
    - при указании name и / или surname функция возвращает список клиентов тезок / полных тезок
    """
    if email or phone:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT s.client_id, s.client_name, s.client_surname
             FROM clients s
                 JOIN phones p
                   ON s.client_id = p.client_id
             WHERE s.client_email = %s
               OR  p.client_phone = %s;
            """, (email, phone))
            print(cur.fetchone())
    elif name and surname:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT s.client_id, s.client_name, s.client_surname
             FROM clients s
             WHERE s.client_name = %s
               AND s.client_surname = %s;
            """, (name, surname))
            print(cur.fetchall())
    elif name or surname:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT s.client_id, s.client_name, s.client_surname
             FROM clients s
             WHERE s.client_name = %s
               OR  s.client_surname = %s;
            """, (name, surname))
            print(cur.fetchall())


if __name__ == '__main__':
    create_db()

with psycopg2.connect(database="netology", user="postgres", password="6802425") as conn:
    drop_tables(conn)
    create_table(conn)
    add_client(conn, *("phone1_client1", "phone2_client1"), name="Name_1", surname="Surname_1",
               email="mail_client1@mail.ru")
    add_client(conn, name="Name_2", surname="Surname_2", email="mail_client2@mail.ru")
    add_client(conn, *("phone1_client3", "phone2_client3"), name="Name_3", surname="Surname_3",
               email="mail_client3@v.ru")
    add_client(conn, *("phone1_client4",), name="Name_4", surname="Surname_4", email="mail_client4@v.ru")
    add_client(conn, name="Name_1", surname="Surname_5", email="mail_client5@mail.ru")
    add_phone(conn, 2, "phone1_client2", "phone2_client2")
    change_client(conn, client_id=1, surname="changed_surname_1")
    change_client(conn, client_id=2, old_phone="phone2_client2", new_phone="new_ph_client2")
    remove_phone(conn, client_id=2, removed_phone="phone1_client2")
    remove_client(conn, client_id=4)
    find_client(conn, email="mail_client3@v.ru")
    find_client(conn, name="Name_1")
conn.close()
