# ---------------------------------------------------------------------------------------------------------------------
# Baseado em: https://github.com/avishek-choudhary/Music-Store-Analysis/tree/main
# Glenda Proença Train
# ---------------------------------------------------------------------------------------------------------------------
import os
import sys
import json
import pandas as pd
import mysql.connector as mysql
from sqlalchemy import create_engine
# ---------------------------------------------------------------------------------------------------------------------

# Define as relações de cada uma das tabelas
relationships = {
    "artist":   {"p_key": "artist_id", "f_key": None},

    "playlist": {"p_key": "playlist_id", "f_key": None},

    "media_type": {"p_key": "media_type_id", "f_key": None},
    
    "genre": {"p_key": "genre_id", "f_key": None},

    "album":    {"p_key": "album_id", "f_key": [["artist_id", "artist(artist_id)"]]},

    "customer": {"p_key": "customer_id", "f_key": [["support_rep_id", "employee(employee_id)"]]},

    "employee": {"p_key": "employee_id", "f_key": [["reports_to", "employee(employee_id)"]]},

    "invoice": {"p_key": "invoice_id", "f_key": [["customer_id", "customer(customer_id)"]]},

    "invoice_line": {"p_key": "invoice_line_id", "f_key": [["invoice_id", "invoice(invoice_id)"],
                                                           ["track_id", "track(track_id)"]]},

    "playlist_track": {"p_key": None, "f_key": [["playlist_id", "playlist(playlist_id)"],
                                                ["track_id", "track(track_id)"]]},

    "track": {"p_key": "track_id", "f_key": [["album_id", "album(album_id)"],
                                             ["media_type_id", "media_type(media_type_id)"],
                                             ["genre_id", "genre(genre_id)"]]}
}
# ---------------------------------------------------------------------------------------------------------------------

# Função que realiza a conexão no banco de dados
# Retorna o conector caso tenha sucesso
# Caso contrário, termina o programa informando erro
def connect_db(host="mysql", database="sqlproject", credentials_file="credentials.json"):

    # Abre e lê o arquivo de credenciais
    with open(credentials_file) as f:
        credentials = json.load(f)

    # Conecta na base de dados
    connection = mysql.connect(
        user=credentials.get('user'),
        password=credentials.get('password'),
        database=database,
        host=host,
        port="3306"
    )

    # Cria a engine para converter o dataframe para o formato de tabela do MySQL
    engine = create_engine("mysql+mysqlconnector://{}:{}@{}/{}".format(credentials.get('user'),
                                                                       credentials.get('password'),
                                                                       host,
                                                                       database))
    
    # Verifica a conexão
    if connection.is_connected():
        return(connection, engine)
    else:
        print("Erro: Não foi possível realizar a conexão!")
        sys.exit(1)
# ---------------------------------------------------------------------------------------------------------------------

# Função que cria as relações necessárias entre as tabelas
def create_relationships(connection, relationships):
    with connection.cursor() as cursor:
        
        # Percorre todas as tabelas
        for table in relationships:

            # Encontra o nome da coluna que vai ser a chave primária
            p_key = relationships[table]["p_key"]
            
            # Adiciona a chave primária
            if(p_key is not None):
                print("P_key: adicionando a chave primária {}".format(p_key))
                
                # Garante que a coluna vai ser do tipo int
                cursor.execute("ALTER TABLE {} MODIFY {} INT".format(table, p_key))
                cursor.execute("ALTER TABLE {} ADD PRIMARY KEY ({})".format(table, p_key))

        # Percorre todas as tabelas (separado, para não dar conflito de tipo)
        for table in relationships:

            # Chaves estrangeiras
            if(relationships[table]["f_key"] is not None):
                for f_key in relationships[table]["f_key"]:
                    print("F_key: adicionando a chave estrangeira {}".format(f_key[0]))
                    
                    # Garante que a coluna vai ser do tipo int
                    cursor.execute("ALTER TABLE {} MODIFY {} INT".format(table, f_key[0]))

                    # Adiciona a chave estrangeira                        
                    sql_add_fkey = "ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {}".format(table, f_key[0], f_key[1])
                    cursor.execute(sql_add_fkey)
# ---------------------------------------------------------------------------------------------------------------------

# Função que exclui todas as relações e tabelas existentes no banco
def drop_all_tables(connection, relationships):
    with connection.cursor(buffered=True) as cursor:

        # Percorre todas as tabelas removendo a chave estrangeira
        for table in relationships:

            # Descobre as colunas da tabela (se ela existir)
            try:
                cursor.execute("DESCRIBE {}".format(table))
            except:
                continue

            # Para cada coluna, remove a chave estrangeira (se existir)
            rows = cursor.fetchall()
            for row in rows:
                column = row[0]
                # Define a chave estrangeira como várias opções (o banco pode salver como o nome da coluna 
                # ou com o sufixo _ibfk)
                for f_key in [column, table + "_ibfk_1", table + "_ibfk_2", table + "_ibfk_3"]:
                    try:
                        cursor.execute("ALTER TABLE {} DROP FOREIGN KEY {}".format(table, f_key))
                    except:
                        continue

        # Após remover as chaves estrangeiras, faz o drop das tabelas
        for table in relationships:
            try:
                cursor.execute("DROP TABLE {}".format(table))
            except mysql.errors.ProgrammingError:
                continue
# ---------------------------------------------------------------------------------------------------------------------

# Função que resolve os exercícios propostos no GitHub
def solve_questions(connection):
    with connection.cursor(buffered=True) as cursor:

        # Qual é o gênero musical mais popular?
        cursor.execute("SELECT COUNT(track.genre_id) AS popularity, genre.genre_name \
                        FROM track \
                        JOIN genre ON track.genre_id = genre.genre_id \
                        GROUP BY genre.genre_name \
                        ORDER BY popularity DESC LIMIT 1")
        
        rows = cursor.fetchall()
        answer = rows[0][1]
        print("\n1) Qual é o gênero musical mais popular?")
        print("   R: {}".format(answer))

        # Qual é o artista mais popular?
        cursor.execute("SELECT COUNT(invoice_line.quantity) AS purchases, artist.artist_name AS artist_name \
                        FROM invoice_line \
                        INNER JOIN track ON track.track_id = invoice_line.track_id \
                        INNER JOIN album ON track.album_id = album.album_id \
                        INNER JOIN artist ON artist.artist_id = album.artist_id \
                        GROUP BY artist_name \
                        ORDER BY purchases DESC \
                        LIMIT 1")
        
        rows = cursor.fetchall()
        answer = rows[0][1]
        print("\n2) Qual é o artista mais popular?")
        print("   R: {}".format(answer))

        # Qual é a música mais popular?
        cursor.execute("SELECT COUNT(invoice_line.quantity) AS purchases, track.track_name AS track_name \
                        FROM invoice_line \
                        INNER JOIN track ON track.track_id = invoice_line.track_id \
                        GROUP BY track_name \
                        ORDER BY purchases DESC \
                        LIMIT 1")
        
        rows = cursor.fetchall()
        answer = rows[0][1]
        print("\n3) Qual é a música mais popular?")
        print("   R: {}".format(answer))

        # Qual é o preço médio de diferentes tipo de música?
        cursor.execute("WITH purchases AS \
	                    (SELECT SUM(invoice.total) AS total_spent, genre.genre_name AS genre \
	                    FROM invoice \
	                    INNER JOIN invoice_line ON invoice_line.invoice_id = invoice.invoice_id \
	                    INNER JOIN track ON track.track_id = invoice_line.track_id \
	                    INNER JOIN genre ON genre.genre_id = track.genre_id \
	                    GROUP BY genre.genre_name \
                        ORDER BY total_spent) \
                        SELECT genre, CONCAT('$', ROUND(AVG(total_spent))) AS total \
                        FROM purchases \
                        GROUP BY genre")
        
        rows = cursor.fetchall()
        print("\n4) Qual é o preço médio de diferentes tipo de música?")
        for row in rows:
            print("    {}: {}".format(row[0], row[1]))

        # Qual é o país mais popular em compras de música?
        cursor.execute("SELECT SUM(invoice_line.quantity) AS invoice_quantity, customer.country AS country \
                        FROM invoice_line \
                        INNER JOIN invoice ON invoice.invoice_id = invoice_line.invoice_line_id \
                        INNER JOIN customer ON customer.customer_id = invoice.customer_id \
                        GROUP BY country \
                        ORDER BY invoice_quantity DESC \
                        LIMIT 1")
        
        rows = cursor.fetchall()
        answer = rows[0][1]
        print("\n5) Qual é o país mais popular em compras de música?")
        print("   R: {}".format(answer))

        # Quem é o funcionário mais antigo?
        cursor.execute("SELECT first_name, last_name FROM employee \
                        ORDER BY hire_date ASC \
                        LIMIT 1")
        rows = cursor.fetchall()
        first_name = rows[0][0]
        last_name = rows[0][1]
        print("\n6) Quem é o funcionário mais antigo?")
        print("   R: {} {}".format(first_name, last_name))

        # Qual país tem a maior quantidade de faturas?
        cursor.execute("SELECT COUNT(invoice_id) as invoice_quantity, billing_country \
                       FROM invoice \
                       GROUP BY billing_country \
                       ORDER BY invoice_quantity DESC")
        
        rows = cursor.fetchall()
        answer = rows[0][1]
        print("\n7) Qual país tem a maior quantidade de faturas?")
        print("   R: {}".format(answer))

        # Qual é o top 3 em valores da fatura total?
        cursor.execute("SELECT * FROM invoice \
                       ORDER BY total DESC \
                       LIMIT 3")
        rows = cursor.fetchall()
        print("\n8) Qual é o top 3 em valores da fatura total?")
        print("   R:")
        print("   {}".format(rows[0]))
        print("   {}".format(rows[1]))
        print("   {}".format(rows[2]))

        # Qual cidade tem os melhores clientes?
        # Nós gostaríamos de dar um festival promocional na cidade em que conseguimos mais dinheiro.
        # Escreva uma consulta que retorne uma cidade que tenha a maior soma de totais de faturas.
        # Retorne o nome da cidade e a soma de todos os totais da fatura.
        cursor.execute("SELECT billing_city, CONCAT(\"$\", ROUND(SUM(total), 2)) AS invoice_total \
                        FROM invoice \
                        GROUP BY billing_city \
                        ORDER BY SUM(total) DESC \
                        LIMIT 1")
        rows = cursor.fetchall()
        city = rows[0][0]
        invoice_total = rows[0][1]
        print("\n9) Qual cidade tem os melhores clientes?")
        print("   R: {} com um total de faturas de {}".format(city, invoice_total))
        
        # Quem é o melhor cliente? O cliente que gastou mais dinheiro será declarado o melhor cliente.
        # Escreva uma consulta que retorne a pessoa que gastou mais dinheiro.
        cursor.execute("SELECT CONCAT(\"$\", ROUND(SUM(invoice.total), 2)) AS invoice_total, customer.first_name, customer.last_name \
                        FROM invoice \
                        INNER JOIN customer ON customer.customer_id = invoice.customer_id \
                        GROUP BY customer.customer_id \
                        ORDER BY SUM(invoice.total) DESC")
        rows = cursor.fetchall()
        customer_name = rows[0][1] + " " + rows[0][2]
        invoice_total = rows[0][0]
        print("\n10)  Quem é o melhor cliente?")
        print("   R: {} com um total de faturas de {}".format(customer_name, invoice_total))

        # Escreva uma consulta para retornar o e-mail, nome, sobrenome e gênero de todos os ouvintes de rock. 
        # Devolva sua lista ordenada em ordem alfabética por e-mail começando com A.
        cursor.execute("SELECT DISTINCT customer.first_name, customer.last_name, customer.email, genre.genre_name \
                        FROM customer \
                        INNER JOIN invoice ON invoice.customer_id = customer.customer_id \
                        INNER JOIN invoice_line ON invoice_line.invoice_id = invoice.invoice_id \
                        INNER JOIN track ON track.track_id = invoice_line.track_id \
                        INNER JOIN genre ON genre.genre_id = track.genre_id \
                        WHERE genre.genre_name = 'Rock' \
                        ORDER BY customer.email ASC")
        print("\n11) Quais são os emails ordenados dos ouvintes de rock?")
        print("   R:")
        rows = cursor.fetchall()
        for row in rows[:10]:
            print("   {}".format(row))
        print("   ...")


        cursor.execute("SELECT COUNT(track.track_id) AS num_songs, artist.artist_name \
                        FROM track \
                        INNER JOIN album ON album.album_id = track.album_id \
                        INNER JOIN artist ON artist.artist_id = album.artist_id \
                        WHERE genre_id \
	                    IN (SELECT genre.genre_id FROM genre \
		                WHERE genre.genre_name = \"Rock\") \
                        GROUP BY artist.artist_id \
                        ORDER BY num_songs DESC \
                        LIMIT 10;")
        
        # Vamos convidar os artistas que mais escreveram rock em nosso conjunto de dados.
        # Escreva uma consulta que retorne o nome do artista e a contagem total de faixas das 10 principais bandas de rock.
        print("\n12) Quais as 10 principais bandas de rock?")
        print("   R: (Número de Músicas x Bandas)")
        rows = cursor.fetchall()
        for row in rows[:10]:
            print("   {}".format(row))
        print("   ...")

        # Retorna todos os nomes de faixas que possuem uma duração de música maior que a duração média da música.
        # Retorna o nome e os milissegundos de cada faixa. Ordene pela duração da música com as músicas mais longas
        # listadas primeiro.
        cursor.execute("SELECT track.track_name, track.milliseconds FROM track \
                        WHERE track.milliseconds > \
	                        (SELECT AVG(milliseconds) FROM track) \
                        ORDER BY track.milliseconds DESC")
        print("\n13) Quais são as faixas que tem duração maior do que média?")
        print("   R: (Música x Duração(ms))")
        rows = cursor.fetchall()
        for row in rows[:10]:
            print("   {}".format(row))
        print("   ...")

        # Descubra quanto valor gasto por cada cliente com artistas.
        # Escreva uma consulta para retornar o nome do cliente, o nome do artista e o total gasto.
        cursor.execute("WITH artists_name AS \
	                        (SELECT artist.artist_id AS artist_id, \
		                    artist.artist_name AS artist_name \
	                        FROM invoice_line \
	                        JOIN track ON track.track_id = invoice_line.track_id \
	                        JOIN album ON album.album_id = track.album_id \
	                        JOIN artist ON artist.artist_id = album.artist_id \
	                        GROUP BY artist.artist_id) \
                        SELECT customer.customer_id AS customer_id, \
	                    customer.first_name AS first_name, \
	                    artists_name.artist_name AS artist_name, \
	                    SUM(invoice_line.unit_price * invoice_line.quantity) AS total_spent \
                        FROM invoice \
                        JOIN customer ON customer.customer_id = invoice.customer_id \
                        JOIN invoice_line ON invoice_line.invoice_id = invoice.invoice_id \
                        JOIN track ON track.track_id = invoice_line.track_id \
                        JOIN album ON album.album_id = track.album_id \
                        JOIN artists_name ON artists_name.artist_id = album.artist_id \
                        GROUP BY 1, 2, 3 \
                        ORDER BY 4 DESC")
        print("\n14) Quanto cada cliente gastou com cada artista?")
        print("   R: (Id x Primeiro Nome Cliente X Nome do Artist x Total de Gastos)")
        rows = cursor.fetchall()
        for row in rows[:10]:
            print("   {}".format(row))  
        print("   ...")      

        # Queremos descobrir o gênero musical mais popular de cada país.
        # Determinamos o gênero mais popular como o gênero com maior quantidade de compras.
        # Escreva uma consulta que retorne cada país junto com o gênero principal.
        # Para países onde o número máximo de compras é compartilhado, devolva todos os gêneros.
        cursor.execute("WITH popular_genre AS \
                            (SELECT COUNT(invoice_line.quantity) AS purchases, \
		                        customer.country, genre.genre_name AS genre_name, \
                                ROW_NUMBER() OVER(PARTITION BY customer.country \
                            ORDER BY COUNT(invoice_line.quantity) DESC) AS row_num \
                            FROM invoice_line \
                            INNER JOIN invoice ON invoice.invoice_id = invoice_line.invoice_id \
                            INNER JOIN customer ON customer.customer_id = invoice.customer_id \
                            INNER JOIN track ON track.track_id = invoice_line.track_id \
                            INNER JOIN genre ON genre.genre_id = track.genre_id \
                            GROUP BY 2, 3 \
                            ORDER BY 2 ASC, 1 DESC) \
                        SELECT country, genre_name, purchases \
                        FROM popular_genre \
                        WHERE row_num <= 1")
        print("\n15) Qual o gênero musical mais popular em cada país?")
        print("   R: (País x Gênero X Vendas)")
        rows = cursor.fetchall()
        for row in rows[:10]:
            print("   {}".format(row))
        print("   ...") 

        # Escreva uma consulta que determine o cliente que gastou mais em música em cada país.
        # Escreva uma consulta que retorne o país junto com o principal cliente e quanto ele gastou.
        # Para países onde o valor mais gasto é compartilhado, forneça todos os clientes que gastaram esse valor.
        cursor.execute("WITH total_customer_country AS (SELECT customer.first_name, \
	                    billing_country, \
                        SUM(invoice.total) AS total_spent, \
                        ROW_NUMBER() OVER(PARTITION BY billing_country ORDER BY SUM(total) DESC) AS row_num \
                        FROM customer \
                        INNER JOIN invoice ON invoice.customer_id = customer.customer_id \
                        GROUP BY 1, 2 \
                        ORDER BY 2, total_spent DESC) \
                        SELECT first_name, billing_country, total_spent \
                        FROM total_customer_country \
                        WHERE row_num = 1")
        print("\n16) Qual cliente gastou mais em cada país?")
        print("   R: (Nome Cliente x País X Total de Gastos)")
        rows = cursor.fetchall()
        for row in rows[:10]:
            print("   {}".format(row)) 
        print("   ...")
# ---------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__": 

    # Faz a conexão com o banco de dados
    connection, engine = connect_db()

    # Exclui as tabelas do banco, caso existam
    drop_all_tables(connection, relationships)
    
    # Descobre o nome dos csv da pasta
    csv_paths = os.listdir("dataset")

    # Cria uma tabela para cada csv
    for csv_path in csv_paths:
        table_name = csv_path.split(".")[0]
        df = pd.read_csv(os.path.join("dataset", csv_path))   

        # Ajusta tipos específicos para datetime
        if(csv_path == "employee.csv"):
            df["hire_date"] = pd.to_datetime(df["hire_date"], infer_datetime_format=True)
            df["birthdate"] = pd.to_datetime(df["birthdate"], infer_datetime_format=True)
        if(csv_path == "invoice.csv"):
            df["invoice_date"] = pd.to_datetime(df["invoice_date"], infer_datetime_format=True)

        # Cria a tabela
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    # Cria as relações entre as tabelas
    create_relationships(connection, relationships)

    # Resolve as questões mencionadas no readme.md
    solve_questions(connection)
# ---------------------------------------------------------------------------------------------------------------------