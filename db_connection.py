#-------------------------------------------------------------------------
# AUTHOR: Nick Karle
# FILENAME: db_connection.py
# SPECIFICATION: 
# FOR: CS 4250- Assignment #2
# TIME SPENT: 
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
import re

dbname = 'database'
user = '<username>'
password = '<password>'
host = '127.0.0.1'
port = '5432'

def connectDataBase():
    # Create a database connection object using psycopg2
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        cur = conn.cursor()

        create_documentss_table_query = """
        CREATE TABLE IF NOT EXIST DOCUMENTS (
            doc INT NOT NULL PRIMARY KEY,
            text LONGTEXT NOT NULL,
            title TINYTEXT NOT NULL,
            num_chars INT NOT NULL,
            date DATE NOT NULL,
            id_cat INT NOT NULL,
            FOREIGN KEY (id_cat) REFERENCES CATEGORIES(id_cat) ON CASCADE DELETE
        )
        """

        create_categories_table_query = """
        CREATE TABLE IF NOT EXIST CATEGORIES (
            id_cat INT NOT NULL PRIMARY KEY,
            name TINYTEXT NOT NULL
        )
        """
        create_terms_table_query = """
        CREATE TABLE IF NOT EXIST TERMS (
            term TINYTEXT NOT NULL PRIMAR KEY,
            num_chars INT NOT NULL
        )
        """
        create_doc_term_pairs_table_query = """
        CREATE TABLE IF NOT EXIST DOC_TERM_PAIRS (
            term TINYTEXT NOT NULL
            doc INT NOT NULL
            term_count INT NOT NULL
            PRIMARY KEY (term, doc)
            FOREIGN KEY (term) TERMS(term)
            FOREIGN KEY (doc) DOCUMENTS(doc)
        )
        """
        cur.execute(create_documentss_table_query)
        cur.execute(create_categories_table_query)
        cur.execute(create_terms_table_query)
        cur.execute(create_doc_term_pairs_table_query)
        return conn
    except psycopg2.Error as e:
        print("Error in database connecting:", e)

def createCategory(cur, catId, catName):
    # Insert a category in the database
    insert_category_query = "INSERT INTO CATEGORIES (%s, %s)"
    values = (catId, catName)
    cur.execute(insert_category_query, values)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    sanitized = re.sub(r'[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]', '', docText).lower()
    # 1 Get the category id based on the informed category name
    select_id_query = "SELECT id_cat FROM CATEGORIES WHERE name=%s"
    cur.execute(select_id_query, (docCat))
    cat_id = cur.fetchone()

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    num_chars = len(sanitized.replace(" ", ""))
    
    insert_document_query = "INSERT INTO DOCUMENTS (%s, %s, %s, %s, %s, %s)"
    values = (docId, docText, docTitle, num_chars, docDate, cat_id)
    cur.execute(insert_document_query, values)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    raw_terms = sanitized.split(" ")
    terms = {}
    for term in raw_terms:
        if term in terms.keys():
            terms[term] += 1
        else:
            terms[term] = 1
    # 3.2 For each term identified, check if the term already exists in the database
    for term in terms.keys():
        check_term_query = "SELECT * FROM TERMS WHERE term=%s"
        # 3.3 In case the term does not exist, insert it into the database
        cur.execute(check_term_query, (term))
        if not cur.fetchall():
            insert_term_query = "INSERT INTO TERMS (%s, %s)"
            values = (term, len(term))
            cur.execute(insert_term_query, values)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
        # done in 3.1
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
        # done in 3.1
    # 4.3 Insert the term and its corresponding count into the database
    for term in terms.keys():
        insert_pair_query = "INSERT INTO DOC_TERM_PAIRS (%s, %s, %s)"
        values = (term, docId, terms[term])
        cur.execute(insert_pair_query, values)

def deleteDocument(cur, docId):
    # 1 Query the index based on the document to identify terms
    search_query = "SELECT term FROM DOC_TERM_PAIRS WHERE doc=%s"
    terms = cur.execute(search_query, (docId)).fetchall()
    terms = [term[0] for term in terms]
    # 1.1 For each term identified, delete its occurrences in the index for that document
    for term in terms:
        delete_query = "DELETE FROM DOC_TERM_PAIRS WHERE doc=%s and term=%s"
        values = (docId, term)
        cur.execute(delete_query, values)
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    for term in terms:
        search_query = "SELECT * FROM DOC_TERM_PAIRS WHERE term=%s"
        cur.execute(search_query, (term))
        if not cur.fetchall():
            delete_query = "DELETE FROM TERMS WHERE term=%s"
            cur.execute(delete_query, (term))
    # 2 Delete the document from the database
    delete_document_query = "DELETE FROM DOCUMENTS WHERE doc=%s"
    cur.execute(delete_document_query, (docId))

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    # 1 Delete the document
    deleteDocument(cur, docId)
    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    index_query = "SELECT term SUM(term_count) FROM DOC_TERM_PAIRS GROUP BY term"
    cur.execute(index_query)
    return cur.fetchall()