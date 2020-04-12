import sqlite3
import utilities


def get_sql_create_master_table():
    return """
        CREATE TABLE master_node (
            filename        VARCHAR(100)    PRIMARY KEY     NOT NULL,
            primary_node    VARCHAR(100)    NOT NULL
        );
    """

def get_sql_create_replication_table():
    return f"""
        CREATE TABLE replication_data (
            filename            VARCHAR(100)    NOT NULL,
            replicated_node     VARCHAR(100)    NOT NULL,
            PRIMARY KEY (filename, replicated_node)
        );
    """


def main():
    db_name = utilities.get_db_name()
    conn = sqlite3.connect(db_name)
    print(f"Created database {db_name}.")
    conn.execute(get_sql_create_master_table())
    print("Table created for master node.")
    conn.execute(get_sql_create_replication_table())
    print("Table created for storing replication data.")
    conn.close()
    print("Setup done!")

if __name__ == "__main__":
    main()
