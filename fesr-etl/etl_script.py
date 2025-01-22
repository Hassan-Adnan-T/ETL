import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import time
import pymysql
from flask import Flask, jsonify

app = Flask(__name__)

# Load environment variables from secrets directory
load_dotenv('secrets/.env')

def create_ssh_tunnel():
    """Create SSH tunnel to RDS matching your manual SSH command"""
    print("Initiating SSH tunnel connection...")
    tunnel = SSHTunnelForwarder(
        (os.getenv('SSH_HOST')),  # EC2 instance address
        ssh_username=os.getenv('SSH_USER'),
        ssh_pkey=os.getenv('SSH_KEY_FILE'),
        remote_bind_address=(os.getenv('DB_HOST'), int(os.getenv('DB_PORT'))),
        local_bind_address=('0.0.0.0', int(os.getenv('LOCAL_PORT'))),
        set_keepalive=60
    )
    print(f"Attempting to connect to {os.getenv('SSH_HOST')} as {os.getenv('SSH_USER')}")
    return tunnel

def get_database_connection(tunnel):
    """Create connection to AWS RDS MySQL through SSH tunnel"""
    print(f"Creating database connection to {os.getenv('DB_NAME')}...")
    
    try:
        connection = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30
        )
        return connection
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        raise

@app.route('/')
def run_etl():
    try:
        print("Starting FESR ETL process...")
        result = {
            'status': 'running',
            'steps': []
        }
        
        with create_ssh_tunnel() as tunnel:
            result['steps'].append('SSH tunnel established')
            tunnel.start()
            
            connection = get_database_connection(tunnel)
            result['steps'].append('Database connected')
            
            with connection.cursor() as cursor:
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", 
                             (os.getenv('DB_NAME'),))
                tables = cursor.fetchall()
                result['tables'] = [table[0] for table in tables]
                
            connection.close()
            result['status'] = 'success'
            
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8002)
