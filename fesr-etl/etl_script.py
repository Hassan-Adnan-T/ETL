import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import time
import pymysql
from flask import Flask, jsonify
from datetime import datetime

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

def get_source_connection(tunnel):
    """Create connection to AWS RDS MySQL through SSH tunnel"""
    print(f"Creating source database connection to {os.getenv('DB_NAME')}...")
    
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
        print(f"Failed to connect to source database: {str(e)}")
        raise

def get_destination_connection():
    """Create connection to local MySQL database"""
    print("Creating destination database connection...")
    
    try:
        connection = pymysql.connect(
            host='db',  # Docker service name
            port=3306,
            user='etl_user',
            password='etl_password',
            database='etl_summary',
            connect_timeout=10
        )
        return connection
    except Exception as e:
        print(f"Failed to connect to destination database: {str(e)}")
        raise

@app.route('/')
def run_etl():
    try:
        print("Starting Faculty Department Summary ETL process...")
        result = {
            'status': 'running',
            'steps': [],
            'metrics': {}
        }
        
        # Connect to source database (AWS RDS)
        with create_ssh_tunnel() as tunnel:
            result['steps'].append('SSH tunnel established')
            tunnel.start()
            
            source_conn = get_source_connection(tunnel)
            result['steps'].append('Source database connected')
            
            # Extract data from source
            with source_conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        d.DepartmentID,
                        d.DepartmentName,
                        d.Description,
                        COUNT(u.UserID) as total_faculty,
                        SUM(CASE WHEN u.EmploymentType = 'fulltime' THEN 1 ELSE 0 END) as fulltime_count,
                        SUM(CASE WHEN u.EmploymentType = 'parttime' THEN 1 ELSE 0 END) as parttime_count,
                        SUM(CASE WHEN u.EmploymentType = 'temporary' THEN 1 ELSE 0 END) as temporary_count,
                        SUM(CASE WHEN u.EmploymentType = 'designee' THEN 1 ELSE 0 END) as designee_count
                    FROM departments d
                    LEFT JOIN users u ON d.DepartmentID = u.DepartmentID
                    GROUP BY d.DepartmentID, d.DepartmentName, d.Description
                """)
                department_summary = cursor.fetchall()
                result['metrics']['departments_processed'] = len(department_summary)
            
            source_conn.close()
            result['steps'].append('Source data extracted')
            
            # Connect to destination database (Local MySQL)
            dest_conn = get_destination_connection()
            result['steps'].append('Destination database connected')
            
            # Create summary table if not exists
            with dest_conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS faculty_department_summary (
                        department_id INT PRIMARY KEY,
                        department_name VARCHAR(100),
                        department_description VARCHAR(255),
                        total_faculty INT,
                        fulltime_count INT,
                        parttime_count INT,
                        temporary_count INT,
                        designee_count INT,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_dept_name (department_name)
                    )
                """)
                dest_conn.commit()
                result['steps'].append('Summary table created/verified')
                
                # Load transformed data
                for dept in department_summary:
                    cursor.execute("""
                        INSERT INTO faculty_department_summary (
                            department_id, department_name, department_description,
                            total_faculty, fulltime_count, parttime_count,
                            temporary_count, designee_count
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            department_name = VALUES(department_name),
                            department_description = VALUES(department_description),
                            total_faculty = VALUES(total_faculty),
                            fulltime_count = VALUES(fulltime_count),
                            parttime_count = VALUES(parttime_count),
                            temporary_count = VALUES(temporary_count),
                            designee_count = VALUES(designee_count),
                            last_updated = CURRENT_TIMESTAMP
                    """, dept)
                
                dest_conn.commit()
                result['steps'].append(f"Loaded {len(department_summary)} department summaries")
            
            dest_conn.close()
            result['status'] = 'success'
            
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8002)
