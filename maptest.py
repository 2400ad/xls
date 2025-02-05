import oracledb
import pandas as pd

class ColumnMapper:
    def __init__(self):
        self.send_info = None
        self.recv_info = None
        self.send_table_info = None
        self.recv_table_info = None
        self.comparison_results = []

    def connect_db(self, sid, username, password):
        """DB 연결"""
        return oracledb.connect(user=username, password=password, dsn=sid)

    def get_column_info(self, connection, owner, table_name):
        """테이블의 컬럼 정보 조회"""
        query = """
        SELECT column_name as name, 
               data_type as type,
               data_length as size,
               nullable
        FROM all_tab_columns 
        WHERE owner = :owner 
        AND table_name = :table_name
        ORDER BY column_id
        """
        with connection.cursor() as cursor:
            cursor.execute(query, {'owner': owner, 'table_name': table_name})
            columns = cursor.fetchall()
            return [dict(zip([d[0].lower() for d in cursor.description], row)) for row in columns]

    def set_send_table(self, owner, table_name, connection):
        """송신 테이블 설정"""
        self.send_table_info = {'owner': owner, 'table_name': table_name}
        self.send_info = self.get_column_info(connection, owner, table_name)
        return self.send_info

    def set_recv_table(self, owner, table_name, connection):
        """수신 테이블 설정"""
        self.recv_table_info = {'owner': owner, 'table_name': table_name}
        self.recv_info = self.get_column_info(connection, owner, table_name)
        return self.recv_info

    def compare_columns(self):
        """송수신 컬럼 비교"""
        if not self.send_info or not self.recv_info:
            return "송신 또는 수신 테이블 정보가 설정되지 않았습니다."

        self.comparison_results = []
        for send, recv in zip(self.send_info, self.recv_info):
            result = {
                'column_name': send['name'],
                'type_diff': self.check_type_diff(send, recv),
                'size_diff': self.check_size_diff(send, recv),
                'size_over': self.check_size_over_1024(send),
                'nullable_diff': self.check_nullable_diff(send, recv)
            }
            self.comparison_results.append(result)
        return self.comparison_results

    def check_type_diff(self, send, recv):
        """타입 차이 체크"""
        if send['type'] == recv['type']:
            return ""
        if (send['type'] in ["VARCHAR", "VARCHAR2", "CHAR"]) and (recv['type'] in ["VARCHAR", "VARCHAR2", "CHAR"]):
            return ""
        return "칼럼 Type NG"

    def check_size_diff(self, send, recv):
        """사이즈 차이 체크"""
        if send['type'] in ["NVARCHAR", "NCHAR", "NVARCHAR2"] or recv['type'] in ["NVARCHAR", "NCHAR", "NVARCHAR2"]:
            return "NCHAR TYPE"
        if send['type'] in ["BLOB", "CLOB"] or recv['type'] in ["BLOB", "CLOB"]:
            return "LOB TYPE"
        if send['size'] != recv['size']:
            return "칼럼 Size NG"
        return ""

    def check_size_over_1024(self, col_info):
        """1024 바이트 초과 체크"""
        if col_info['type'] in ["NVARCHAR", "NCHAR", "NVARCHAR2"]:
            if float(col_info['size']) > 1024 / 3:
                return "칼럼 Size > 1024"
        if float(col_info['size']) > 1024:
            return "칼럼 Size > 1024"
        return ""

    def check_nullable_diff(self, send, recv):
        """Nullable 차이 체크"""
        send_nullable = send['nullable']
        recv_nullable = recv['nullable']
        
        # NULLABLE 값 유효성 체크
        if send_nullable not in ['Y', 'N']:
            return f"송신 Nullable 값 오류: {send_nullable}"
        if recv_nullable not in ['Y', 'N']:
            return f"수신 Nullable 값 오류: {recv_nullable}"
        
        # 수신이 Not Null(N)인데 송신이 Nullable(Y)인 경우
        if recv_nullable == "N" and send_nullable == "Y":
            return "Nullable NG (수신 Not Null 제약 위반 가능성)"
        return ""

    def generate_send_sql(self):
        """송신 SQL 생성"""
        if not self.send_info or not self.send_table_info:
            return "송신 테이블 정보가 설정되지 않았습니다."
        return generate_full_send_sql(self.send_table_info, self.send_info)

    def generate_recv_sql(self):
        """수신 SQL 생성"""
        if not self.recv_info or not self.recv_table_info:
            return "수신 테이블 정보가 설정되지 않았습니다."
        return generate_full_receive_sql(self.recv_table_info, self.recv_info)

    def generate_field_xml(self):
        """필드 XML 생성"""
        if not self.send_info:
            return "송신 테이블 정보가 설정되지 않았습니다."
        xml = generate_field_xml(self.send_info)
        return format_field_xml(xml)

def generate_full_send_sql(table_info, columns_info):
    """전체 송신 SQL 생성 (SELECT 문 전체)
    Args:
        table_info: 테이블 정보 {'owner': 스키마명, 'table_name': 테이블명}
        columns_info: 컬럼 정보 리스트
    
    Returns:
        완성된 SELECT 문
    """
    # 기본 쿼리 생성
    base_query = f"SELECT "
    
    # 컬럼 부분 생성
    columns_part = generate_send_sql(columns_info, base_query)
    
    # FROM 절 추가
    if table_info['owner']:
        from_clause = f"\nFROM {table_info['owner']}.{table_info['table_name']}"
    else:
        from_clause = f"\nFROM {table_info['table_name']}"
    
    return f"{columns_part}{from_clause}"

def generate_send_sql(columns_info, base_query):
    """송신 SQL 생성
    Args:
        columns_info: 컬럼 정보 리스트 [{'name': 컬럼명, 'type': 데이터타입, ...}, ...]
        base_query: 기본 쿼리 ($D$3에 해당)
    
    Returns:
        생성된 SQL 문자열
    """
    # 컬럼 개수 체크
    if not columns_info:
        return "송수신 컬럼 개수가 틀립니다!! 확인해주세요."
    
    # 결과 리스트
    sql_parts = []
    
    # 일반 컬럼 처리
    for col_info in columns_info:
        col_name = col_info['name']
        col_type = col_info['type']
        
        # DATE 타입 처리
        if col_type == "DATE":
            sql_parts.append(f"TO_CHAR({col_name},'YYYYMMDDHH24MISS')")
        else:
            sql_parts.append(col_name)
    
    # 컬럼들을 콤마로 연결
    columns_sql = ','.join(sql_parts)
    
    return f"{base_query}{columns_sql}"

def generate_full_receive_sql(table_info, columns_info):
    """전체 수신 INSERT 문 생성
    Args:
        table_info: 테이블 정보 {'owner': 스키마명, 'table_name': 테이블명}
        columns_info: 컬럼 정보 리스트
    
    Returns:
        완성된 INSERT 문
    """
    # 기본 쿼리
    base_query = "INSERT INTO "
    if table_info['owner']:
        base_query += f"{table_info['owner']}."
    base_query += f"{table_info['table_name']} ("
    
    # INTO 절 생성
    into_part = generate_receive_insert_into(columns_info, base_query)
    
    # VALUES 절 생성
    values_part = generate_receive_insert_values(columns_info, "VALUES (")
    
    return f"{into_part})\n{values_part})"

def generate_receive_insert_into(columns_info, base_query):
    """수신 INSERT 문의 INTO 부분 생성
    Args:
        columns_info: 컬럼 정보 리스트
        base_query: 기본 쿼리 ($E$3에 해당)
    
    Returns:
        INTO 절 문자열
    """
    if not columns_info:
        return "송수신 컬럼 개수가 틀립니다!! 확인해주세요."
    
    # 일반 컬럼 처리
    column_names = [col_info['name'] for col_info in columns_info]
    return f"{base_query}{', '.join(column_names)}"

def generate_receive_insert_values(columns_info, base_query):
    """수신 INSERT 문의 VALUES 부분 생성
    Args:
        columns_info: 컬럼 정보 리스트
        base_query: 기본 쿼리 ($E$3에 해당)
    
    Returns:
        VALUES 절 문자열
    """
    if not columns_info:
        return "송수신 컬럼 개수가 틀립니다!! 확인해주세요."
    
    # 일반 컬럼 처리
    value_parts = []
    for col_info in columns_info:
        col_name = col_info['name']
        col_type = col_info['type']
        
        # DATE 타입 처리
        if col_type == "DATE":
            value_parts.append(f"TO_DATE(:{col_name},'YYYYMMDDHH24MISS')")
        else:
            value_parts.append(f":{col_name}")
    
    return f"{base_query}{','.join(value_parts)}"

def generate_field_xml(columns_info):
    """필드 XML 생성
    Args:
        columns_info: 컬럼 정보 리스트
    
    Returns:
        XML 문자열
    """
    # 필드 개수 계산 (field가 포함된 컬럼 수)
    field_count = sum(1 for col in columns_info if 'field' in col.get('name', '').lower())
    
    # XML 라인 리스트
    xml_lines = []
    
    # 헤더 추가
    xml_lines.append(f'<fields count="{field_count}">')
    
    # 일반 필드 처리
    for col_info in columns_info:
        if not col_info['name']:  # 빈 컬럼 스킵
            continue
            
        # 기본 속성
        attrs = {
            'key': '0',
            'nofetch': '0',
            'name': col_info['name']
        }
        
        # 타입별 추가 속성
        col_type = col_info['type']
        col_size = col_info.get('size', 0)
        
        try:
            size = int(col_size)
        except (ValueError, TypeError):
            size = 0
            
        if col_type in ['NVARCHAR', 'NCHAR', 'NVARCHAR2']:
            if size * 3 > 1024:
                attrs['length'] = str(size * 3)
        elif col_type == 'BLOB':
            attrs.update({
                'length': '1000000',
                'type': 'blob',
                'length_info': '1000000',
                'start_info': '1',
                'attr': 'bin'
            })
        elif col_type == 'CLOB':
            attrs.update({
                'length': '3000000',
                'type': 'clob',
                'length_info': '0',
                'start_info': '0',
                'attr': 'bin'
            })
        elif size > 1024:
            attrs['length'] = str(size)
        
        # XML 태그 생성
        attr_str = ' '.join(f'{k}="{v}"' for k, v in attrs.items())
        xml_lines.append(f'<field {attr_str}/>')
    
    # 닫는 태그 추가
    xml_lines.append('</fields>')
    
    return '\n'.join(xml_lines)

def format_field_xml(xml_str):
    """XML 문자열을 보기 좋게 포맷팅
    Args:
        xml_str: XML 문자열
    Returns:
        들여쓰기가 적용된 XML 문자열
    """
    lines = xml_str.split('\n')
    indent = 0
    formatted_lines = []
    
    for line in lines:
        if line.strip().startswith('</'):  # 닫는 태그
            indent -= 1
        formatted_lines.append('  ' * indent + line.strip())
        if not (line.strip().endswith('/>') or line.strip().startswith('</')):  # 여는 태그
            indent += 1
    
    return '\n'.join(formatted_lines)

if __name__ == "__main__":
    # 테스트를 위한 DB 연결 정보
    DB_INFO = {
        'sid': 'your_sid',
        'username': 'your_username',
        'password': 'your_password'
    }
    
    # 테스트를 위한 테이블 정보
    SEND_TABLE = {
        'owner': 'SEND_OWNER',
        'table_name': 'SEND_TABLE'
    }
    RECV_TABLE = {
        'owner': 'RECV_OWNER',
        'table_name': 'RECV_TABLE'
    }
    
    def run_test():
        mapper = ColumnMapper()
        
        print("="*50)
        print("1. 송신 테이블 컬럼 정보 조회")
        print("="*50)
        try:
            conn = mapper.connect_db(**DB_INFO)
            send_cols = mapper.set_send_table(**SEND_TABLE, connection=conn)
            print("송신 컬럼 정보:")
            for col in send_cols:
                print(f"컬럼명: {col['name']}, 타입: {col['type']}, 크기: {col['size']}, Nullable: {col['nullable']}")
        except Exception as e:
            print(f"송신 테이블 조회 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("2. 수신 테이블 컬럼 정보 조회")
        print("="*50)
        try:
            recv_cols = mapper.set_recv_table(**RECV_TABLE, connection=conn)
            print("수신 컬럼 정보:")
            for col in recv_cols:
                print(f"컬럼명: {col['name']}, 타입: {col['type']}, 크기: {col['size']}, Nullable: {col['nullable']}")
        except Exception as e:
            print(f"수신 테이블 조회 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("3. 송수신 컬럼 비교")
        print("="*50)
        try:
            results = mapper.compare_columns()
            for result in results:
                print(f"\n컬럼: {result['column_name']}")
                if result['type_diff']: print(f"타입 차이: {result['type_diff']}")
                if result['size_diff']: print(f"크기 차이: {result['size_diff']}")
                if result['size_over']: print(f"크기 초과: {result['size_over']}")
                if result['nullable_diff']: print(f"Nullable 차이: {result['nullable_diff']}")
        except Exception as e:
            print(f"컬럼 비교 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("4. 송신 SQL 생성")
        print("="*50)
        try:
            send_sql = mapper.generate_send_sql()
            print(send_sql)
        except Exception as e:
            print(f"송신 SQL 생성 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("5. 수신 SQL 생성")
        print("="*50)
        try:
            recv_sql = mapper.generate_recv_sql()
            print(recv_sql)
        except Exception as e:
            print(f"수신 SQL 생성 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("6. 필드 XML 생성")
        print("="*50)
        try:
            field_xml = mapper.generate_field_xml()
            print(field_xml)
        except Exception as e:
            print(f"필드 XML 생성 실패: {str(e)}")
        
        # DB 연결 종료
        conn.close()
    
    # 테스트 실행
    run_test()