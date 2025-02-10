import oracledb
import pandas as pd

class ColumnMapper:
    def __init__(self):
        self.send_connection = None
        self.recv_connection = None
        self.send_mapping = []  # 사용자가 입력한 송신 컬럼 순서
        self.recv_mapping = []  # 사용자가 입력한 수신 컬럼 순서
        self.send_columns = {}  # DB에서 가져온 송신 컬럼 정보 (key: 컬럼명)
        self.recv_columns = {}  # DB에서 가져온 수신 컬럼 정보 (key: 컬럼명)
        self.send_table_info = None
        self.recv_table_info = None
        self.comparison_results = []
        self.send_mapping_str = '''''' # 송신 매핑 문자열
        self.recv_mapping_str = '''''' # 수신 매핑 문자열

    def connect_db(self, sid, username, password):
        """DB 연결을 생성합니다."""
        return oracledb.connect(user=username, password=password, dsn=sid)

    def connect_send_db(self, sid, username, password):
        """송신 DB에 연결합니다."""
        self.send_connection = self.connect_db(sid, username, password)
        return self.send_connection

    def connect_recv_db(self, sid, username, password):
        """수신 DB에 연결합니다."""
        self.recv_connection = self.connect_db(sid, username, password)
        return self.recv_connection

    def close_connections(self):
        """모든 DB 연결을 종료합니다."""
        if self.send_connection:
            try:
                self.send_connection.close()
            except:
                pass
        if self.recv_connection:
            try:
                self.recv_connection.close()
            except:
                pass

    def get_column_info(self, owner, table_name, connection):
        """테이블의 컬럼 정보를 조회합니다."""
        cursor = connection.cursor()
        query = """
            SELECT column_name, data_type, data_length, nullable
            FROM all_tab_columns
            WHERE owner = :owner
            AND table_name = :table_name
            ORDER BY column_id
        """
        cursor.execute(query, owner=owner, table_name=table_name)
        columns = {}
        for row in cursor:
            columns[row[0]] = {
                'name': row[0],
                'type': row[1],
                'size': str(row[2]),
                'nullable': 'Y' if row[3] == 'Y' else 'N'
            }
        cursor.close()
        return columns

    def set_send_mapping(self, column_list):
        """송신 매핑 컬럼을 설정합니다."""
        self.send_mapping = [col.strip() for col in column_list.split('\n') if col.strip()]

    def set_recv_mapping(self, column_list):
        """수신 매핑 컬럼을 설정합니다."""
        self.recv_mapping = [col.strip() for col in column_list.split('\n') if col.strip()]

    def set_send_table(self, owner, table_name):
        """송신 테이블 정보를 설정합니다."""
        if not self.send_connection:
            raise Exception("송신 DB 연결이 필요합니다.")
        self.send_table_info = {'owner': owner, 'table_name': table_name}
        self.send_columns = self.get_column_info(owner, table_name, self.send_connection)
        return self.send_columns

    def set_recv_table(self, owner, table_name):
        """수신 테이블 정보를 설정합니다."""
        if not self.recv_connection:
            raise Exception("수신 DB 연결이 필요합니다.")
        self.recv_table_info = {'owner': owner, 'table_name': table_name}
        self.recv_columns = self.get_column_info(owner, table_name, self.recv_connection)
        return self.recv_columns

    def convert_mapping_str_to_list(self, mapping_str=None, mapping_type='send'):
        """매핑 문자열을 리스트로 변환합니다.
        
        Args:
            mapping_str: 변환할 매핑 문자열. None인 경우 mapping_type에 따라 self.send_mapping_str 또는 self.recv_mapping_str 사용
            mapping_type: 매핑 타입 ('send' 또는 'recv')
            
        Returns:
            변환된 리스트
        """
        if mapping_str is None:
            if mapping_type == 'send':
                mapping_str = self.send_mapping_str
            else:
                mapping_str = self.recv_mapping_str
            
        return [col.strip() for col in mapping_str.split('\n') if col.strip()]

    def compare_columns(self):
        """송수신 컬럼 비교"""
        if not self.send_mapping:
            return [{"error": "송신 매핑 정보가 설정되지 않았습니다."}]
        if not self.send_columns:
            return [{"error": "송신 테이블 정보가 설정되지 않았습니다."}]

        self.comparison_results = []
        has_error = False

        # 송신 컬럼 수와 수신 매핑 리스트 길이 맞추기
        recv_mapping = self.recv_mapping if self.recv_mapping else [""] * len(self.send_mapping)
        if len(recv_mapping) < len(self.send_mapping):
            recv_mapping.extend([""] * (len(self.send_mapping) - len(recv_mapping)))

        # 모든 컬럼 비교 수행
        for idx, (send_col, recv_col) in enumerate(zip(self.send_mapping, recv_mapping)):
            result = {
                'send_column': send_col,
                'recv_column': recv_col,
                'send_info': None,
                'recv_info': None,
                'type_diff': None,
                'size_diff': None,
                'size_over': None,
                'nullable_diff': None,
                'errors': []
            }

            # 송신 컬럼 정보 확인
            if not send_col:
                result['errors'].append("송신 컬럼이 비어있습니다.")
                has_error = True
            elif send_col not in self.send_columns:
                result['errors'].append(f"송신 테이블에 {send_col} 컬럼이 존재하지 않습니다.")
                has_error = True
            else:
                result['send_info'] = self.send_columns[send_col]

            # 수신 컬럼이 비어있는 경우는 정상적인 상황으로 처리
            if not recv_col:
                result['errors'].append("수신 매핑이 설정되지 않았습니다 (선택적 수신)")
            else:
                # 수신 컬럼이 있는 경우에만 컬럼 존재 여부와 상세 비교 수행
                if not self.recv_columns:
                    result['errors'].append("수신 테이블 정보가 설정되지 않았습니다.")
                    has_error = True
                elif recv_col not in self.recv_columns:
                    result['errors'].append(f"수신 테이블에 {recv_col} 컬럼이 존재하지 않습니다.")
                    has_error = True
                else:
                    result['recv_info'] = self.recv_columns[recv_col]
                    
                    # 상세 비교는 송신과 수신 정보가 모두 있는 경우에만 수행
                    if result['send_info'] and result['recv_info']:
                        send_info = result['send_info']
                        recv_info = result['recv_info']
                        
                        # 타입 비교
                        type_diff = self.check_type_diff(send_info, recv_info)
                        if type_diff:
                            result['type_diff'] = type_diff
                            result['errors'].append(f"타입이 다릅니다: 송신({send_info['type']}) vs 수신({recv_info['type']})")
                            has_error = True
                        
                        # 크기 비교
                        size_diff = self.check_size_diff(send_info, recv_info)
                        if size_diff:
                            result['size_diff'] = size_diff
                            result['errors'].append(f"크기가 다릅니다: 송신({send_info['size']}) vs 수신({recv_info['size']})")
                            has_error = True
                        
                        # 1024 바이트 초과 여부 확인
                        size_over = self.check_size_over_1024(send_info)
                        if size_over:
                            result['size_over'] = size_over
                            result['errors'].append("송신 컬럼 크기가 1024 바이트를 초과합니다.")
                            has_error = True
                        
                        # Nullable 비교
                        nullable_diff = self.check_nullable_diff(send_info, recv_info)
                        if nullable_diff:
                            result['nullable_diff'] = nullable_diff
                            result['errors'].append(f"NULL 허용 여부가 다릅니다: 송신({send_info['nullable']}) vs 수신({recv_info['nullable']})")
                            has_error = True

            self.comparison_results.append(result)

        return self.comparison_results

    def check_type_diff(self, send, recv):
        """타입 차이 체크"""
        if not isinstance(send, dict) or not isinstance(recv, dict):
            return "컬럼 정보 형식 오류"
        if 'type' not in send or 'type' not in recv:
            return "컬럼 타입 정보 누락"
        if send['type'] == recv['type']:
            return ""
        if (send['type'] in ["VARCHAR", "VARCHAR2", "CHAR"]) and (recv['type'] in ["VARCHAR", "VARCHAR2", "CHAR"]):
            return ""
        return "칼럼 Type NG"

    def check_size_diff(self, send, recv):
        """사이즈 차이 체크"""
        if not isinstance(send, dict) or not isinstance(recv, dict):
            return "컬럼 정보 형식 오류"
        if 'type' not in send or 'type' not in recv or 'size' not in send or 'size' not in recv:
            return "컬럼 타입 또는 크기 정보 누락"
        if send['type'] in ["NVARCHAR", "NCHAR", "NVARCHAR2"] or recv['type'] in ["NVARCHAR", "NCHAR", "NVARCHAR2"]:
            return "NCHAR TYPE"
        if send['type'] in ["BLOB", "CLOB"] or recv['type'] in ["BLOB", "CLOB"]:
            return "LOB TYPE"
        if send['size'] != recv['size']:
            return "칼럼 Size NG"
        return ""

    def check_size_over_1024(self, col_info):
        """1024 바이트 초과 체크"""
        if not isinstance(col_info, dict):
            return "컬럼 정보 형식 오류"
        if 'type' not in col_info or 'size' not in col_info:
            return "컬럼 타입 또는 크기 정보 누락"
        if col_info['type'] in ["NVARCHAR", "NCHAR", "NVARCHAR2"]:
            if float(col_info['size']) > 1024 / 3:
                return "칼럼 Size > 1024"
        if float(col_info['size']) > 1024:
            return "칼럼 Size > 1024"
        return ""

    def check_nullable_diff(self, send, recv):
        """Nullable 차이 체크"""
        if not isinstance(send, dict) or not isinstance(recv, dict):
            return "컬럼 정보 형식 오류"
        if 'nullable' not in send or 'nullable' not in recv:
            return "Nullable 정보 누락"
            
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

    def generate_send_sql_from_mapping(self):
        """송신 SQL 생성"""
        if not self.send_mapping or not self.send_table_info:
            return "송신 테이블 정보가 설정되지 않았습니다."
        return self.generate_full_send_sql(self.send_table_info, self.send_mapping, self.send_columns)

    def generate_recv_sql(self):
        """수신 SQL 생성"""
        if not self.recv_mapping or not self.recv_table_info:
            return "수신 테이블 정보가 설정되지 않았습니다."
        return self.generate_full_receive_sql(self.recv_table_info, self.recv_mapping, self.recv_columns)

    def generate_field_xml_from_mapping(self):
        """필드 XML 생성"""
        if not self.send_mapping:
            return "송신 테이블 정보가 설정되지 않았습니다."
        xml = self.generate_field_xml(self.send_mapping, self.send_columns)
        return self.format_field_xml(xml)

    def generate_receive_insert_into(self, column_list, columns_info, base_query):
        """수신 INSERT 문의 INTO 부분 생성
        Args:
            column_list: 컬럼 목록
            columns_info: 컬럼 정보 딕셔너리
            base_query: 기본 쿼리 ($E$3에 해당)
        
        Returns:
            INTO 절 문자열
        """
        # 필수 시스템 컬럼 추가 (항상 처음에 포함)
        sql_parts = [
            "EAI_SEQ_ID",
            "DATA_INTERFACE_TYPE_CODE",
            "EAI_INTERFACE_DATE",
            "APPLICATION_TRANSFER_FLAG"
        ]
        
        # 사용자가 지정한 컬럼들 추가
        for col in column_list:
            if not col:  # 빈 컬럼은 건너뜀
                continue
                
            if col not in columns_info:
                continue
                
            sql_parts.append(col)
        
        # 컬럼들을 2개씩 그룹화하여 포매팅
        formatted_parts = []
        for i in range(0, len(sql_parts), 2):
            if i + 1 < len(sql_parts):
                formatted_parts.append(f"    {sql_parts[i]}, {sql_parts[i+1]}")
            else:
                formatted_parts.append(f"    {sql_parts[i]}")
        
        # 기본 쿼리가 있으면 합치고, 없으면 컬럼 목록만 반환
        if base_query:
            return f"{base_query}\n" + ",\n".join(formatted_parts)
        return ",\n".join(formatted_parts)

    def generate_receive_insert_values(self, column_list, columns_info, base_query):
        """수신 INSERT 문의 VALUES 부분 생성
        Args:
            column_list: 컬럼 목록 (수신 컬럼 목록)
            columns_info: 컬럼 정보 딕셔너리 (수신 컬럼 정보)
            base_query: 기본 쿼리 ($E$3에 해당)
        
        Returns:
            VALUES 절 문자열
        """
        # 필수 시스템 컬럼 값 추가 (항상 처음에 포함)
        sql_parts = [
            ":EAI_SEQ_ID", ":DATA_INTERFACE_TYPE_CODE",
            "SYSDATE", "'N'"
        ]
        
        # 사용자가 지정한 컬럼들의 값 추가
        for idx, recv_col in enumerate(column_list):
            if not recv_col:  # 빈 컬럼은 건너뜀
                continue
                
            if recv_col not in columns_info:
                continue
                
            # 송신 컬럼명 가져오기 (같은 인덱스의 송신 매핑에서)
            send_col = self.send_mapping[idx] if idx < len(self.send_mapping) else None
            if not send_col:
                continue

            col_info = columns_info[recv_col]
            
            # DATE 타입인 경우 TO_DATE 변환 추가
            if col_info['type'] == 'DATE':
                sql_parts.append(f"TO_DATE(:{send_col}, 'YYYYMMDDHH24MISS')")
            else:
                sql_parts.append(f":{send_col}")
        
        # 값들을 2개씩 그룹화하여 포매팅
        formatted_parts = []
        for i in range(0, len(sql_parts), 2):
            if i + 1 < len(sql_parts):
                formatted_parts.append(f"    {sql_parts[i]}, {sql_parts[i+1]}")
            else:
                formatted_parts.append(f"    {sql_parts[i]}")
        
        # 기본 쿼리가 있으면 합치고, 없으면 값 목록만 반환
        if base_query:
            return f"{base_query}\n" + ",\n".join(formatted_parts)
        return ",\n".join(formatted_parts)

    def generate_full_send_sql(self, table_info, column_list, columns_info):
        """전체 송신 SQL 생성 (SELECT 문 전체)
        
        Args:
            table_info: 테이블 정보 {'owner': 스키마명, 'table_name': 테이블명}
            column_list: 컬럼 목록
            columns_info: 컬럼 정보 딕셔너리
        
        Returns:
            완성된 SELECT 문
        """
        columns = []
        for col in column_list:
            if col in columns_info:
                columns.append(col)
        
        if not columns:
            return ""
        
        sql = f"SELECT\n"
        # 컬럼들을 2개씩 그룹화하여 포매팅
        formatted_parts = []
        for i in range(0, len(columns), 2):
            if i + 1 < len(columns):
                formatted_parts.append(f"    {columns[i]}, {columns[i+1]}")
            else:
                formatted_parts.append(f"    {columns[i]}")
        sql += ",\n".join(formatted_parts)
        sql += f"\nFROM {table_info['owner']}.{table_info['table_name']}"
        return sql

    def generate_send_sql(self, column_list, columns_info, base_query):
        """송신 SQL 생성
        
        Args:
            column_list: 컬럼 목록
            columns_info: 컬럼 정보 딕셔너리
            base_query: 기본 쿼리 ($D$3에 해당)
        
        Returns:
            생성된 SQL 문자열
        """
        # 필수 시스템 컬럼 추가 (항상 처음에 포함)
        sql_parts = ["EAI_SEQ_ID", "DATA_INTERFACE_TYPE_CODE"]
        
        # 사용자가 지정한 컬럼들 추가
        for col in column_list:
            if not col:  # 빈 컬럼은 건너뜀
                continue
                
            if col not in columns_info:
                continue
                
            col_info = columns_info[col]
            # DATE 타입인 경우 TO_CHAR 변환 추가
            if col_info['type'] == 'DATE':
                sql_parts.append(f"TO_CHAR({col}, 'YYYYMMDDHH24MISS')")
            else:
                sql_parts.append(col)
        
        # 컬럼들을 2개씩 그룹화하여 포매팅
        formatted_parts = []
        for i in range(0, len(sql_parts), 2):
            if i + 1 < len(sql_parts):
                formatted_parts.append(f"    {sql_parts[i]}, {sql_parts[i+1]}")
            else:
                formatted_parts.append(f"    {sql_parts[i]}")
        
        # SQL 문 조합
        if base_query:
            sql = f"{base_query}\n"
        else:
            sql = "SELECT\n"
        sql += ",\n".join(formatted_parts)
        return sql

    def generate_full_receive_sql(self, table_info, column_list, columns_info):
        """전체 수신 INSERT 문 생성
        
        Args:
            table_info: 테이블 정보 {'owner': 스키마명, 'table_name': 테이블명}
            column_list: 컬럼 목록
            columns_info: 컬럼 정보 딕셔너리
        
        Returns:
            완성된 INSERT 문
        """
        # 기본 쿼리
        base_query = "INSERT INTO "
        if table_info['owner']:
            base_query += f"{table_info['owner']}."
        base_query += f"{table_info['table_name']} ("
        
        # INTO 절 생성 (기본 시스템 컬럼 포함)
        into_part = self.generate_receive_insert_into(column_list, columns_info, "")
        
        # VALUES 절 생성 (기본 시스템 값 포함)
        values_part = self.generate_receive_insert_values(column_list, columns_info, "")
        
        return f"{base_query}\n{into_part}\n) VALUES (\n{values_part}\n)"

    def generate_field_xml(self, column_list, columns_info):
        """필드 XML 생성
        
        Args:
            column_list: 컬럼 목록
            columns_info: 컬럼 정보 딕셔너리
        
        Returns:
            XML 문자열
        """
        # 필수 시스템 컬럼 추가
        all_columns = ["EAI_SEQ_ID", "DATA_INTERFACE_TYPE_CODE"] + [col for col in column_list if col]
        
        # 필드 개수 계산
        field_count = len(all_columns)
        
        xml_parts = []
        xml_parts.append(f'<fields count="{field_count}">')
        
        # 필드 생성
        for col in all_columns:
            # 기본 속성
            attrs = {
                'key': '0',
                'nofetch': '0',
                'name': col
            }
            
            # 시스템 컬럼이 아닌 경우에만 추가 속성 검사
            if col in columns_info:
                col_info = columns_info[col]
                col_type = col_info['type']
                
                try:
                    size = int(col_info['size'])
                except (ValueError, TypeError):
                    size = 0
                
                # NVARCHAR, NCHAR, NVARCHAR2 타입 처리
                if col_type in ['NVARCHAR', 'NCHAR', 'NVARCHAR2']:
                    if size * 3 > 1024:
                        attrs['length'] = str(size * 3)
                
                # BLOB 타입 처리
                elif col_type == 'BLOB':
                    attrs.update({
                        'length': '1000000',
                        'type': 'blob',
                        'length_info': '1000000',
                        'start_info': '1',
                        'attr': 'bin'
                    })
                
                # CLOB 타입 처리
                elif col_type == 'CLOB':
                    attrs.update({
                        'length': '3000000',
                        'type': 'clob',
                        'length_info': '0',
                        'start_info': '0',
                        'attr': 'bin'
                    })
                
                # 기타 타입의 크기가 1024 초과인 경우
                elif size > 1024:
                    attrs['length'] = str(size)
            
            # XML 태그 생성
            attr_str = ' '.join(f'{k}="{v}"' for k, v in attrs.items())
            xml_parts.append(f'    <field {attr_str}/>')
        
        xml_parts.append('</fields>')
        return '\n'.join(xml_parts)

    def format_field_xml(self, xml_str):
        """XML 문자열을 보기 좋게 포맷팅
        
        Args:
            xml_str: XML 문자열
        Returns:
            들여쓰기가 적용된 XML 문자열
        """
        try:
            import xml.dom.minidom
            dom = xml.dom.minidom.parseString(xml_str)
            return dom.toprettyxml()
        except Exception as e:
            print(f"XML 포맷팅 중 오류 발생: {str(e)}")
            return xml_str

if __name__ == "__main__":
    # 테스트를 위한 DB 연결 정보
    SEND_DB_INFO = {
        'sid': 'your_send_sid',
        'username': 'your_send_username',
        'password': 'your_send_password'
    }
    RECV_DB_INFO = {
        'sid': 'your_recv_sid',
        'username': 'your_recv_username',
        'password': 'your_recv_password'
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
        print("1. 송신 DB 연결")
        print("="*50)
        try:
            mapper.connect_send_db(**SEND_DB_INFO)
        except Exception as e:
            print(f"송신 DB 연결 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("2. 수신 DB 연결")
        print("="*50)
        try:
            mapper.connect_recv_db(**RECV_DB_INFO)
        except Exception as e:
            print(f"수신 DB 연결 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("3. 송신 테이블 컬럼 정보 조회")
        print("="*50)
        try:
            send_cols = mapper.set_send_table(**SEND_TABLE)
            print("송신 컬럼 정보:")
            for col_name, col_info in send_cols.items():
                print(f"컬럼명: {col_name}, 타입: {col_info['type']}, 크기: {col_info['size']}, Nullable: {col_info['nullable']}")
        except Exception as e:
            print(f"송신 테이블 조회 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("4. 수신 테이블 컬럼 정보 조회")
        print("="*50)
        try:
            recv_cols = mapper.set_recv_table(**RECV_TABLE)
            print("수신 컬럼 정보:")
            for col_name, col_info in recv_cols.items():
                print(f"컬럼명: {col_name}, 타입: {col_info['type']}, 크기: {col_info['size']}, Nullable: {col_info['nullable']}")
        except Exception as e:
            print(f"수신 테이블 조회 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("5. 송수신 컬럼 비교")
        print("="*50)
        try:
            results = mapper.compare_columns()
            for result in results:
                print(f"\n송신 컬럼: {result['send_column']} -> 수신 컬럼: {result['recv_column']}")
                if result['type_diff']: print(f"타입 차이: {result['type_diff']}")
                if result['size_diff']: 
                    print(f"크기 차이: {result['size_diff']}")
                    # 문자형이고 수신 컬럼이 더 큰 경우 안전하다는 메시지 추가
                    if ('CHAR' in result['send_info']['type'].upper() or 'VARCHAR' in result['send_info']['type'].upper()) and \
                       int(result['recv_info']['size']) > int(result['send_info']['size']):
                        print("(안전: 수신 컬럼의 크기가 더 크므로 데이터 손실 위험 없음)")
                if result['size_over']: print(f"크기 초과: {result['size_over']}")
                if result['nullable_diff']: print(f"Nullable 차이: {result['nullable_diff']}")
        except Exception as e:
            print(f"컬럼 비교 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("6. 송신 SQL 생성")
        print("="*50)
        try:
            send_sql = mapper.generate_send_sql_from_mapping()
            print(send_sql)
        except Exception as e:
            print(f"송신 SQL 생성 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("7. 수신 SQL 생성")
        print("="*50)
        try:
            recv_sql = mapper.generate_recv_sql()
            print(recv_sql)
        except Exception as e:
            print(f"수신 SQL 생성 실패: {str(e)}")
        
        print("\n" + "="*50)
        print("8. 필드 XML 생성")
        print("="*50)
        try:
            field_xml = mapper.generate_field_xml_from_mapping()
            print(field_xml)
        except Exception as e:
            print(f"필드 XML 생성 실패: {str(e)}")
        
        # DB 연결 종료
        mapper.close_connections()
    
    # 테스트 실행
    run_test()