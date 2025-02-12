import oracledb
import pandas as pd

class ColumnMapper:
	def __init__(self):
		oracledb.init_oracle_client(lib_dir=r"C:\instantclient_21_3")
		self.send_connection = None
		self.recv_connection = None
		self.send_mapping = []  # 사용자가 입력한 송신 컬럼 순서
		self.recv_mapping = []  # 사용자가 입력한 수신 컬럼 순서
		self.send_columns = {}  # DB에서 가져온 송신 컬럼 정보 (key: 컬럼명)
		self.recv_columns = {}  # DB에서 가져온 수신 컬럼 정보 (key: 컬럼명)
		self.send_table_info = None
		self.recv_table_info = None
		self.comparison_results = []
		self.send_mapping_str = ''''''  # 송신 매핑 문자열
		self.recv_mapping_str = ''''''  # 수신 매핑 문자열

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
		has_warning = False

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
				'errors': [],
				'warnings': []
			}

			# 송신 컬럼 정보 확인
			if not send_col:
				result['errors'].append("송신 컬럼이 비어있습니다.")
			elif send_col not in self.send_columns:
				result['errors'].append(f"송신 테이블에 {send_col} 컬럼이 존재하지 않습니다.")
			else:
				result['send_info'] = self.send_columns[send_col]

			# 수신 컬럼이 비어있는 경우는 정상적인 상황으로 처리
			if not recv_col:
				result['warnings'].append("수신 매핑이 설정되지 않았습니다 (선택적 수신)")
			else:
				if not self.recv_columns:
					result['errors'].append("수신 테이블 정보가 설정되지 않았습니다.")
				elif recv_col not in self.recv_columns:
					result['errors'].append(f"수신 테이블에 {recv_col} 컬럼이 존재하지 않습니다.")
				else:
					result['recv_info'] = self.recv_columns[recv_col]
					if result['send_info'] and result['recv_info']:
						send_info = result['send_info']
						recv_info = result['recv_info']
						
						# 타입 비교
						type_diff = self.check_type_diff(send_info, recv_info)
						if type_diff:
							result['type_diff'] = type_diff
							result['warnings'].append(f"타입이 다릅니다: 송신({send_info['type']}) vs 수신({recv_info['type']})")
							has_warning = True
							
						# 크기 비교
						size_diff = self.check_size_diff(send_info, recv_info)
						if size_diff:
							result['size_diff'] = size_diff
							result['warnings'].append(f"크기가 다릅니다: {size_diff}")
							has_warning = True
							
						# 1024 바이트 초과 여부 확인
						size_over = self.check_size_over_1024(send_info)
						if size_over:
							result['size_over'] = size_over
							result['warnings'].append("송신 컬럼 크기가 1024 바이트를 초과합니다.")
							has_warning = True
							
						# Nullable 비교
						nullable_diff = self.check_nullable_diff(send_info, recv_info)
						if nullable_diff:
							result['nullable_diff'] = nullable_diff
							result['warnings'].append(f"NULL 허용 여부가 다릅니다: 송신({send_info['nullable']}) vs 수신({recv_info['nullable']})")
							has_warning = True

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
		"""크기 차이 체크"""
		if not isinstance(send, dict) or not isinstance(recv, dict):
			return "컬럼 정보 형식 오류"
		if 'type' not in send or 'type' not in recv or 'size' not in send or 'size' not in recv:
			return "컬럼 크기 정보 누락"
			
		# DATE 타입이 포함된 경우 크기 비교하지 않음
		if 'DATE' in (send['type'], recv['type']):
			return ""
			
		# VARCHAR 계열 타입끼리만 크기 비교
		if (send['type'] in ["VARCHAR", "VARCHAR2", "CHAR"]) and (recv['type'] in ["VARCHAR", "VARCHAR2", "CHAR"]):
			try:
				send_size = int(send['size'])
				recv_size = int(recv['size'])
				if send_size > recv_size:
					return f"송신({send_size}) > 수신({recv_size})"
				return ""
			except ValueError:
				return "크기 변환 오류"
		return ""

	def check_size_over_1024(self, col_info):
		"""1024 바이트 초과 체크"""
		if not isinstance(col_info, dict):
			return "컬럼 정보 형식 오류"
		if 'type' not in col_info or 'size' not in col_info:
			return "컬럼 크기 정보 누락"
			
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
		
		if send_nullable not in ['Y', 'N']:
			return f"송신 Nullable 값 오류: {send_nullable}"
		if recv_nullable not in ['Y', 'N']:
			return f"수신 Nullable 값 오류: {recv_nullable}"
		
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
		"""매핑 정보로부터 필드 XML 생성"""
		if not self.send_mapping or not self.send_columns:
			return ""
		
		lines = []
		lines.append(f'<field count="{len(self.send_mapping)}">')
		
		for idx, send_col in enumerate(self.send_mapping, 1):
			if send_col in self.send_columns:
				col_info = self.send_columns[send_col]
				recv_col = self.recv_mapping[idx-1] if idx <= len(self.recv_mapping) else ""
				
				lines.append(f'    <field key="{idx-1}" nofetch="0" name="{send_col}"></field>')
				lines.append(f'    <col{idx}>')
				lines.append(f'        <length>{col_info["size"]}</length>')
				lines.append(f'        <type>{col_info["type"]}</type>')
				lines.append(f'        <n>{send_col}</n>')
				if recv_col:
					lines.append(f'        <mapping>{recv_col}</mapping>')
				lines.append(f'    </col{idx}>')
		
		lines.append('</field>')
		return '\n'.join(lines)

	def generate_receive_insert_into(self, column_list, columns_info, base_query):
		"""수신 INSERT 문의 INTO 부분 생성
		Args:
			column_list: 컬럼 목록
			columns_info: 컬럼 정보 딕셔너리
			base_query: 기본 쿼리 ($E$3에 해당)
		
		Returns:
			INTO 절 문자열
		"""
		sql_parts = [
			"EAI_SEQ_ID", "DATA_INTERFACE_TYPE_CODE",
			"EAI_INTERFACE_DATE", "APPLICATION_TRANSFER_FLAG"
		]
		for col in column_list:
			if not col:
				continue
			if col not in columns_info:
				continue
			sql_parts.append(col)
		formatted_parts = []
		for i in range(0, len(sql_parts), 2):
			pair = []
			pair.append(sql_parts[i])
			if i + 1 < len(sql_parts):
				pair.append(sql_parts[i+1])
			formatted_parts.append(f"    {', '.join(pair)}")
		if formatted_parts:
			formatted_parts[-1] = formatted_parts[-1].rstrip(',')
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
		sql_parts = [
			":EAI_SEQ_ID", ":DATA_INTERFACE_TYPE_CODE",
			"SYSDATE", "'N'"
		]
		for idx, recv_col in enumerate(column_list):
			if not recv_col:
				continue
			if recv_col not in columns_info:
				continue
			send_col = self.send_mapping[idx] if idx < len(self.send_mapping) else None
			if not send_col:
				continue
			col_info = columns_info[recv_col]
			if col_info['type'] == 'DATE':
				sql_parts.append(f"TO_DATE(:{send_col}, 'YYYYMMDDHH24MISS')")
			else:
				sql_parts.append(f":{send_col}")
		formatted_parts = []
		for i in range(0, len(sql_parts), 2):
			pair = []
			pair.append(sql_parts[i])
			if i + 1 < len(sql_parts):
				pair.append(sql_parts[i+1])
			formatted_parts.append(f"    {', '.join(pair)}")
		if formatted_parts:
			formatted_parts[-1] = formatted_parts[-1].rstrip(',')
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
		columns_sql = self.generate_send_sql(column_list, columns_info, "")
		if not columns_sql:
			return ""
		sql = f"SELECT\n{columns_sql}"
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
		sql_parts = ["EAI_SEQ_ID", "DATA_INTERFACE_TYPE_CODE"]
		for col in column_list:
			if not col:
				continue
			if col not in columns_info:
				continue
			col_info = columns_info[col]
			if col_info['type'] == 'DATE':
				sql_parts.append(f"TO_CHAR({col}, 'YYYYMMDDHH24MISS')")
			else:
				sql_parts.append(col)
		formatted_parts = []
		for i in range(0, len(sql_parts), 2):
			pair = []
			pair.append(sql_parts[i])
			if i + 1 < len(sql_parts):
				pair.append(sql_parts[i+1])
			formatted_parts.append(f"    {', '.join(pair)}")
		if formatted_parts:
			formatted_parts[-1] = formatted_parts[-1].rstrip(',')
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
		base_query = "INSERT INTO "
		if table_info['owner']:
			base_query += f"{table_info['owner']}."
		base_query += f"{table_info['table_name']} ("
		into_part = self.generate_receive_insert_into(column_list, columns_info, "")
		values_part = self.generate_receive_insert_values(column_list, columns_info, "")
		return f"{base_query}\n{into_part}\n) VALUES (\n{values_part}\n)"

if __name__ == "__main__":
	# 테스트를 위한 DB 연결 정보
	SEND_DB_INFO = {
		'sid': 'your_send_sid',
		'username': 'your_send_username',
		'password': 'your_send_password'
	}
	
	# 테스트 실행
	def run_test():
		mapper = ColumnMapper()
		try:
			mapper.connect_send_db(SEND_DB_INFO['sid'], SEND_DB_INFO['username'], SEND_DB_INFO['password'])
			print("송신 DB 연결 성공")
		except Exception as e:
			print(f"송신 DB 연결 실패: {str(e)}")

	run_test()