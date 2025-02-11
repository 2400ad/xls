import openpyxl
import ast
from maptest import ColumnMapper
import os

def read_interface_block(ws, start_col):
	"""Excel에서 3컬럼 단위로 하나의 인터페이스 정보를 읽습니다.
	
	Args:
		ws: Worksheet
		start_col: 시작 컬럼 인덱스 (1부터 시작)
	
	Returns:
		interface_info: 인터페이스 정보를 담은 딕셔너리
	"""
	# 첫번째 컬럼에서 정보 추출
	interface_name = ws.cell(row=1, column=start_col).value
	if not interface_name:  # 더 이상 인터페이스가 없음
		return None
	
	interface_id = ws.cell(row=2, column=start_col).value
	
	try:
		# 문자열로 된 dict를 실제 dict로 변환
		send_db = ast.literal_eval(ws.cell(row=3, column=start_col).value)
		send_table = ast.literal_eval(ws.cell(row=4, column=start_col).value)
		recv_db = ast.literal_eval(ws.cell(row=3, column=start_col+1).value)
		recv_table = ast.literal_eval(ws.cell(row=4, column=start_col+1).value)
	except Exception as e:
		print(f"Error parsing DB/Table info for interface {interface_name}: {str(e)}")
		return None
	
	# 컬럼 정보 시작 행
	start_row = 5
	
	# 빈 값이 아닌 행까지의 컬럼 정보 수집
	send_columns = []
	recv_columns = []
	comments = []
	
	# 먼저 송신 컬럼 목록을 모두 수집
	max_row = ws.max_row
	last_send_row = start_row
	for row in range(start_row, max_row + 1):
		send_val = ws.cell(row=row, column=start_col).value
		if not send_val:
			break
		send_columns.append(send_val)
		last_send_row = row
	
	# 송신 컬럼 개수만큼 수신 컬럼과 코멘트 초기화
	recv_columns = [''] * len(send_columns)
	comments = [''] * len(send_columns)
	
	# 수신 컬럼 매핑
	for row in range(start_row, last_send_row + 1):
		recv_val = ws.cell(row=row, column=start_col+1).value
		comment_val = ws.cell(row=row, column=start_col+2).value
		
		if recv_val:  # 수신 컬럼이 있는 경우만 해당 위치에 매핑
			idx = row - start_row
			recv_columns[idx] = recv_val
			if comment_val:
				comments[idx] = comment_val
	
	return {
		'name': interface_name,
		'id': interface_id,
		'send_db': send_db,
		'send_table': send_table,
		'recv_db': recv_db,
		'recv_table': recv_table,
		'send_columns': send_columns,
		'recv_columns': recv_columns,
		'comments': comments
	}

def analyze_excel():
	"""input.xlsx 파일을 읽고 분석합니다."""
	try:
		# Excel 파일 읽기
		wb = openpyxl.load_workbook('input.xlsx', data_only=True)
		ws = wb.active
		
		# 3컬럼씩 인터페이스 정보 읽기 (B열부터 시작)
		interfaces = []
		for col in range(2, ws.max_column + 1, 3):  # 2는 B열을 의미
			interface_info = read_interface_block(ws, col)
			if interface_info:
				interfaces.append(interface_info)
			else:
				break  # 더 이상 인터페이스가 없음
		
		# 분석 결과 출력
		print(f"총 {len(interfaces)}개의 인터페이스를 발견했습니다.\n")
		
		for interface in interfaces:
			print("="*50)
			print(f"인터페이스 이름: {interface['name']}")
			print(f"인터페이스 ID: {interface['id']}")
			print("\n[송신 정보]")
			print(f"DB 정보: {interface['send_db']}")
			print(f"테이블 정보: {interface['send_table']}")
			print("컬럼 목록:")
			for i, col in enumerate(interface['send_columns']):
				print(f"  {i+1}. {col}")
			
			print("\n[수신 정보]")
			print(f"DB 정보: {interface['recv_db']}")
			print(f"테이블 정보: {interface['recv_table']}")
			print("컬럼 목록:")
			for i, col in enumerate(interface['recv_columns']):
				if col:  # 빈 문자열이 아닌 경우만 출력
					print(f"  {i+1}. {col}")
			
			print("\n[컬럼 매핑]")
			# 송신과 수신 컬럼을 별도로 출력
			print("송신 컬럼:")
			for i, col in enumerate(interface['send_columns']):
				print(f"  {i+1}. {col}")
			
			print("\n수신 컬럼:")
			for i, col in enumerate(interface['recv_columns']):
				if col:  # 빈 문자열이 아닌 경우만 출력
					print(f"  {i+1}. {col}")
			
			print("\n매핑 관계:")
			# 실제 매핑 관계만 출력 (수신 컬럼이 빈 문자열이 아닌 경우)
			for i, (send_col, recv_col) in enumerate(zip(interface['send_columns'], interface['recv_columns'])):
				if recv_col:  # 수신 컬럼이 있는 경우만 매핑 표시
					comment = interface['comments'][i]
					print(f"  {send_col} -> {recv_col}")
					if comment:
						print(f"    설명: {comment}")
				else:
					print(f"  {send_col} -> (매핑 없음)")
			print()
	
	except Exception as e:
		print(f"Error analyzing Excel file: {str(e)}")
	finally:
		if 'wb' in locals():
			wb.close()

def get_interface_info(xlsx_path):
	# input.xlsx 파일의 active sheet에서 1열을 이용하여 인터페이스 정보를 읽어옵니다.
	try:
		wb = openpyxl.load_workbook(xlsx_path)
	except Exception as e:
		print(f'Excel 파일 로드 실패: {e}')
		return {}
	
	ws = wb.active
	interface_info = read_interface_block(ws, 1)
	wb.close()
	return interface_info

def get_mapping_instance(interface_info):
	# 인터페이스 정보에 따라 매핑 인스턴스를 선택합니다.
	# 예시로 maptest 모듈의 ColumnMapper 클래스를 사용합니다.
	return ColumnMapper()

def process_interface_mapping(mapping_instance, interface_info):
	# 매핑 인스턴스를 이용하여 각 기능(컬럼 비교, 송신 SQL, 수신 SQL, 필드 생성)을 실행합니다.
	results = {}
	try:
		results['column_comparison'] = mapping_instance.compare_columns(interface_info)
	except Exception as e:
		results['column_comparison'] = f'Error: {e}'

	try:
		results['send_sql'] = mapping_instance.create_send_sql(interface_info)
	except Exception as e:
		results['send_sql'] = f'Error: {e}'

	try:
		results['recv_sql'] = mapping_instance.create_recv_sql(interface_info)
	except Exception as e:
		results['recv_sql'] = f'Error: {e}'

	try:
		results['field_creation'] = mapping_instance.create_fields(interface_info)
	except Exception as e:
		results['field_creation'] = f'Error: {e}'

	return results

def update_excel_with_results(xlsx_path, results):
	# openpyxl을 사용하여 input.xlsx 파일에 '인터페이스 결과' 시트를 생성 혹은 초기화한 후 결과를 기록합니다.
	try:
		wb = openpyxl.load_workbook(xlsx_path)
	except Exception as e:
		print(f'Excel 파일 로드 실패: {e}')
		return

	if '인터페이스 결과' in wb.sheetnames:
		ws = wb['인터페이스 결과']
		# 기존 시트의 내용을 모두 지웁니다.
		ws.delete_rows(1, ws.max_row)
	else:
		ws = wb.create_sheet('인터페이스 결과')

	# 헤더 작성
	ws.append(['항목', '결과'])

	for key, value in results.items():
		ws.append([key, value])

	wb.save(xlsx_path)
	print('Excel 업데이트 완료')

def main():
	# input.xlsx 파일은 현재 작업 디렉토리에 있다고 가정합니다.
	current_dir = os.getcwd()
	xlsx_path = os.path.join(current_dir, 'input.xlsx')

	# Excel 파일로부터 인터페이스 정보를 읽어옵니다.
	interface_info = get_interface_info(xlsx_path)

	mapping_instance = get_mapping_instance(interface_info)
	results = process_interface_mapping(mapping_instance, interface_info)
	update_excel_with_results(xlsx_path, results)
	print('인터페이스 매핑 및 Excel 업데이트 완료.')

if __name__ == "__main__":
	analyze_excel()
	main()
