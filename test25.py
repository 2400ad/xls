import openpyxl
import ast

def safe_get_dict_value(dictionary, key, default=''):
    """
    딕셔너리에서 안전하게 값을 가져오는 유틸리티 함수
    
    Args:
        dictionary: 검색할 딕셔너리 (None일 수 있음)
        key: 검색할 키
        default: 기본값 (dictionary가 None이거나 key가 없을 경우 반환)
    
    Returns:
        찾은 값 또는 기본값
    """
    if dictionary is None:
        return default
    return dictionary.get(key, default)

def read_interface_block(ws, start_col):
    """Excel에서 3컬럼 단위로 하나의 인터페이스 정보를 읽습니다."""
    interface_info = {
        'interface_name': ws.cell(row=1, column=start_col).value or '',  # IF NAME (1행)
        'interface_id': ws.cell(row=2, column=start_col).value or '',    # IF ID (2행)
        'send': {'owner': None, 'table_name': None, 'columns': [], 'db_info': None},
        'recv': {'owner': None, 'table_name': None, 'columns': [], 'db_info': None}
    }
    
    try:
        # DB 연결 정보 (3행에서 읽기)
        send_db_info = ast.literal_eval(ws.cell(row=3, column=start_col).value)
        recv_db_info = ast.literal_eval(ws.cell(row=3, column=start_col + 1).value)
        interface_info['send']['db_info'] = send_db_info
        interface_info['recv']['db_info'] = recv_db_info
        
        # 테이블 정보 (4행에서 읽기)
        send_table_info = ast.literal_eval(ws.cell(row=4, column=start_col).value)
        recv_table_info = ast.literal_eval(ws.cell(row=4, column=start_col + 1).value)
        
        interface_info['send']['owner'] = send_table_info.get('owner')
        interface_info['send']['table_name'] = send_table_info.get('table_name')
        interface_info['recv']['owner'] = recv_table_info.get('owner')
        interface_info['recv']['table_name'] = recv_table_info.get('table_name')
        
        # 컬럼 매핑 정보 (5행부터)
        row = 5
        while True:
            send_col = ws.cell(row=row, column=start_col).value
            recv_col = ws.cell(row=row, column=start_col + 1).value
            
            if not send_col and not recv_col:
                break
                
            interface_info['send']['columns'].append(send_col if send_col else '')
            interface_info['recv']['columns'].append(recv_col if recv_col else '')
            row += 1
            
    except Exception as e:
        print(f'인터페이스 정보 읽기 중 오류 발생: {str(e)}')
        return None
    
    return interface_info

# 테스트 코드
if __name__ == "__main__":
    try:
        # 예시: input.xlsx 파일 로드
        input_xlsx_path = 'input.xlsx'
        wb_input = openpyxl.load_workbook(input_xlsx_path)
        ws_input = wb_input.active
        
        # 첫 번째 인터페이스 블록 읽기 (B열부터 시작)
        interface_info = read_interface_block(ws_input, 2)
        
        if interface_info:
            print(f"인터페이스 이름: {interface_info['interface_name']}")
            print(f"인터페이스 ID: {interface_info['interface_id']}")
            print(f"송신 테이블: {interface_info['send']['owner']}.{interface_info['send']['table_name']}")
            print(f"수신 테이블: {interface_info['recv']['owner']}.{interface_info['recv']['table_name']}")
            print(f"송신 컬럼 수: {len(interface_info['send']['columns'])}")
            print(f"수신 컬럼 수: {len(interface_info['recv']['columns'])}")
        else:
            print("인터페이스 정보를 읽을 수 없습니다.")
            
        wb_input.close()
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")