import openpyxl
import ast
from maptest import ColumnMapper
import os

def read_interface_block(ws, start_col):
    """Excel에서 3컬럼 단위로 하나의 인터페이스 정보를 읽습니다."""
    interface_info = {
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

def process_interface(interface_info, mapper):
    """하나의 인터페이스에 대한 모든 처리를 수행합니다."""
    if not interface_info:
        return {
            'comparison': None,
            'send_sql': None,
            'recv_sql': None,
            'field_xml': None,
            'errors': ['인터페이스 정보를 읽을 수 없습니다.']
        }
    
    results = {
        'comparison': None,
        'send_sql': None,
        'recv_sql': None,
        'field_xml': None,
        'errors': []
    }
    
    try:
        # DB 연결
        if interface_info['send']['db_info']:
            mapper.connect_send_db(
                interface_info['send']['db_info']['sid'],
                interface_info['send']['db_info']['username'],
                interface_info['send']['db_info']['password']
            )
        else:
            results['errors'].append("송신 DB 연결 정보가 없습니다.")
            
        if interface_info['recv']['db_info']:
            mapper.connect_recv_db(
                interface_info['recv']['db_info']['sid'],
                interface_info['recv']['db_info']['username'],
                interface_info['recv']['db_info']['password']
            )
        else:
            results['errors'].append("수신 DB 연결 정보가 없습니다.")
        
        # 송신 테이블 설정
        if interface_info['send']['owner'] and interface_info['send']['table_name']:
            mapper.set_send_table(interface_info['send']['owner'], interface_info['send']['table_name'])
        else:
            results['errors'].append("송신 테이블 정보가 없습니다.")
        
        # 수신 테이블 설정
        if interface_info['recv']['owner'] and interface_info['recv']['table_name']:
            mapper.set_recv_table(interface_info['recv']['owner'], interface_info['recv']['table_name'])
        else:
            results['errors'].append("수신 테이블 정보가 없습니다.")
        
        # 매핑 설정
        send_columns = '\n'.join(interface_info['send']['columns'])
        recv_columns = '\n'.join(interface_info['recv']['columns'])
        mapper.set_send_mapping(send_columns)
        mapper.set_recv_mapping(recv_columns)
        
        # 컬럼 비교 수행
        results['comparison'] = mapper.compare_columns()
        
        # SQL 생성
        results['send_sql'] = mapper.generate_send_sql_from_mapping()
        results['recv_sql'] = mapper.generate_recv_sql()
        
        # 필드 XML 생성
        results['field_xml'] = mapper.generate_field_xml_from_mapping()
        
    except Exception as e:
        results['errors'].append(str(e))
    finally:
        mapper.close_connections()
    
    return results

def write_results_to_excel(wb, interface_results, sheet_name='인터페이스 결과'):
    """처리 결과를 Excel 시트에 기록합니다."""
    if sheet_name in wb.sheetnames:
        wb.remove(wb[sheet_name])
    ws = wb.create_sheet(sheet_name)
    
    # 헤더 작성
    headers = ['구분', '송신테이블', '수신테이블', '컬럼비교결과', '송신SQL', '수신SQL', '필드XML', '오류']
    ws.append(headers)
    
    # 각 인터페이스 결과 기록
    for idx, (interface_info, results) in enumerate(interface_results):
        row = [
            f'인터페이스 {idx+1}',
            f"{interface_info['send']['owner']}.{interface_info['send']['table_name']}",
            f"{interface_info['recv']['owner']}.{interface_info['recv']['table_name']}",
            str(results['comparison']),
            results['send_sql'],
            results['recv_sql'],
            results['field_xml'],
            '\n'.join(results['errors'])
        ]
        ws.append(row)
    
    # 컬럼 너비 자동 조정
    for column in ws.columns:
        max_length = 0
        column = [cell for cell in column]
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column[0].column_letter].width = min(adjusted_width, 100)

def main():
    try:
        # Excel 파일 로드
        xlsx_path = 'input.xlsx'
        wb = openpyxl.load_workbook(xlsx_path)
        ws = wb.active
        
        # 각 인터페이스 블록 처리
        interface_results = []
        current_col = 2  # B열부터 시작
        while current_col <= ws.max_column:
            interface_info = read_interface_block(ws, current_col)
            if not interface_info:
                break
                
            mapper = ColumnMapper()
            results = process_interface(interface_info, mapper)
            interface_results.append((interface_info, results))
            current_col += 3  # 다음 인터페이스 블록으로
        
        # 결과를 Excel에 기록
        write_results_to_excel(wb, interface_results)
        wb.save(xlsx_path)
        print('인터페이스 처리 완료')
        
    except Exception as e:
        print(f'오류 발생: {str(e)}')
    finally:
        if 'wb' in locals():
            wb.close()

if __name__ == "__main__":
    main()
