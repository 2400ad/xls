import pandas as pd
import ast
from maptest import ColumnMapper

def read_interface_block(df, start_col):
    """Excel에서 3컬럼 단위로 하나의 인터페이스 정보를 읽습니다.
    
    Args:
        df: DataFrame
        start_col: 시작 컬럼 인덱스 (0부터 시작)
    
    Returns:
        interface_info: 인터페이스 정보를 담은 딕셔너리
    """
    # 컬럼 이름 가져오기
    cols = df.columns[start_col:start_col+3]
    if len(cols) < 3:
        return None
        
    # 첫번째 컬럼에서 정보 추출
    interface_name = df.iloc[0, start_col]
    interface_id = df.iloc[1, start_col]
    
    try:
        # 문자열로 된 dict를 실제 dict로 변환
        send_db = ast.literal_eval(df.iloc[2, start_col])
        send_table = ast.literal_eval(df.iloc[3, start_col])
        recv_db = ast.literal_eval(df.iloc[2, start_col+1])
        recv_table = ast.literal_eval(df.iloc[3, start_col+1])
    except Exception as e:
        print(f"Error parsing DB/Table info for interface {interface_name}: {str(e)}")
        return None
    
    # 컬럼 정보 시작 행
    start_row = 4
    
    # 빈 값이 아닌 행까지의 컬럼 정보 수집
    send_columns = []
    recv_columns = []
    comments = []
    
    for idx, row in df.iloc[start_row:].iterrows():
        if pd.isna(row[start_col]) and pd.isna(row[start_col+1]):
            break
            
        if not pd.isna(row[start_col]):
            send_columns.append(row[start_col])
        if not pd.isna(row[start_col+1]):
            recv_columns.append(row[start_col+1])
        if not pd.isna(row[start_col+2]):
            comments.append(row[start_col+2])
    
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
        df = pd.read_excel('input.xlsx', header=None)
        
        # 3컬럼씩 인터페이스 정보 읽기
        interfaces = []
        for i in range(0, len(df.columns), 3):
            interface_info = read_interface_block(df, i)
            if interface_info:
                interfaces.append(interface_info)
        
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
                print(f"  {i+1}. {col}")
            
            print("\n[컬럼 매핑]")
            for i in range(min(len(interface['send_columns']), len(interface['recv_columns']))):
                comment = interface['comments'][i] if i < len(interface['comments']) else ""
                print(f"  {interface['send_columns'][i]} -> {interface['recv_columns'][i]}")
                if comment:
                    print(f"    설명: {comment}")
            print()
            
    except Exception as e:
        print(f"Error analyzing Excel file: {str(e)}")

if __name__ == "__main__":
    analyze_excel()
