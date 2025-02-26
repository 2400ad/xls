"""
간단한 테스트 스크립트 - SQL 쿼리 파싱 기능 검증
"""
from comp_q import QueryParser

def test_simple_parsing():
    """
    파싱 기능을 단계별로 테스트합니다.
    """
    parser = QueryParser()
    
    # 간단한 쿼리로 테스트
    query = "SELECT col1, col2, to_char(tdate, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    
    print("1. 쿼리 정규화")
    norm_query = parser.normalize_query(query)
    print(f"Original: {query}")
    print(f"Normalized: {norm_query}")
    
    print("\n2. 테이블명 추출")
    table = parser.extract_table_name(query)
    print(f"Extracted table: {table}")
    
    print("\n3. SELECT 컬럼 파싱")
    try:
        columns = parser.parse_select_columns(query)
        print(f"Parsed columns: {columns}")
    except Exception as e:
        print(f"Error parsing columns: {e}")
    
    print("\nTest completed.")

if __name__ == "__main__":
    test_simple_parsing()
