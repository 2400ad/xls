"""
쿼리 별칭 처리 테스트 모듈
"""
import sys
from comp_q import QueryParser

def test_alias_handling():
    """
    별칭 처리 로직을 테스트합니다.
    다양한 형태의 별칭이 있는 쿼리와 없는 쿼리를 비교합니다.
    """
    parser = QueryParser()
    
    # 테스트 1: 동일한 쿼리, 하나는 별칭 있고 하나는 별칭 없음
    query1 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    query2 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') FROM tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 1 (별칭 있음 vs 없음): {'성공' if result.is_equal else '실패'}")
    if not result.is_equal:
        print(result)
    
    # 테스트 2: 별칭 키워드(AS) 사용 여부
    query1 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    query2 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') tdate FROM tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 2 (AS 키워드 유무): {'성공' if result.is_equal else '실패'}")
    if not result.is_equal:
        print(result)
    
    # 테스트 3: 여러 공백이 있는 경우
    query1 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS')    tdate FROM tb_test"
    query2 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') tdate FROM tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 3 (공백 차이): {'성공' if result.is_equal else '실패'}")
    if not result.is_equal:
        print(result)
    
    # 테스트 4: 대소문자 차이
    query1 = "SELECT TO_CHAR(tdate, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    query2 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 4 (대소문자 차이): {'성공' if result.is_equal else '실패'}")
    if not result.is_equal:
        print(result)
    
    # 테스트 5: 함수 내부 공백 차이
    query1 = "SELECT to_char(tdate,'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    query2 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 5 (함수 내부 공백 차이): {'성공' if result.is_equal else '실패'}")
    if not result.is_equal:
        print(result)
    
    # 테스트 6: 실제 다른 쿼리
    query1 = "SELECT to_char(tdate, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    query2 = "SELECT to_char(updated_at, 'YYYYMMDDHH24MISS') AS tdate FROM tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 6 (실제 다른 쿼리): {'성공' if not result.is_equal else '실패'}")
    if result.is_equal:
        print("두 쿼리가 같다고 판단되었지만 실제로는 다릅니다.")
    else:
        print("정상적으로 쿼리 차이를 감지했습니다:")
        print(result)
    
    print("\n모든 테스트가 완료되었습니다.")

if __name__ == "__main__":
    test_alias_handling()
