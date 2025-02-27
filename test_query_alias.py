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
    query1 = "select to_char(tdate, 'YYYYMMDDHH24MISS') tdate from tb_test"
    query2 = "select to_char(tdate, 'YYYYMMDDHH24MISS')    from tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 1 (별칭 있음 vs 없음): {'성공' if result.is_equal else '실패'}")
    if not result.is_equal:
        print(result)
    
    # 테스트 2: 동일한 쿼리, 두개의 to_char format이 다름
    query1 = "select to_char(tdate, 'YYYYMMDDHH24MISS') tdate from tb_test"
    query2 = "select to_char(tdate, 'YYYY-MM-DD HH24:MI:SS')  from tb_test"
    result = parser.compare_queries(query1, query2)
    print(f"테스트 2 (다른 to_char 포맷): {'성공' if result.is_equal else '실패'}")
    if not result.is_equal:
        print(result)

    print("\n모든 테스트가 완료되었습니다.")

if __name__ == "__main__":
    test_alias_handling()
