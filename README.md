# xlstest
# xlstest
# xls

comp_q.py의 디버깅

# 276줄과 284줄
col_names = [c.strip() for c in columns_match.group(1).split(',')]
col_values = [v.strip() for v in values_match.group(1).split(',')]


가능한 솔루션:
1. 정규식을 더 엄격하게 수정
# 276줄과 284줄 대체
col_names = [re.sub(r'\s+', '', c) for c in columns_match.group(1).split(',')]
col_values = [re.sub(r'\s+', '', v) for v in values_match.group(1).split(',')]

2. 문자열 매핑 로직 강화
291줄
# 컬럼과 값 매핑 전 로깅 추가
print(f"열 이름: {col_names}")
print(f"열 값: {col_values}")

# 매핑 검증
if len(col_names) != len(col_values):
    print(f"경고: 열 개수 불일치 - 열 이름: {len(col_names)}, 열 값: {len(col_values)}")
    