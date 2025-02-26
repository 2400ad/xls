"""
comp_q.py 패치 스크립트
"""
import re
import fileinput
import sys

# 변경할 함수 내용
new_parse_select_columns = '''    def parse_select_columns(self, query) -> Optional[Dict[str, Dict]]:
        """Extract columns from SELECT query and return as dictionary"""
        # 정규화된 쿼리 사용
        query = self.normalize_query(query)
        
        # SELECT와 FROM 사이의 컬럼 추출
        match = re.search(r'SELECT\s+(.*?)\s+FROM', query, flags=re.IGNORECASE)
        if not match:
            return None
            
        columns = {}
        # 컬럼 분리시 괄호를 고려하여 처리
        column_text = match.group(1)
        
        try:
            parsed_columns = self._parse_csv_with_functions(column_text)
        
            for col in parsed_columns:
                col = col.strip()
                if not col:  # 빈 문자열인 경우 스킵
                    continue
                    
                # SQL 함수나 표현식에서 별칭 추출
                # AS 키워드가 있는 경우 처리 (대소문자 무시)
                alias_with_as_match = re.search(r'(.+?)\s+AS\s+([a-zA-Z0-9_]+)$', col, re.IGNORECASE)
                
                # AS 키워드 없이 별칭만 있는 경우 처리
                alias_without_as_match = re.search(r'(.+?)\s+([a-zA-Z0-9_]+)$', col)
                
                if alias_with_as_match:
                    # AS 키워드가 있는 경우
                    expr, alias = alias_with_as_match.groups()
                    expr = expr.strip()
                    alias = alias.strip()
                    columns[expr] = {'expr': expr, 'alias': alias, 'full': col}
                elif alias_without_as_match:
                    # AS 키워드 없이 별칭만 있는 경우
                    expr, alias = alias_without_as_match.groups()
                    expr = expr.strip()
                    alias = alias.strip()
                    columns[expr] = {'expr': expr, 'alias': alias, 'full': col}
                else:
                    # 별칭이 없는 경우
                    columns[col] = {'expr': col, 'alias': None, 'full': col}
                    
        except Exception as e:
            # 단순히 쉼표로 분리해서 시도
            for col in column_text.split(','):
                col = col.strip()
                if not col:  # 빈 문자열인 경우 스킵
                    continue
                
                # AS 키워드가 있는 경우 처리 (대소문자 무시)
                alias_with_as_match = re.search(r'(.+?)\s+AS\s+([a-zA-Z0-9_]+)$', col, re.IGNORECASE)
                
                # AS 키워드 없이 별칭만 있는 경우 처리
                alias_without_as_match = re.search(r'(.+?)\s+([a-zA-Z0-9_]+)$', col)
                
                if alias_with_as_match:
                    expr, alias = alias_with_as_match.groups()
                    expr = expr.strip()
                    alias = alias.strip()
                    columns[expr] = {'expr': expr, 'alias': alias, 'full': col}
                elif alias_without_as_match:
                    expr, alias = alias_without_as_match.groups()
                    expr = expr.strip()
                    alias = alias.strip()
                    columns[expr] = {'expr': expr, 'alias': alias, 'full': col}
                else:
                    columns[col] = {'expr': col, 'alias': None, 'full': col}
        
        return columns if columns else None'''

print("패치 스크립트 생성 완료. 아래 명령으로 적용하세요:")
print("python patch_comp_q.py")
