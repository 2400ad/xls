import os
import sys
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Optional

class XMLQueryValidator:
    """
    XML 파일 내부의 SQL 쿼리를 검증하는 클래스
    """
    
    def __init__(self):
        """초기화 메서드"""
        self.query_parser = QueryParser()  # 기존 QueryParser 활용
    
    def extract_from_xml(self, xml_path: str) -> Tuple[str, str, List[str], int]:
        """
        XML 파일에서 쿼리, XML 내용, 필드 이름 목록 및 필드 수를 추출합니다.
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            Tuple[str, str, List[str], int]: (쿼리, XML 내용, 필드 이름 목록, 필드 수)
        """
        try:
            # XML 파일이 제대로 로드되었는지 확인
            if not os.path.exists(xml_path):
                print(f"Warning: XML file not found: {xml_path}")
                return None, None, [], 0
                
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # XML 내용이 유효한지 확인
            if root is None:
                print(f"Warning: Invalid XML content in file: {xml_path}")
                return None, None, [], 0
            
            # SQL 노드 찾기
            sql_node = root.find(".//SQL")
            if sql_node is None or not sql_node.text:
                print(f"Warning: No SQL content found in file: {xml_path}")
                return None, None, [], 0
            
            # fields 태그 찾기
            fields_tag = root.find(".//fields")
            field_names = []
            fields_count = 0
            
            if fields_tag is not None:
                # fields의 count 속성 확인
                fields_count = int(fields_tag.get('count', '0'))
                
                # 모든 field 태그를 찾아 name 속성 값 추출
                for field_tag in fields_tag.findall(".//field"):
                    name = field_tag.get('name')
                    if name:
                        field_names.append(name.lower())  # 소문자로 통일하여 비교
            
            query = sql_node.text.strip()
            xml_content = ET.tostring(root, encoding='unicode')
            
            # 추출된 쿼리가 유효한지 확인
            if not query:
                print(f"Warning: Empty SQL query in file: {xml_path}")
                return None, None, [], 0
            
            return query, xml_content, field_names, fields_count
            
        except ET.ParseError as e:
            print(f"Error parsing XML file {xml_path}: {e}")
            return None, None, [], 0
        except Exception as e:
            print(f"Unexpected error processing file {xml_path}: {e}")
            return None, None, [], 0
    
    def validate_xml_file(self, xml_path: str) -> Dict:
        """
        XML 파일을 파싱하고 내부의 SQL 쿼리의 포맷을 검증합니다.
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            Dict: 검증 결과
        """
        result = {
            'valid': False,
            'xml_structure': False,
            'select_queries': [],
            'insert_queries': [],
            'errors': [],
            'xml_content': None,
            'raw_query': None,
            'field_names': [],
            'fields_count': 0
        }
        
        try:
            # XML 파일이 존재하는지 확인
            if not os.path.exists(xml_path):
                result['errors'] = [f"XML 파일을 찾을 수 없습니다: {xml_path}"]
                return result
                
            # XML에서 쿼리와 필드 정보 추출
            query, xml_content, field_names, fields_count = self.extract_from_xml(xml_path)
            
            if not query or not xml_content:
                result['errors'].append(f"XML 파일에서 쿼리를 추출할 수 없습니다: {xml_path}")
                return result
            
            # 추출된 원본 쿼리와 XML 저장 (로깅 목적)
            result['raw_query'] = query
            result['xml_content'] = xml_content
            result['xml_structure'] = True
            result['field_names'] = field_names
            result['fields_count'] = fields_count
            
            print(f"\n추출된 원본 쿼리:\n{query}\n")
            print(f"추출된 필드 이름: {field_names}")
            print(f"필드 개수: {fields_count}")
            
            # XML에서 쿼리 유형 판별 (SELECT 또는 INSERT)
            if query.strip().upper().startswith('SELECT'):
                # SELECT 쿼리 검증
                query_result = self.validate_select_query(query, field_names)
                result['select_queries'].append(query_result)
            elif query.strip().upper().startswith('INSERT'):
                # INSERT 쿼리 검증
                query_result = self.validate_insert_query(query, fields_count)
                result['insert_queries'].append(query_result)
            else:
                # 다른 유형의 쿼리는 현재 지원하지 않음
                result['errors'].append(f"지원되지 않는 쿼리 유형: {query[:30]}...")
            
            # 전체 결과에 대한 유효성 결정
            if all(q['valid'] for q in result['select_queries'] + result['insert_queries']):
                result['valid'] = True
                
            return result
            
        except Exception as e:
            result['errors'] = [f"예상치 못한 오류: {str(e)}"]
            return result
    
    def extract_select_columns(self, query: str) -> List[str]:
        """
        SELECT 쿼리에서 컬럼 이름을 추출합니다.
        
        Args:
            query (str): SELECT 쿼리
            
        Returns:
            List[str]: 추출된 컬럼 이름 목록
        """
        # SELECT와 FROM 사이의 컬럼 부분 추출
        match = re.match(r'SELECT\s+(.+?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        
        columns_text = match.group(1).strip()
        
        # '*'인 경우 모든 컬럼을 선택하는 것이므로 비어있는 리스트 반환
        if columns_text == '*':
            return []
        
        # 컬럼 이름 분리
        columns = []
        
        # 괄호 내 서브쿼리나 함수 호출을 처리하기 위한 변수
        in_parentheses = 0
        current_column = ""
        
        for char in columns_text:
            if char == ',' and in_parentheses == 0:
                # 컬럼 구분자를 만났고 괄호 내부가 아닌 경우
                if current_column.strip():
                    # 앨리어스 제거 (AS 키워드 다음이나 공백 다음의 이름)
                    column_name = re.sub(r'(?i)\s+AS\s+\w+$|\s+\w+$', '', current_column.strip())
                    # 테이블 접두사 제거 (schema.table.column 또는 table.column 형식)
                    column_name = column_name.strip().split('.')[-1].lower()
                    columns.append(column_name)
                current_column = ""
            else:
                if char == '(':
                    in_parentheses += 1
                elif char == ')' and in_parentheses > 0:
                    in_parentheses -= 1
                current_column += char
        
        # 마지막 컬럼 처리
        if current_column.strip():
            column_name = re.sub(r'(?i)\s+AS\s+\w+$|\s+\w+$', '', current_column.strip())
            column_name = column_name.strip().split('.')[-1].lower()
            columns.append(column_name)
        
        return columns
    
    def validate_select_query(self, query: str, xml_field_names: List[str]) -> Dict:
        """
        SELECT 쿼리의 유효성을 검사합니다.
        
        Args:
            query (str): 검사할 SELECT 쿼리
            xml_field_names (List[str]): XML에서 추출한 필드 이름 목록
            
        Returns:
            Dict: 검증 결과
        """
        result = {
            'query': query,
            'valid': False,
            'has_columns': False,
            'has_table': False,
            'columns_match_xml_fields': False,
            'extracted_columns': [],
            'errors': []
        }
        
        # 기본 SELECT 쿼리 구조 확인
        if not query.strip().upper().startswith('SELECT'):
            result['errors'].append("쿼리가 SELECT로 시작하지 않습니다.")
            return result
            
        # SELECT 절과 FROM 절 추출
        select_from_match = re.match(r'SELECT\s+(.+?)\s+FROM\s+(.+?)(?:\s+WHERE|\s*$)', 
                                    query, 
                                    re.IGNORECASE | re.DOTALL)
                                    
        if not select_from_match:
            result['errors'].append("쿼리에서 SELECT와 FROM 사이의 컬럼을 찾을 수 없습니다.")
            return result
            
        columns_str = select_from_match.group(1).strip()
        table_str = select_from_match.group(2).strip()
        
        # 컬럼 확인
        if columns_str and columns_str != '*':
            result['has_columns'] = True
        elif columns_str == '*':
            result['has_columns'] = True  # '*'도 유효한 컬럼으로 간주
        else:
            result['errors'].append("쿼리에 컬럼이 지정되지 않았습니다.")
            
        # 테이블 확인  
        if table_str:
            result['has_table'] = True
        else:
            result['errors'].append("쿼리에 테이블명이 지정되지 않았습니다.")
        
        # 쿼리에서 컬럼 추출
        extracted_columns = self.extract_select_columns(query)
        result['extracted_columns'] = extracted_columns
        
        # XML 필드 이름과 쿼리 컬럼 비교
        if extracted_columns and xml_field_names:
            # 모든 컬럼 이름이 XML 필드에 있는지 확인
            missing_columns = [col for col in extracted_columns if col.lower() not in [f.lower() for f in xml_field_names]]
            
            if not missing_columns:
                result['columns_match_xml_fields'] = True
            else:
                result['errors'].append(f"쿼리 컬럼과 XML 필드 이름이 일치하지 않습니다. 누락된 컬럼: {missing_columns}")
        elif not xml_field_names:
            result['errors'].append("XML에 <fields> 태그가 없거나 <field> 태그의 name 속성이 없습니다.")
        
        # 종합 유효성 판단
        if (result['has_columns'] and result['has_table'] and 
            (result['columns_match_xml_fields'] or not xml_field_names)):
            result['valid'] = True
            
        return result
    
    def extract_insert_columns_and_values(self, query: str) -> Tuple[List[str], List[str]]:
        """
        INSERT 쿼리에서 컬럼 이름과 값을 추출합니다.
        
        Args:
            query (str): INSERT 쿼리
            
        Returns:
            Tuple[List[str], List[str]]: (컬럼 목록, 값 목록)
        """
        columns = []
        values = []
        
        # 컬럼 추출 (INSERT INTO table_name (col1, col2, ...) VALUES ...)
        columns_match = re.search(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', query, re.IGNORECASE | re.DOTALL)
        if columns_match:
            columns_str = columns_match.group(1).strip()
            columns = [col.strip().lower() for col in columns_str.split(',')]
        
        # VALUES 절 추출
        values_match = re.search(r'VALUES\s*\(([^)]+)\)', query, re.IGNORECASE | re.DOTALL)
        if values_match:
            values_str = values_match.group(1).strip()
            
            # 값 파싱 (문자열 내의 쉼표 처리)
            in_string = False
            string_delimiter = None
            current_value = ""
            
            for char in values_str:
                if char in ("'", '"') and (not in_string or char == string_delimiter):
                    if in_string:
                        in_string = False
                    else:
                        in_string = True
                        string_delimiter = char
                    current_value += char
                elif char == ',' and not in_string:
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            
            # 마지막 값 추가
            if current_value.strip():
                values.append(current_value.strip())
        
        return columns, values
        
    def validate_insert_query(self, query: str, fields_count: int) -> Dict:
        """
        INSERT 쿼리의 유효성을 검사합니다.
        
        Args:
            query (str): 검사할 INSERT 쿼리
            fields_count (int): XML의 <fields> 태그의 count 속성 값
            
        Returns:
            Dict: 검증 결과
        """
        result = {
            'query': query,
            'valid': False,
            'columns_count': 0,
            'values_count': 0,
            'columns_values_match': False,
            'xml_fields_count_valid': False,
            'columns_values_mapping': {},
            'errors': []
        }
        
        # 기본 INSERT 쿼리 구조 확인
        if not query.strip().upper().startswith('INSERT INTO'):
            result['errors'].append("쿼리가 INSERT INTO로 시작하지 않습니다.")
            return result
        
        # 컬럼과 값 추출
        columns, values = self.extract_insert_columns_and_values(query)
        
        if not columns:
            result['errors'].append("INSERT 쿼리에서 컬럼 리스트를 찾을 수 없습니다.")
            return result
            
        if not values:
            result['errors'].append("INSERT 쿼리에서 VALUES 절을 찾을 수 없습니다.")
            return result
        
        result['columns_count'] = len(columns)
        result['values_count'] = len(values)
        
        # 컬럼과 값 매핑
        if len(columns) == len(values):
            result['columns_values_match'] = True
            result['columns_values_mapping'] = dict(zip(columns, values))
        else:
            result['errors'].append(
                f"컬럼 수({len(columns)})와 값 수({len(values)})가 일치하지 않습니다."
            )
        
        # XML의 <fields> 태그 count 속성 확인
        if fields_count >= 2:
            result['xml_fields_count_valid'] = True
        else:
            result['errors'].append(f"XML의 <fields> 태그 count가 2 미만입니다: {fields_count}")
        
        # 종합 유효성 판단
        if result['columns_values_match'] and result['xml_fields_count_valid']:
            result['valid'] = True
            
        return result

def validate_xml_files_in_directory(directory: str, output_file: str = None):
    """
    지정된 디렉토리의 모든 XML 파일을 검증하고 결과를 출력합니다.
    
    Args:
        directory (str): 검색할 디렉토리 경로
        output_file (str, optional): 결과를 저장할 파일 경로
    """
    validator = XMLQueryValidator()
    results = []
    
    # 디렉토리 내의 모든 XML 파일 검색
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.xml'):
                file_path = os.path.join(root, file)
                print(f"검증 중: {file_path}")
                
                # XML 파일 검증
                result = validator.validate_xml_file(file_path)
                result['file_path'] = file_path
                results.append(result)
                
                # 결과 요약 출력
                print(f"  XML 구조 유효성: {'O' if result['xml_structure'] else 'X'}")
                print(f"  필드 이름: {result['field_names']}")
                print(f"  필드 개수: {result['fields_count']}")
                print(f"  SELECT 쿼리 수: {len(result['select_queries'])}")
                print(f"  INSERT 쿼리 수: {len(result['insert_queries'])}")
                print(f"  전체 유효성: {'O' if result['valid'] else 'X'}")
                
                # 원본 쿼리 출력 (로깅)
                if result['raw_query']:
                    print(f"\n  원본 쿼리 (처음 100자):")
                    print(f"    {result['raw_query'][:100]}...")
                
                # 오류가 있는 경우 자세한 정보 출력
                if not result['valid']:
                    print("  오류 세부 정보:")
                    
                    for i, query_result in enumerate(result['select_queries']):
                        if not query_result['valid']:
                            print(f"    SELECT 쿼리 #{i+1}:")
                            for error in query_result['errors']:
                                print(f"      - {error}")
                                
                    for i, query_result in enumerate(result['insert_queries']):
                        if not query_result['valid']:
                            print(f"    INSERT 쿼리 #{i+1}:")
                            for error in query_result['errors']:
                                print(f"      - {error}")
                
                print("")
    
    # 결과를 파일로 저장 (선택 사항)
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("XML 쿼리 검증 결과\n")
            f.write("=" * 50 + "\n\n")
            
            for result in results:
                f.write(f"파일: {result['file_path']}\n")
                f.write(f"XML 구조 유효성: {'O' if result['xml_structure'] else 'X'}\n")
                f.write(f"필드 이름: {result['field_names']}\n")
                f.write(f"필드 개수: {result['fields_count']}\n")
                f.write(f"SELECT 쿼리 수: {len(result['select_queries'])}\n")
                f.write(f"INSERT 쿼리 수: {len(result['insert_queries'])}\n")
                f.write(f"전체 유효성: {'O' if result['valid'] else 'X'}\n")
                
                if result['raw_query']:
                    f.write(f"원본 쿼리:\n{result['raw_query']}\n")
                
                if not result['valid']:
                    f.write("오류 세부 정보:\n")
                    
                    for i, query_result in enumerate(result['select_queries']):
                        if not query_result['valid']:
                            f.write(f"  SELECT 쿼리 #{i+1}:\n")
                            f.write(f"  쿼리: {query_result['query']}\n")
                            for error in query_result['errors']:
                                f.write(f"    - {error}\n")
                                
                    for i, query_result in enumerate(result['insert_queries']):
                        if not query_result['valid']:
                            f.write(f"  INSERT 쿼리 #{i+1}:\n")
                            f.write(f"  쿼리: {query_result['query']}\n")
                            for error in query_result['errors']:
                                f.write(f"    - {error}\n")
                
                f.write("\n" + "-" * 50 + "\n\n")
            
            print(f"결과가 파일에 저장되었습니다: {output_file}")
    
    return results

if __name__ == "__main__":
    # 커맨드 라인 인자 처리
    if len(sys.argv) > 1:
        directory = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        validate_xml_files_in_directory(directory, output_file)
    else:
        print("사용법: python test23.py <XML_디렉토리_경로> [결과_파일_경로]")
        
        # 예제 실행
        example_xml = input("검증할 XML 파일 경로를 입력하세요 (Enter 키를 누르면 건너뜁니다): ")
        if example_xml:
            validator = XMLQueryValidator()
            result = validator.validate_xml_file(example_xml)
            
            print("\n검증 결과:")
            print(f"XML 구조 유효성: {'O' if result['xml_structure'] else 'X'}")
            print(f"필드 이름: {result['field_names']}")
            print(f"필드 개수: {result['fields_count']}")
            print(f"SELECT 쿼리 수: {len(result['select_queries'])}")
            print(f"INSERT 쿼리 수: {len(result['insert_queries'])}")
            print(f"전체 유효성: {'O' if result['valid'] else 'X'}")
            
            if result['raw_query']:
                print(f"\n원본 쿼리:")
                print(result['raw_query'])
            
            if not result['valid']:
                print("\n오류 세부 정보:")
                
                for i, query_result in enumerate(result['select_queries']):
                    if not query_result['valid']:
                        print(f"\nSELECT 쿼리 #{i+1}:")
                        print(f"쿼리: {query_result['query']}")
                        for error in query_result['errors']:
                            print(f"  - {error}")
                            
                for i, query_result in enumerate(result['insert_queries']):
                    if not query_result['valid']:
                        print(f"\nINSERT 쿼리 #{i+1}:")
                        print(f"쿼리: {query_result['query']}")
                        for error in query_result['errors']:
                            print(f"  - {error}")
