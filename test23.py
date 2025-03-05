import os
import sys
import re
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Optional

# Import functionality from existing files
from comp_q import QueryParser
from comp_xml import XMLComparator

class XMLQueryValidator:
    """
    XML 파일 내부의 SQL 쿼리를 검증하는 클래스
    """
    
    def __init__(self):
        """초기화 메서드"""
        self.query_parser = QueryParser()  # 기존 QueryParser 활용
    
    def validate_xml_file(self, xml_path: str) -> Dict[str, List[Dict]]:
        """
        XML 파일을 파싱하고 내부의 SQL 쿼리의 포맷을 검증합니다.
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            Dict[str, List[Dict]]: 검증 결과
                {
                    'valid': True/False,
                    'xml_structure': True/False,
                    'select_queries': [
                        {
                            'query': 쿼리 문자열,
                            'valid': True/False,
                            'has_columns': True/False,
                            'has_table': True/False,
                            'errors': [오류 메시지 리스트]
                        }
                    ],
                    'insert_queries': [
                        {
                            'query': 쿼리 문자열, 
                            'valid': True/False,
                            'columns_count': 컬럼 수,
                            'values_count': 값 수,
                            'columns_values_match': True/False,
                            'errors': [오류 메시지 리스트]
                        }
                    ]
                }
        """
        result = {
            'valid': False,
            'xml_structure': False,
            'select_queries': [],
            'insert_queries': [],
            'errors': []
        }
        
        try:
            # XML 파일이 존재하는지 확인
            if not os.path.exists(xml_path):
                result['errors'] = [f"XML 파일을 찾을 수 없습니다: {xml_path}"]
                return result
                
            # XML 파일이 유효한지 확인
            try:
                tree = ET.parse(xml_path)
                root = tree.getroot()
                result['xml_structure'] = True
            except ET.ParseError as e:
                result['errors'] = [f"XML 파싱 오류: {str(e)}"]
                return result
                
            # XML에서 쿼리 추출
            select_queries, insert_queries = self.query_parser.parse_xml_file(xml_path)
            
            # SELECT 쿼리 검증
            for query in select_queries:
                query_result = self.validate_select_query(query)
                result['select_queries'].append(query_result)
                
            # INSERT 쿼리 검증  
            for query in insert_queries:
                query_result = self.validate_insert_query(query)
                result['insert_queries'].append(query_result)
                
            # 전체 결과에 대한 유효성 결정
            if all(q['valid'] for q in result['select_queries'] + result['insert_queries']):
                result['valid'] = True
                
            return result
            
        except Exception as e:
            result['errors'] = [f"예상치 못한 오류: {str(e)}"]
            return result
    
    def validate_select_query(self, query: str) -> Dict:
        """
        SELECT 쿼리의 유효성을 검사합니다.
        
        Args:
            query (str): 검사할 SELECT 쿼리
            
        Returns:
            Dict: 검증 결과
        """
        result = {
            'query': query,
            'valid': False,
            'has_columns': False,
            'has_table': False,
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
            
        # 종합 유효성 판단
        if result['has_columns'] and result['has_table']:
            result['valid'] = True
            
        return result
        
    def validate_insert_query(self, query: str) -> Dict:
        """
        INSERT 쿼리의 유효성을 검사합니다.
        
        Args:
            query (str): 검사할 INSERT 쿼리
            
        Returns:
            Dict: 검증 결과
        """
        result = {
            'query': query,
            'valid': False,
            'columns_count': 0,
            'values_count': 0,
            'columns_values_match': False,
            'errors': []
        }
        
        # 기본 INSERT 쿼리 구조 확인
        if not query.strip().upper().startswith('INSERT INTO'):
            result['errors'].append("쿼리가 INSERT INTO로 시작하지 않습니다.")
            return result
            
        # 컬럼 리스트 추출 (INSERT INTO table_name (col1, col2, ...) VALUES ...)
        columns_match = re.search(r'INSERT\s+INTO\s+\w+\s*\(([^)]+)\)', query, re.IGNORECASE | re.DOTALL)
        if not columns_match:
            result['errors'].append("INSERT 쿼리에서 컬럼 리스트를 찾을 수 없습니다.")
            return result
            
        columns = columns_match.group(1).strip()
        result['columns_count'] = len([c.strip() for c in columns.split(',')])
        
        # VALUES 절 추출
        values_match = re.search(r'VALUES\s*\(([^)]+)\)', query, re.IGNORECASE | re.DOTALL)
        if not values_match:
            result['errors'].append("INSERT 쿼리에서 VALUES 절을 찾을 수 없습니다.")
            return result
            
        values = values_match.group(1).strip()
        
        # 값 개수 계산 (단순 콤마로 분리하는 것보다 더 정교한 방법이 필요할 수 있음)
        # 문자열 리터럴 내의 콤마를 고려해야 함
        in_string = False
        string_delimiter = None
        value_count = 1  # 항상 최소 1개 값이 있다고 가정
        
        for char in values:
            if char in ("'", '"') and (not in_string or char == string_delimiter):
                if in_string:
                    in_string = False
                else:
                    in_string = True
                    string_delimiter = char
            elif char == ',' and not in_string:
                value_count += 1
                
        result['values_count'] = value_count
        
        # 컬럼 수와 값 수 비교
        if result['columns_count'] == result['values_count']:
            result['columns_values_match'] = True
            result['valid'] = True
        else:
            result['errors'].append(
                f"컬럼 수({result['columns_count']})와 값 수({result['values_count']})가 일치하지 않습니다."
            )
            
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
                print(f"  SELECT 쿼리 수: {len(result['select_queries'])}")
                print(f"  INSERT 쿼리 수: {len(result['insert_queries'])}")
                print(f"  전체 유효성: {'O' if result['valid'] else 'X'}")
                
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
                f.write(f"SELECT 쿼리 수: {len(result['select_queries'])}\n")
                f.write(f"INSERT 쿼리 수: {len(result['insert_queries'])}\n")
                f.write(f"전체 유효성: {'O' if result['valid'] else 'X'}\n")
                
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
            print(f"SELECT 쿼리 수: {len(result['select_queries'])}")
            print(f"INSERT 쿼리 수: {len(result['insert_queries'])}")
            print(f"전체 유효성: {'O' if result['valid'] else 'X'}")
            
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
