import openpyxl
from xltest import read_interface_block, process_interface
from comp_q import QueryParser, QueryDifference
from maptest import ColumnMapper
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
import os
import fnmatch

class XMLComparator:
    def __init__(self, excel_path: str, search_dir: str):
        """
        XML 비교를 위한 클래스 초기화
        
        Args:
            excel_path (str): 인터페이스 정보가 있는 Excel 파일 경로
            search_dir (str): XML 파일을 검색할 디렉토리 경로
        """
        self.excel_path = excel_path
        self.search_dir = search_dir
        self.workbook = openpyxl.load_workbook(excel_path)
        self.worksheet = self.workbook.active
        self.mapper = ColumnMapper()
        self.query_parser = QueryParser()
        
    def extract_from_xml(self, xml_path: str) -> Tuple[str, str]:
        """
        XML 파일에서 쿼리와 XML 내용을 추출합니다.
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            Tuple[str, str]: (쿼리, XML 내용)
        """
        try:
            # XML 파일이 제대로 로드되었는지 확인
            if not os.path.exists(xml_path):
                print(f"Warning: XML file not found: {xml_path}")
                return None, None
                
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # XML 내용이 유효한지 확인
            if root is None:
                print(f"Warning: Invalid XML content in file: {xml_path}")
                return None, None
            
            # SQL 노드 찾기
            sql_node = root.find(".//SQL")
            if sql_node is None or not sql_node.text:
                print(f"Warning: No SQL content found in file: {xml_path}")
                return None, None
                
            query = sql_node.text.strip()
            xml_content = ET.tostring(root, encoding='unicode')
            
            # 추출된 쿼리가 유효한지 확인
            if not query:
                print(f"Warning: Empty SQL query in file: {xml_path}")
                return None, None
                
            return query, xml_content
            
        except ET.ParseError as e:
            print(f"Error parsing XML file {xml_path}: {e}")
            return None, None
        except Exception as e:
            print(f"Unexpected error processing file {xml_path}: {e}")
            return None, None
            
    def compare_queries(self, query1: str, query2: str) -> QueryDifference:
        """
        두 쿼리를 비교합니다.
        
        Args:
            query1 (str): 첫 번째 쿼리
            query2 (str): 두 번째 쿼리
            
        Returns:
            QueryDifference: 쿼리 비교 결과
        """
        if not query1 or not query2:
            return None
        return self.query_parser.compare_queries(query1, query2)
        
    def find_interface_files(self, if_id: str) -> Dict[str, Dict]:
        """
        주어진 IF ID에 해당하는 송수신 XML 파일을 찾고 쿼리를 추출합니다.
        파일명 패턴: {if_id}로 시작하고 .SND.xml 또는 .RCV.xml로 끝나는 파일
        
        Args:
            if_id (str): 인터페이스 ID
            
        Returns:
            Dict[str, Dict]: {
                'send': {'path': 송신파일경로, 'query': 송신쿼리, 'xml': 송신XML},
                'recv': {'path': 수신파일경로, 'query': 수신쿼리, 'xml': 수신XML}
            }
        """
        results = {
            'send': {'path': None, 'query': None, 'xml': None},
            'recv': {'path': None, 'query': None, 'xml': None}
        }
        
        if not if_id:
            print("Warning: Empty IF_ID provided")
            return results
            
        try:
            # 디렉토리 내의 모든 XML 파일 검색
            for file in os.listdir(self.search_dir):
                if not file.startswith(if_id):
                    continue
                    
                file_path = os.path.join(self.search_dir, file)
                
                # 송신 파일 (.SND.xml)
                if file.endswith('.SND.xml'):
                    results['send']['path'] = file_path
                    query, xml = self.extract_from_xml(file_path)
                    if query and xml:
                        results['send']['query'] = query
                        results['send']['xml'] = xml
                    else:
                        print(f"Warning: Failed to extract query from send file: {file_path}")
                
                # 수신 파일 (.RCV.xml)
                elif file.endswith('.RCV.xml'):
                    results['recv']['path'] = file_path
                    query, xml = self.extract_from_xml(file_path)
                    if query and xml:
                        results['recv']['query'] = query
                        results['recv']['xml'] = xml
                    else:
                        print(f"Warning: Failed to extract query from receive file: {file_path}")
            
            # 파일을 찾았는지 확인
            if not results['send']['path'] and not results['recv']['path']:
                print(f"Warning: No interface files found for IF_ID: {if_id}")
            elif not results['send']['path']:
                print(f"Warning: No send file found for IF_ID: {if_id}")
            elif not results['recv']['path']:
                print(f"Warning: No receive file found for IF_ID: {if_id}")
            
            return results
            
        except Exception as e:
            print(f"Error finding interface files: {e}")
            return results
        
    def process_interface_block(self, start_col: int) -> Optional[Dict]:
        """
        Excel에서 하나의 인터페이스 블록을 처리합니다.
        
        Args:
            start_col (int): 인터페이스 블록이 시작되는 컬럼
            
        Returns:
            Optional[Dict]: 처리된 인터페이스 정보와 결과, 실패시 None
        """
        try:
            # Excel에서 인터페이스 정보 읽기
            interface_info = read_interface_block(self.worksheet, start_col)
            if not interface_info:
                print(f"Warning: Failed to read interface block at column {start_col}")
                return None
                
            # Excel에서 추출된 쿼리와 XML 얻기
            excel_results = process_interface(interface_info, self.mapper)
            if not excel_results:
                print(f"Warning: Failed to process interface at column {start_col}")
                return None
                
            # 송수신 파일 찾기
            file_results = self.find_interface_files(interface_info['interface_id'])
            if not file_results:
                print(f"Warning: No interface files found for IF_ID: {interface_info['interface_id']}")
                return None
                
            # 결과 초기화
            comparisons = {
                'send': None,
                'recv': None
            }
            warnings = {
                'send': [],
                'recv': []
            }
            
            # 송신 쿼리 처리
            if excel_results['send_sql'] and file_results['send']['query']:
                try:
                    comparisons['send'] = self.query_parser.compare_queries(
                        excel_results['send_sql'],
                        file_results['send']['query']
                    )
                    warnings['send'].extend(
                        self.query_parser.check_special_columns(
                            file_results['send']['query'],
                            'send'
                        )
                    )
                except Exception as e:
                    print(f"Error comparing send queries: {e}")
                    print(f"Excel query: {excel_results['send_sql']}")
                    print(f"File query: {file_results['send']['query']}")
            
            # 수신 쿼리 처리
            if excel_results['recv_sql'] and file_results['recv']['query']:
                try:
                    comparisons['recv'] = self.query_parser.compare_queries(
                        excel_results['recv_sql'],
                        file_results['recv']['query']
                    )
                    warnings['recv'].extend(
                        self.query_parser.check_special_columns(
                            file_results['recv']['query'],
                            'recv'
                        )
                    )
                except Exception as e:
                    print(f"Error comparing receive queries: {e}")
                    print(f"Excel query: {excel_results['recv_sql']}")
                    print(f"File query: {file_results['recv']['query']}")
            
            return {
                'if_id': interface_info['interface_id'],
                'interface_name': interface_info['interface_name'],
                'comparisons': comparisons,
                'warnings': warnings,
                'excel': excel_results,
                'files': file_results
            }
            
        except Exception as e:
            print(f"Error processing interface block at column {start_col}: {e}")
            return None
            
    def process_all_interfaces(self) -> List[Dict]:
        """
        Excel 파일의 모든 인터페이스를 처리합니다.
        B열부터 시작하여 3컬럼 단위로 처리합니다.
        
        Returns:
            List[Dict]: 각 인터페이스의 처리 결과 목록
        """
        results = []
        col = 2  # B열부터 시작
        
        while True:
            # 인터페이스 ID가 없으면 종료
            if not self.worksheet.cell(row=2, column=col).value:
                break
                
            result = self.process_interface_block(col)
            if result:
                results.append(result)
                
            col += 3  # 다음 인터페이스 블록으로 이동
            
        # 결과 출력
        for idx, result in enumerate(results, 1):
            print(f"\n=== 인터페이스 {idx} ===")
            print(f"ID: {result['if_id']}")
            print(f"이름: {result['interface_name']}")
            
            print("\n파일 검색 결과:")
            print(f"송신 파일: {result['files']['send']['path']}")
            print(f"수신 파일: {result['files']['recv']['path']}")
            
            print("\n쿼리 비교 결과:")
            if result['comparisons']['send']:
                print("송신 쿼리:")
                print(f"  {result['comparisons']['send']}")
            if result['comparisons']['recv']:
                print("수신 쿼리:")
                print(f"  {result['comparisons']['recv']}")
            
            # 경고가 있을 때만 경고 섹션 출력
            send_warnings = result['warnings']['send']
            recv_warnings = result['warnings']['recv']
            if send_warnings or recv_warnings:
                print("\n경고:")
                if send_warnings:
                    print("송신 쿼리 경고:")
                    for warning in send_warnings:
                        print(f"  - {warning}")
                if recv_warnings:
                    print("수신 쿼리 경고:")
                    for warning in recv_warnings:
                        print(f"  - {warning}")
                
        return results
        
    def close(self):
        """리소스 정리"""
        self.workbook.close()
        if self.mapper:
            self.mapper.close_connections()

def main():
    """
    메인 실행 함수
    """
    excel_path = "input.xlsx"
    search_dir = "."  # 현재 디렉토리에서 검색
    
    comparator = XMLComparator(excel_path, search_dir)
    try:
        results = comparator.process_all_interfaces()
        
    finally:
        comparator.close()

if __name__ == "__main__":
    main()
